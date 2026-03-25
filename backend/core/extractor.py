import asyncio
import json
import os
import sqlite3
import threading
import time
import warnings

import httpx
import json_repair
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from jinja2 import Template

from backend.core.constants import DB_DIR, EXPORT_DIR
from backend.core.data_io import (
    get_task_hash,
    is_null_value,
    universal_read_data,
    write_records_csv,
    write_records_excel,
    write_records_jsonl,
)
from backend.core.template_tools import analyze_template_requirements
from backend.security.secrets_codec import encrypt_config_yaml

warnings.simplefilter(action="ignore", category=FutureWarning)


def resolve_api_key(config_api_key):
    env_key = os.getenv("LLM_API_KEY", "").strip() or os.getenv("OPENAI_API_KEY", "").strip()
    if env_key:
        return env_key

    cfg_key = (config_api_key or "").strip()
    if cfg_key and "YOUR_API_KEY" not in cfg_key:
        return cfg_key

    raise ValueError("未配置 API Key。请在环境变量 LLM_API_KEY（或 OPENAI_API_KEY）中设置，或在 config.yml 中填写 llm.api_key。")


def export_results_to_file(yaml_str, file_path, format_type):
    if not file_path or not os.path.exists(file_path):
        return None

    task_hash = get_task_hash(yaml_str, file_path)
    db_path = os.path.join(DB_DIR, f"task_{task_hash}.db")
    if not os.path.exists(db_path):
        return None

    with sqlite3.connect(db_path) as conn:
        cursor = conn.execute("SELECT row_id, input_data, output_data, status, error_msg FROM results")
        rows = cursor.fetchall()

    if not rows:
        return None

    records = []
    for row_id, input_data, output_data, status, error_msg in rows:
        in_data = json.loads(input_data) if input_data else {}
        out_data = json.loads(output_data) if output_data else {}
        for k, v in out_data.items():
            if isinstance(v, (dict, list)):
                out_data[k] = json.dumps(v, ensure_ascii=False)

        record = {"id": row_id}
        record.update(in_data)
        record.update(out_data)
        record["执行状态"] = status
        record["错误信息"] = error_msg
        records.append(record)

    export_file = os.path.join(EXPORT_DIR, f"export_{task_hash[:8]}_{int(time.time())}")

    if format_type == "CSV":
        export_file += ".csv"
        write_records_csv(records, export_file)
    elif format_type == "Excel":
        export_file += ".xlsx"
        write_records_excel(records, export_file)
    elif format_type == "JSONL":
        export_file += ".jsonl"
        write_records_jsonl(records, export_file)
    else:
        return None

    return export_file


class UniversalExtractor:
    def __init__(self, yaml_str, file_path):
        self.config_dict = yaml.safe_load(yaml_str)
        try:
            self.llm_cfg = self.config_dict["llm"]
            self.run_cfg = self.config_dict["run"]
            self.task_cfg = self.config_dict["task"]
            self.max_workers = self.run_cfg.get("max_workers", 10)
            self.shuffle_before_run = self.run_cfg.get("shuffle_before_run", True)
            self.schema_dict = self.task_cfg["target_schema"]

            self.yaml_str = yaml_str
            self.file_path = file_path

            template_str = self.task_cfg.get("prompt_template", "")
            self.req_cols, self.opt_cols = analyze_template_requirements(template_str)
            self.input_cols = self.req_cols + self.opt_cols
            self.llm_api_key = resolve_api_key(self.llm_cfg.get("api_key", ""))

        except KeyError as e:
            raise KeyError(f"配置文件缺失必要参数: {str(e)}")

        self.write_lock = threading.Lock()
        self.output_cols = list(self.schema_dict.keys())
        self.schema_str_for_prompt = json.dumps(self.schema_dict, ensure_ascii=False, indent=2)
        self.prompt_template = Template(self.task_cfg["prompt_template"])
        self.final_id_col = "row_id"

        task_hash = get_task_hash(yaml_str, file_path)
        self.task_hash = task_hash
        self.db_path = os.path.join(DB_DIR, f"task_{task_hash}.db")
        self._init_db()

    def _init_db(self):
        secure_yaml = encrypt_config_yaml(self.yaml_str)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS experiments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT,
                yaml_config TEXT,
                task_name TEXT,
                description TEXT,
                status TEXT DEFAULT 'idle',
                input_rows INTEGER DEFAULT 0,
                output_rows INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
            )
            conn.execute(
                """CREATE TABLE IF NOT EXISTS results (
                row_id TEXT PRIMARY KEY,
                input_data TEXT,
                output_data TEXT,
                raw_llm TEXT,
                status TEXT DEFAULT 'success',
                error_msg TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )"""
            )

            for ddl in [
                "ALTER TABLE experiments ADD COLUMN task_name TEXT",
                "ALTER TABLE experiments ADD COLUMN description TEXT",
                "ALTER TABLE experiments ADD COLUMN status TEXT DEFAULT 'idle'",
                "ALTER TABLE experiments ADD COLUMN input_rows INTEGER DEFAULT 0",
                "ALTER TABLE experiments ADD COLUMN output_rows INTEGER DEFAULT 0",
                "ALTER TABLE experiments ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP",
                "ALTER TABLE results ADD COLUMN status TEXT DEFAULT 'success'",
                "ALTER TABLE results ADD COLUMN error_msg TEXT",
            ]:
                try:
                    conn.execute(ddl)
                except sqlite3.OperationalError:
                    pass

            rows = conn.execute("SELECT id, task_name, description, status FROM experiments ORDER BY id DESC").fetchall()
            if rows:
                keep_id = rows[0][0]
                if len(rows) > 1:
                    conn.execute("DELETE FROM experiments WHERE id != ?", (keep_id,))

                task_name = rows[0][1] or f"任务_{self.task_hash[:8]}"
                description = rows[0][2] or ""
                status = rows[0][3] or "idle"
                conn.execute(
                    """
                    UPDATE experiments
                    SET file_path = ?, yaml_config = ?, task_name = ?, description = ?, status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """,
                    (self.file_path, secure_yaml, task_name, description, status, keep_id),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO experiments (file_path, yaml_config, task_name, description, status)
                    VALUES (?, ?, ?, ?, ?)
                """,
                    (self.file_path, secure_yaml, f"任务_{self.task_hash[:8]}", "", "idle"),
                )

    async def _call_llm_async(self, prompt, sem):
        """异步 LLM 调用，带并发控制和重试"""
        async with sem:  # 控制并发度，最多同时 max_workers 个请求
            headers = {
                "Authorization": f"Bearer {self.llm_api_key}",
                "Content-Type": "application/json",
            }
            payload = {
                "model": self.llm_cfg["model"],
                "messages": [{"role": "user", "content": prompt}],
                "temperature": self.llm_cfg["temperature"],
                "max_tokens": int(self.llm_cfg.get("max_tokens", 2048) or 2048),
            }

            # 如果启用 think 模式，添加相关参数
            if self.llm_cfg.get("enable_think", False):
                # 支持多种 API 格式（可按需修改）
                # OpenAI o1 风格：
                payload["reasoning"] = {"type": "enabled", "rules": ["custom"]}
                # 或简单标记：
                # payload["enable_thinking"] = True

            timeout = httpx.Timeout(self.llm_cfg.get("timeout", 60), connect=10)
            proxy_url = (self.llm_cfg.get("proxy_url", "") or "").strip()
            for attempt in range(self.llm_cfg.get("max_try", 3)):
                try:
                    client_kwargs = {"timeout": timeout}
                    if proxy_url:
                        client_kwargs["proxy"] = proxy_url

                    async with httpx.AsyncClient(**client_kwargs) as client:
                        response = await client.post(
                            self.llm_cfg["base_url"],
                            headers=headers,
                            json=payload,
                        )
                        response.raise_for_status()
                        return response.json()["choices"][0]["message"]["content"]
                except Exception as e:
                    if attempt == self.llm_cfg.get("max_try", 3) - 1:
                        raise RuntimeError(f"大模型 API 请求失败: {str(e)}")
                    # 等待后重试
                    await asyncio.sleep(self.llm_cfg.get("retry_delay", 2))

    def _call_llm(self, prompt, stop_event):
        """同步包装，供线程调用"""
        sem = asyncio.Semaphore(self.max_workers)
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._call_llm_async(prompt, sem)
            )
        finally:
            loop.close()


    def _parse_single_output(self, raw_text, row_dict):
        parsed_dict = {}
        if raw_text:
            parsed = json_repair.loads(raw_text)
            if isinstance(parsed, dict):
                parsed_dict = parsed
            elif isinstance(parsed, list) and len(parsed) > 0 and isinstance(parsed[0], dict):
                parsed_dict = parsed[0]

        in_dict = {col: str(row_dict.get(col, "")) if not is_null_value(row_dict.get(col)) else "" for col in self.input_cols}
        out_dict = {field: parsed_dict.get(field, "") for field in self.output_cols}
        return in_dict, out_dict, parsed_dict

    def _load_done_ids(self):
        done = set()
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT row_id FROM results")
                for row in cursor:
                    done.add(str(row[0]).strip())
        except Exception:
            pass
        return done

    def _save_to_db(self, pid, in_dict, out_dict, raw_text, status="success", error_msg=""):
        """保存单条记录到批处理列表（而不是直接写入）"""
        # 返回元组供批量提交
        return (
            pid,
            json.dumps(in_dict, ensure_ascii=False),
            json.dumps(out_dict, ensure_ascii=False),
            raw_text,
            status,
            error_msg,
        )

    def _save_batch_to_db(self, batch):
        """批量写入数据库（关键优化）"""
        if not batch:
            return
        
        with self.write_lock:
            with sqlite3.connect(self.db_path) as conn:
                conn.executemany(
                    """INSERT OR REPLACE INTO results 
                       (row_id, input_data, output_data, raw_llm, status, error_msg)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    batch,
                )
                conn.commit()
        
        # 更新运行时 done_ids
        for item in batch:
            self.done_ids_runtime.add(str(item[0]))


    def _process_single(self, row_dict, log_callback, stop_event):
        """处理单行数据（优化：直接接收字典，避免转换）"""
        if stop_event.is_set():
            return None, "stopped"

        pid = str(row_dict.get(self.final_id_col, "")).strip() if not is_null_value(row_dict.get(self.final_id_col)) else ""
        # row_dict 已经是字典，无需转换
        prompt = self.prompt_template.render(row=row_dict, schema=self.schema_str_for_prompt)
        raw_text = ""

        try:
            raw_text = self._call_llm(prompt, stop_event)
            if stop_event.is_set():
                return None, "stopped"
            if not raw_text:
                raise ValueError("大模型未返回任何文本")

            in_dict, out_dict, raw_json_dict = self._parse_single_output(raw_text, row_dict)
            if not raw_json_dict:
                raise ValueError("未提取到任何有效 JSON 结构")

            log_content = json.dumps(raw_json_dict, ensure_ascii=False, indent=2)
            log_callback(f"✅ ID: {pid} | 成功解析\n{log_content}")

            # 返回数据记录供批量提交（不再直接写入）
            record = self._save_to_db(pid, in_dict, out_dict, raw_text, status="success", error_msg="")
            return record, "success"

        except Exception as e:
            error_msg = str(e)
            log_callback(f"❌ ID: {pid} | 处理失败: {error_msg}")
            in_dict = {col: str(row_dict.get(col, "")) if not is_null_value(row_dict.get(col)) else "" for col in self.input_cols}
            
            # 返回数据记录供批量提交
            record = self._save_to_db(pid, in_dict, {}, raw_text, status="failed", error_msg=error_msg)
            return record, "failed"

    def run(self, stop_event, shared_state, log_callback=print):
        table = None
        for attempt in range(10):
            try:
                table = universal_read_data(self.file_path)
                break
            except Exception as e:
                time.sleep(1)
                if attempt == 9:
                    return f"❌ 读取文件失败: {str(e)}"

        if table is None:
            return "❌ 无法读取文件，请确保没有被占用！"

        missing_req = [col for col in self.req_cols if col not in table.columns]
        if missing_req:
            return f"❌ 中止：数据源缺少必填列 {missing_req}。"

        self.final_id_col = "row_id"
        table = table.with_row_id(self.final_id_col)

        if self.shuffle_before_run:
            table = table.shuffled()

        done_ids = self._load_done_ids()
        self.done_ids_runtime = set(done_ids)
        todo_table = table.filter_not_in(self.final_id_col, done_ids)
        total_todo = len(todo_table)
        shared_state["total"] = total_todo

        if total_todo == 0:
            return "✅ 本文件使用当前配置的所有数据均已在库中处理完毕！"

        data = todo_table.to_records()

        executor = ThreadPoolExecutor(max_workers=self.max_workers)
        futures = []
        batch = []
        batch_size = 100  # 每 100 条记录批量提交一次
        
        try:
            # 优化：直接传递字典，无需再转换
            futures = [executor.submit(self._process_single, row_dict, log_callback, stop_event) for row_dict in data]
            for future in as_completed(futures):
                if stop_event.is_set():
                    for pending in futures:
                        pending.cancel()
                    # 提交剩余批次
                    if batch:
                        self._save_batch_to_db(batch)
                    break
                try:
                    record, result_status = future.result()
                    shared_state["processed"] += 1
                    
                    if result_status == "success":
                        shared_state["success"] += 1
                        if record:
                            batch.append(record)
                    elif result_status == "failed":
                        shared_state["failed"] += 1
                        if record:
                            batch.append(record)
                    
                    # 批量提交检查
                    if len(batch) >= batch_size:
                        self._save_batch_to_db(batch)
                        batch = []
                        
                except Exception as e:
                    print(f"❌ 线程异常: {e}")
            
            # 提交剩余的批次
            if batch and not stop_event.is_set():
                self._save_batch_to_db(batch)
        finally:
            executor.shutdown(wait=not stop_event.is_set(), cancel_futures=stop_event.is_set())

        if stop_event.is_set():
            return f"🛑 已中断！进度已保存。本次新增: 成功 {shared_state['success']} 条，失败 {shared_state['failed']} 条。"
        return f"🎉 全部提取完成！本次新增: 成功 {shared_state['success']} 条，失败 {shared_state['failed']} 条。"
