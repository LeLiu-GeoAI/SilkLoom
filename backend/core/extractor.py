import asyncio
import base64
import json
import mimetypes
import os
import sqlite3
import threading
import time
import warnings
from pathlib import Path

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
            self.image_path_field = str(self.task_cfg.get("image_path_field", "") or "").strip()
            self.nested_target_field = str(self.task_cfg.get("nested_target_field", "") or "").strip()
            nested_target_cfg = self.task_cfg.get("nested_target", {})
            if not self.nested_target_field and isinstance(nested_target_cfg, dict):
                self.nested_target_field = str(nested_target_cfg.get("field", "") or "").strip()
            if self.image_path_field and self.image_path_field not in self.input_cols:
                self.input_cols.append(self.image_path_field)
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

    def _resolve_image_items(self, row_dict):
        if not self.image_path_field:
            return []

        raw_value = row_dict.get(self.image_path_field)
        if is_null_value(raw_value):
            return []

        candidates = []
        if isinstance(raw_value, (list, tuple)):
            candidates = list(raw_value)
        elif isinstance(raw_value, str):
            text = raw_value.strip()
            if not text:
                return []
            if text.startswith("[") and text.endswith("]"):
                try:
                    parsed = json.loads(text)
                    if isinstance(parsed, list):
                        candidates = parsed
                    else:
                        candidates = [text]
                except Exception:
                    candidates = [text]
            else:
                candidates = [text]
        else:
            candidates = [str(raw_value)]

        base_dir = Path(self.file_path).resolve().parent
        resolved_items = []
        for item in candidates:
            label_text = ""
            path_text = ""

            if isinstance(item, dict):
                path_text = str(
                    item.get("path")
                    or item.get("image_path")
                    or item.get("image_url")
                    or ""
                ).strip()
                label_text = str(
                    item.get("label")
                    or item.get("text")
                    or item.get("meta")
                    or item.get("time")
                    or ""
                ).strip()
            else:
                raw_item = str(item or "").strip()
                if "||" in raw_item:
                    left, right = raw_item.split("||", 1)
                    label_text = left.strip()
                    path_text = right.strip()
                else:
                    path_text = raw_item

            if not path_text:
                continue

            if path_text.startswith("http://") or path_text.startswith("https://"):
                resolved_items.append({"path": path_text, "label": label_text})
                continue

            image_path = Path(path_text)
            if not image_path.is_absolute():
                image_path = (base_dir / image_path).resolve()

            if not image_path.exists() or not image_path.is_file():
                raise FileNotFoundError(f"图片文件不存在: {image_path}")

            resolved_items.append({"path": str(image_path), "label": label_text})

        return resolved_items

    @staticmethod
    def _build_image_data_url(image_path):
        mime, _ = mimetypes.guess_type(image_path)
        if not mime:
            mime = "application/octet-stream"

        with open(image_path, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("ascii")
        return f"data:{mime};base64,{encoded}"

    def _prepare_image_payloads(self, image_items):
        prepared = []
        for item in image_items:
            path = item.get("path", "")
            label = str(item.get("label", "") or "").strip()
            if not path:
                continue

            if path.startswith("http://") or path.startswith("https://"):
                prepared.append({"label": label, "url": path})
                continue

            data_url = self._build_image_data_url(path)
            prepared.append({"label": label, "url": data_url})

        return prepared

    def _build_messages(self, prompt, prepared_images, image_sampling_note=""):
        if not prepared_images:
            return [{"role": "user", "content": prompt}]

        content = [{"type": "text", "text": prompt}]
        if image_sampling_note:
            content.append({"type": "text", "text": image_sampling_note})

        for idx, item in enumerate(prepared_images, start=1):
            label = item.get("label", "")
            if label:
                content.append({"type": "text", "text": label})
            else:
                content.append({"type": "text", "text": f"图片{idx}"})

            image_value = item.get("url", "")
            content.append({"type": "image_url", "image_url": {"url": image_value}})

        return [{"role": "user", "content": content}]

    async def _call_llm_async(self, prompt, sem, image_items=None, image_sampling_note=""):
        """异步 LLM 调用，带并发控制和重试"""
        async with sem:  # 控制并发度，最多同时 max_workers 个请求
            headers = {
                "Authorization": f"Bearer {self.llm_api_key}",
                "Content-Type": "application/json",
            }
            payload_base = {
                "model": self.llm_cfg["model"],
            }

            try:
                payload_base["max_tokens"] = int(self.llm_cfg.get("max_tokens", 2048) or 2048)
            except Exception:
                payload_base["max_tokens"] = 2048

            try:
                temperature = float(self.llm_cfg.get("temperature", 0.7))
            except Exception:
                temperature = 0.7
            if 0 < temperature < 1:
                payload_base["temperature"] = temperature

            # 如果启用 think 模式，添加相关参数
            if self.llm_cfg.get("enable_think", False):
                # 支持多种 API 格式（可按需修改）
                # OpenAI o1 风格：
                payload_base["reasoning"] = {"type": "enabled", "rules": ["custom"]}
                # 或简单标记：
                # payload["enable_thinking"] = True

            timeout = httpx.Timeout(self.llm_cfg.get("timeout", 60), connect=10)
            proxy_url = (self.llm_cfg.get("proxy_url", "") or "").strip()
            current_image_items = list(image_items or [])
            current_sampling_note = image_sampling_note
            last_error_msg = ""
            for attempt in range(self.llm_cfg.get("max_try", 3)):
                prepared_images = self._prepare_image_payloads(current_image_items)
                messages = self._build_messages(
                    prompt,
                    prepared_images,
                    image_sampling_note=current_sampling_note,
                )

                try:
                    payload = dict(payload_base)
                    payload["messages"] = messages

                    if proxy_url:
                        async with httpx.AsyncClient(timeout=timeout, proxy=proxy_url) as client:
                            response = await client.post(
                                self.llm_cfg["base_url"],
                                headers=headers,
                                json=payload,
                            )
                    else:
                        async with httpx.AsyncClient(timeout=timeout) as client:
                            response = await client.post(
                                self.llm_cfg["base_url"],
                                headers=headers,
                                json=payload,
                            )

                    response.raise_for_status()
                    return response.json()["choices"][0]["message"]["content"]
                except httpx.HTTPStatusError as e:
                    status_code = e.response.status_code if e.response is not None else "unknown"
                    detail = ""
                    if e.response is not None:
                        try:
                            detail = json.dumps(e.response.json(), ensure_ascii=False)
                        except Exception:
                            detail = (e.response.text or "").strip()
                    last_error_msg = f"HTTP {status_code} | {detail}"

                except Exception as e:
                    last_error_msg = str(e)

                if attempt < self.llm_cfg.get("max_try", 3) - 1:
                    await asyncio.sleep(self.llm_cfg.get("retry_delay", 2))

            raise RuntimeError(f"大模型 API 请求失败: {last_error_msg} | image_count={len(current_image_items)}")

    def _call_llm(self, prompt, stop_event, image_items=None, image_sampling_note=""):
        """同步包装，供线程调用"""
        sem = asyncio.Semaphore(self.max_workers)
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(
                self._call_llm_async(
                    prompt,
                    sem,
                    image_items=image_items,
                    image_sampling_note=image_sampling_note,
                )
            )
        finally:
            loop.close()

    @staticmethod
    def _source_row_id_from_record_id(record_id):
        text = str(record_id or "").strip()
        if not text:
            return ""
        if "#" in text:
            return text.split("#", 1)[0]
        return text

    def _resolve_nested_path(self, payload, path_text):
        current = payload
        for key in [part.strip() for part in str(path_text or "").split(".") if part.strip()]:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return None
        return current

    def _extract_output_items(self, parsed_payload):
        if self.nested_target_field and isinstance(parsed_payload, dict):
            nested_value = self._resolve_nested_path(parsed_payload, self.nested_target_field)
            if isinstance(nested_value, dict):
                return [nested_value]
            if isinstance(nested_value, list):
                return [item for item in nested_value if isinstance(item, dict)]
            return []

        if isinstance(parsed_payload, dict):
            return [parsed_payload]
        if isinstance(parsed_payload, list):
            return [item for item in parsed_payload if isinstance(item, dict)]
        return []


    def _parse_single_output(self, raw_text, row_dict):
        parsed_payload = {}
        if raw_text:
            parsed = json_repair.loads(raw_text)
            if isinstance(parsed, (dict, list)):
                parsed_payload = parsed

        in_dict = {col: str(row_dict.get(col, "")) if not is_null_value(row_dict.get(col)) else "" for col in self.input_cols}
        output_items = self._extract_output_items(parsed_payload)
        out_rows = [{field: item.get(field, "") for field in self.output_cols} for item in output_items]
        return in_dict, out_rows, parsed_payload

    def _load_done_ids(self):
        done = set()
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT row_id FROM results")
                for row in cursor:
                    done.add(self._source_row_id_from_record_id(row[0]))
        except Exception:
            pass
        return done

    def _save_to_db(self, pid, in_dict, out_rows, raw_text, status="success", error_msg=""):
        """保存记录到批处理列表（而不是直接写入）"""
        base_pid = str(pid or "").strip() or f"row_{int(time.time() * 1000)}"
        records = []

        if status == "success" and out_rows:
            multi = len(out_rows) > 1
            for idx, out_dict in enumerate(out_rows, start=1):
                row_id = f"{base_pid}#{idx}" if multi else base_pid
                records.append(
                    (
                        row_id,
                        json.dumps(in_dict, ensure_ascii=False),
                        json.dumps(out_dict, ensure_ascii=False),
                        raw_text,
                        status,
                        error_msg,
                    )
                )
            return records

        records.append(
            (
                base_pid,
                json.dumps(in_dict, ensure_ascii=False),
                json.dumps({}, ensure_ascii=False),
                raw_text,
                status,
                error_msg,
            )
        )
        return records

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
            self.done_ids_runtime.add(self._source_row_id_from_record_id(item[0]))


    def _process_single(self, row_dict, log_callback, stop_event):
        """处理单行数据（优化：直接接收字典，避免转换）"""
        if stop_event.is_set():
            return [], "stopped", 0

        pid = str(row_dict.get(self.final_id_col, "")).strip() if not is_null_value(row_dict.get(self.final_id_col)) else ""
        # row_dict 已经是字典，无需转换
        prompt = self.prompt_template.render(row=row_dict, schema=self.schema_str_for_prompt)
        image_items = []
        image_sampling_note = ""
        raw_text = ""

        try:
            image_items = self._resolve_image_items(row_dict)
            raw_text = self._call_llm(
                prompt,
                stop_event,
                image_items=image_items,
                image_sampling_note=image_sampling_note,
            )
            if stop_event.is_set():
                return [], "stopped", 0
            if not raw_text:
                raise ValueError("大模型未返回任何文本")

            in_dict, out_rows, raw_json_payload = self._parse_single_output(raw_text, row_dict)
            if not raw_json_payload:
                raise ValueError("未提取到任何有效 JSON 结构")
            if not out_rows:
                if self.nested_target_field:
                    raise ValueError(f"嵌套目标字段未提取到有效对象: {self.nested_target_field}")
                raise ValueError("未提取到任何有效输出行")

            log_content = json.dumps(raw_json_payload, ensure_ascii=False, indent=2)
            log_callback(f"✅ ID: {pid} | 成功解析 {len(out_rows)} 行\n{log_content}")

            # 返回数据记录供批量提交（不再直接写入）
            records = self._save_to_db(pid, in_dict, out_rows, raw_text, status="success", error_msg="")
            return records, "success", len(out_rows)

        except Exception as e:
            error_msg = str(e)
            log_callback(f"❌ ID: {pid} | 处理失败: {error_msg}")
            in_dict = {col: str(row_dict.get(col, "")) if not is_null_value(row_dict.get(col)) else "" for col in self.input_cols}
            
            # 返回数据记录供批量提交
            records = self._save_to_db(pid, in_dict, [], raw_text, status="failed", error_msg=error_msg)
            return records, "failed", 0

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
        if self.image_path_field and self.image_path_field not in table.columns:
            missing_req.append(self.image_path_field)
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
                    records, result_status, output_count = future.result()
                    shared_state["processed"] += 1
                    
                    if result_status == "success":
                        shared_state["success"] += 1
                        shared_state["output_rows"] = shared_state.get("output_rows", 0) + output_count
                        if records:
                            batch.extend(records)
                    elif result_status == "failed":
                        shared_state["failed"] += 1
                        if records:
                            batch.extend(records)
                    
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
            return (
                f"🛑 已中断！进度已保存。本次新增: 成功输入 {shared_state['success']} 条，"
                f"失败输入 {shared_state['failed']} 条，输出 {shared_state.get('output_rows', shared_state['success'])} 行。"
            )
        return (
            f"🎉 全部提取完成！本次新增: 成功输入 {shared_state['success']} 条，"
            f"失败输入 {shared_state['failed']} 条，输出 {shared_state.get('output_rows', shared_state['success'])} 行。"
        )
