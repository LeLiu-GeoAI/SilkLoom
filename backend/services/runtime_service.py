import os
import threading
import time
import yaml
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from backend.config_io import save_yaml
from backend.core.data_io import get_task_hash, universal_read_data
from backend.core.extractor import UniversalExtractor
from backend.core.task_repository import get_task_config, update_task_metadata


@dataclass
class RuntimeSnapshot:
    running: bool
    starting: bool
    task_hash: Optional[str]
    shared_state: dict
    latest_log: str
    status_text: str
    final_message: str
    stopping: bool


@dataclass
class RuntimeStore:
    task_hash: Optional[str] = None
    thread: Optional[threading.Thread] = None
    starting: bool = False
    stop_event: Optional[threading.Event] = None
    shared_state: dict = field(default_factory=lambda: {"processed": 0, "total": 0, "success": 0, "failed": 0})
    latest_log: str = ""
    status_text: str = "等待启动..."
    final_message: str = ""


class RuntimeService:
    def __init__(self):
        self._lock = threading.Lock()
        self._store = RuntimeStore()
        self._df_cache = {}  # DataFrame 缓存，避免重复读取文件（关键优化）


    def _console_log(self, stage, message):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] [{stage}] {message}", flush=True)

    def get_or_read_data(self, file_path):
        """
        带缓存的数据读取（关键优化）
        避免重复读取同一个文件
        缓存仅保存未修改的文件
        """
        if not file_path or not os.path.exists(file_path):
            return None
        
        try:
            mtime = os.path.getmtime(file_path)
            cache_key = file_path
            
            # 检查缓存是否有效
            if cache_key in self._df_cache:
                cached_mtime, cached_df = self._df_cache[cache_key]
                if cached_mtime == mtime:
                    # 缓存命中！无需重读
                    self._console_log("CACHE", f"DataFrame 缓存命中 {file_path[:50]}")
                    return cached_df.copy()  # 返回副本，防止修改原数据
            
            # 缓存未命中，读取文件
            self._console_log("READ", f"读取文件 {file_path[:50]}")
            df = universal_read_data(file_path)
            
            # 存入缓存
            self._df_cache[cache_key] = (mtime, df)
            return df
        except Exception as e:
            self._console_log("ERROR", f"读取数据失败: {str(e)}")
            return None

    def _is_running_unlocked(self):
        thread = self._store.thread
        return thread is not None and thread.is_alive()

    def is_running(self):
        with self._lock:
            return self._is_running_unlocked()

    def active_task_hash(self):
        with self._lock:
            return self._store.task_hash

    def snapshot(self):
        with self._lock:
            return RuntimeSnapshot(
                running=self._is_running_unlocked(),
                starting=bool(self._store.starting),
                task_hash=self._store.task_hash,
                shared_state=dict(self._store.shared_state),
                latest_log=self._store.latest_log,
                status_text=self._store.status_text,
                final_message=self._store.final_message,
                stopping=self._store.stop_event is not None and self._store.stop_event.is_set(),
            )

    def get_runtime_task_bootstrap_data(self):
        snap = self.snapshot()
        if not snap.running or not snap.task_hash:
            return None
        return get_task_config(snap.task_hash)

    def start_processing(self, yaml_content, input_file_path):
        self._console_log("START", f"收到启动请求，file_path={input_file_path}")
        save_yaml(yaml_content)

        configured_task_name = None
        try:
            cfg = yaml.safe_load(yaml_content) or {}
            if isinstance(cfg, dict):
                task_cfg = cfg.get("task", {})
                if isinstance(task_cfg, dict):
                    name = str(task_cfg.get("task_name", "") or "").strip()
                    if name:
                        configured_task_name = name
        except Exception:
            configured_task_name = None

        if not input_file_path or not os.path.exists(input_file_path):
            self._console_log("START", "启动中止：输入文件不存在或未选择。")
            with self._lock:
                self._store.starting = False
                self._store.task_hash = None
                self._store.status_text = "⚠️ 请先选择有效的数据文件！"
                self._store.final_message = "⚠️ 请先选择有效的数据文件！"
            yield {
                "type": "invalid-input",
                "status": "⚠️ 请先选择有效的数据文件！",
                "log": "请选择有效文件后重试。",
                "shared": {"processed": 0, "total": 0, "success": 0, "failed": 0},
            }
            return

        task_hash = get_task_hash(yaml_content, input_file_path)
        self._console_log("START", f"计算任务哈希完成，task_hash={task_hash[:8]}")

        with self._lock:
            if self._is_running_unlocked() or self._store.starting:
                active_hash = self._store.task_hash
                same_task = active_hash == task_hash
                if self._store.starting and not self._is_running_unlocked():
                    status = "任务正在初始化，请稍候再试。"
                else:
                    status = "同一任务已在运行中，请勿重复启动。" if same_task else f"已有任务在运行中（{str(active_hash)[:8]}），请先停止。"

                self._console_log("START", f"拒绝启动：{status}")
                yield {
                    "type": "already-running",
                    "status": f"⚠️ {status}",
                    "log": self._store.latest_log,
                    "shared": dict(self._store.shared_state),
                }
                return

            self._store.starting = True
            self._store.task_hash = task_hash
            self._store.status_text = "⏳ 任务初始化中..."
            self._store.final_message = ""
            self._store.latest_log = ""

        local_stop_event = threading.Event()
        latest_log = [""]
        log_buffer = []  # 日志缓冲，减少 UI 更新频率
        log_buffer_time = [time.time()]  # 上次刷新时间
        shared_state = {"processed": 0, "total": 0, "success": 0, "failed": 0}

        def web_log_callback(msg):
            """日志回调，带缓冲机制（关键优化）"""
            log_buffer.append(msg)
            
            # 每 0.5s 发送一次日志，而不是每条都发送
            now = time.time()
            if now - log_buffer_time[0] > 0.5 and log_buffer:
                # 只保留最后 10 条日志用于显示
                combined = "\n".join(log_buffer[-10:])
                latest_log[0] = combined
                self._console_log("LLM", f"[缓冲发送] {len(log_buffer)} 条日志")
                
                with self._lock:
                    self._store.latest_log = combined
                
                log_buffer.clear()
                log_buffer_time[0] = now

        try:
            self._console_log("INIT", f"开始初始化提取器，task_hash={task_hash[:8]}")
            extractor = UniversalExtractor(yaml_content, input_file_path)

            try:
                input_df = self.get_or_read_data(input_file_path)  # ← 使用缓存读取
                if input_df is not None:
                    input_rows = len(input_df)
                else:
                    input_rows = 0
                self._console_log("INIT", f"输入文件读取成功，rows={input_rows}")
            except Exception:
                input_rows = 0
                self._console_log("INIT", "输入文件读取失败，rows回退为0")

            update_task_metadata(task_hash, task_name=configured_task_name, status="running", input_rows=input_rows)
            self._console_log("META", f"任务状态写入 running，task_hash={task_hash[:8]}，input_rows={input_rows}")

            with self._lock:
                self._store.task_hash = task_hash
                self._store.stop_event = local_stop_event
                self._store.shared_state = shared_state
                self._store.latest_log = ""
                self._store.status_text = "🚀 引擎准备就绪..."
                self._store.final_message = ""
                self._store.starting = False

            yield {
                "type": "initialized",
                "status": "🚀 引擎准备就绪...",
                "log": "连接到数据库...",
                "shared": {"processed": 0, "total": max(1, input_rows), "success": 0, "failed": 0},
            }

            final_result = [""]

            def run_thread():
                final_result[0] = extractor.run(stop_event=local_stop_event, shared_state=shared_state, log_callback=web_log_callback)

            thread = threading.Thread(target=run_thread, daemon=True)
            with self._lock:
                self._store.thread = thread
            thread.start()
            self._console_log("RUN", f"工作线程已启动，task_hash={task_hash[:8]}")

            last_progress = (-1, -1, -1, -1)
            last_log = ""
            while thread.is_alive():
                progress_now = (
                    shared_state["processed"],
                    shared_state["total"],
                    shared_state["success"],
                    shared_state["failed"],
                )
                log_changed = latest_log[0] != last_log
                progress_changed = progress_now != last_progress

                # 只在进度或日志实际改变时发送事件（事件驱动，而非固定轮询）
                if log_changed or progress_changed:
                    status_str = (
                        f"⏳ 提取中... [ 进度: {shared_state['processed']} / {shared_state['total']} ] | "
                        f"成功: {shared_state['success']} | 失败: {shared_state['failed']}"
                    )
                    with self._lock:
                        self._store.status_text = status_str
                    
                    yield {
                        "type": "progress",
                        "status": status_str,
                        "log": latest_log[0],
                        "shared": dict(shared_state),
                    }
                    
                    if progress_changed:
                        self._console_log(
                            "PROGRESS",
                            f"processed={progress_now[0]}/{progress_now[1]}, success={progress_now[2]}, failed={progress_now[3]}",
                        )
                        last_progress = progress_now
                    
                    if log_changed:
                        last_log = latest_log[0]
                
                time.sleep(0.05)  # 小间隔让出CPU，但不作为事件驱动的触发

            thread.join()
            self._console_log("RUN", f"工作线程结束，task_hash={task_hash[:8]}")

            output_rows = shared_state["success"]
            final_status = "completed" if not local_stop_event.is_set() else "stopped"
            update_task_metadata(task_hash, status=final_status, output_rows=output_rows)
            self._console_log("META", f"任务状态写入 {final_status}，task_hash={task_hash[:8]}，output_rows={output_rows}")

            final_log = (latest_log[0] + "\n\n🏁 运行已结束，可进行导出。") if latest_log[0] else "🏁 运行已结束，可进行导出。"
            with self._lock:
                self._store.thread = None
                self._store.stop_event = None
                self._store.status_text = final_result[0]
                self._store.final_message = final_result[0]
                self._store.latest_log = final_log
                self._store.starting = False
                self._store.task_hash = None

            yield {
                "type": "finished",
                "status": final_result[0],
                "log": final_log,
                "shared": dict(shared_state),
                "final_status": final_status,
            }
            self._console_log("DONE", f"任务完成，task_hash={task_hash[:8]}，result={final_result[0]}")

        except Exception as e:
            self._console_log("ERROR", f"任务异常，task_hash={task_hash[:8]}，error={str(e)}")
            try:
                update_task_metadata(task_hash, status="error", output_rows=shared_state.get("success", 0))
                self._console_log("META", f"任务状态写入 error，task_hash={task_hash[:8]}，output_rows={shared_state.get('success', 0)}")
            except Exception:
                self._console_log("META", "任务状态写入 error 失败。")

            with self._lock:
                self._store.thread = None
                self._store.stop_event = None
                self._store.status_text = f"❌ 运行报错: {str(e)}"
                self._store.final_message = f"❌ 运行报错: {str(e)}"
                self._store.latest_log = self._store.latest_log
                self._store.starting = False
                self._store.task_hash = None

            yield {
                "type": "error",
                "status": f"❌ 运行报错: {str(e)}",
                "log": self._store.latest_log,
                "shared": dict(shared_state),
            }

    def trigger_stop(self):
        with self._lock:
            if not self._is_running_unlocked() or self._store.stop_event is None:
                self._console_log("STOP", "忽略停止请求：当前无运行任务。")
                return {
                    "ok": False,
                    "status": "当前无运行任务。",
                    "log": self._store.latest_log,
                    "shared": {"processed": 0, "total": 0, "success": 0, "failed": 0},
                    "stopping": False,
                }

            self._store.stop_event.set()
            self._store.status_text = "🛑 正在安全中断..."
            shared = dict(self._store.shared_state)
            task_hash = self._store.task_hash
            self._console_log("STOP", f"收到停止请求，task_hash={str(task_hash)[:8]}")

        if task_hash:
            update_task_metadata(task_hash, status="stopping")
            self._console_log("META", f"任务状态写入 stopping，task_hash={task_hash[:8]}")

        return {
            "ok": True,
            "status": "🛑 正在安全中断...",
            "log": "已发送停止信号，等待当前批次安全退出...",
            "shared": shared,
            "stopping": True,
        }
