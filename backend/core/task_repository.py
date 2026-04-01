import os
import sqlite3
import time
import gc

from backend.core.constants import DB_DIR
from backend.security.secrets_codec import decrypt_config_yaml


def list_all_tasks():
    tasks = []
    if not os.path.exists(DB_DIR):
        return tasks

    for filename in os.listdir(DB_DIR):
        if filename.startswith("task_") and filename.endswith(".db"):
            db_path = os.path.join(DB_DIR, filename)
            task_hash = filename.replace("task_", "").replace(".db", "")

            try:
                with sqlite3.connect(db_path) as conn:
                    exp_row = conn.execute(
                        """
                        SELECT file_path, created_at, updated_at, task_name, description, status, input_rows, output_rows
                        FROM experiments
                        ORDER BY updated_at DESC, id DESC
                        LIMIT 1
                    """
                    ).fetchone()
                    if not exp_row:
                        continue

                    file_path, created_at, updated_at, task_name, desc, status, input_rows, output_rows = exp_row
                    results = conn.execute(
                        "SELECT COUNT(*), SUM(CASE WHEN status='success' THEN 1 ELSE 0 END), "
                        "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) FROM results"
                    ).fetchone()
                    total, success, failed = results if results[0] else (0, 0, 0)

                    tasks.append({
                        "hash": task_hash,
                        "file_path": file_path,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "task_name": task_name or f"任务_{task_hash[:8]}",
                        "description": desc or "",
                        "status": status or "idle",
                        "total": total or 0,
                        "success": success or 0,
                        "failed": failed or 0,
                        "input_rows": input_rows or 0,
                        "output_rows": output_rows or 0,
                    })
            except Exception:
                pass

    return sorted(tasks, key=lambda x: x["updated_at"], reverse=True)


def reconcile_stale_task_statuses():
    if not os.path.exists(DB_DIR):
        return

    for filename in os.listdir(DB_DIR):
        if not (filename.startswith("task_") and filename.endswith(".db")):
            continue

        db_path = os.path.join(DB_DIR, filename)
        try:
            with sqlite3.connect(db_path) as conn:
                row = conn.execute(
                    """
                    SELECT id, status FROM experiments
                    ORDER BY updated_at DESC, id DESC
                    LIMIT 1
                """
                ).fetchone()
                if not row:
                    continue
                row_id, status = row
                if status in ("running", "stopping"):
                    conn.execute(
                        """
                        UPDATE experiments
                        SET status = 'interrupted', updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """,
                        (row_id,),
                    )
        except Exception:
            continue


def get_task_config(task_hash):
    db_path = os.path.join(DB_DIR, f"task_{task_hash}.db")
    if not os.path.exists(db_path):
        return None

    try:
        with sqlite3.connect(db_path) as conn:
            exp_row = conn.execute(
                """
                SELECT file_path, yaml_config, task_name, description, status, input_rows, output_rows
                FROM experiments
                ORDER BY updated_at DESC, id DESC
                LIMIT 1
            """
            ).fetchone()
            if not exp_row:
                return None

            file_path, yaml_config, task_name, desc, status, input_rows, output_rows = exp_row
            results = conn.execute(
                "SELECT COUNT(*), SUM(CASE WHEN status='success' THEN 1 ELSE 0 END), "
                "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) FROM results"
            ).fetchone()
            total, success, failed = results if results[0] else (0, 0, 0)

            return {
                "hash": task_hash,
                "file_path": file_path,
                "yaml_config": decrypt_config_yaml(yaml_config or ""),
                "task_name": task_name or f"任务_{task_hash[:8]}",
                "description": desc or "",
                "status": status,
                "total": total or 0,
                "success": success or 0,
                "failed": failed or 0,
                "input_rows": input_rows or 0,
                "output_rows": output_rows or 0,
            }
    except Exception:
        return None


def update_task_metadata(task_hash, task_name=None, description=None, status=None, input_rows=None, output_rows=None):
    db_path = os.path.join(DB_DIR, f"task_{task_hash}.db")
    if not os.path.exists(db_path):
        return False

    try:
        with sqlite3.connect(db_path) as conn:
            updates = []
            values = []
            if task_name is not None:
                updates.append("task_name = ?")
                values.append(task_name)
            if description is not None:
                updates.append("description = ?")
                values.append(description)
            if status is not None:
                updates.append("status = ?")
                values.append(status)
            if input_rows is not None:
                updates.append("input_rows = ?")
                values.append(input_rows)
            if output_rows is not None:
                updates.append("output_rows = ?")
                values.append(output_rows)

            if updates:
                updates.append("updated_at = CURRENT_TIMESTAMP")
                query = (
                    f"UPDATE experiments SET {', '.join(updates)} "
                    "WHERE id = (SELECT id FROM experiments ORDER BY updated_at DESC, id DESC LIMIT 1)"
                )
                conn.execute(query, values)
        return True
    except Exception:
        return False


def delete_task_db(task_hash):
    db_path = os.path.join(DB_DIR, f"task_{task_hash}.db")
    if not os.path.exists(db_path):
        return False, f"数据库文件不存在: {db_path}"

    sidecars = [f"{db_path}-wal", f"{db_path}-shm", f"{db_path}-journal"]
    last_error = ""
    # 连接刚关闭时 Windows 句柄可能仍在短时间内被占用，适当延长重试窗口。
    for _ in range(20):
        try:
            # 主动触发 checkpoint，尽量收敛 WAL/SHM 状态，减少侧文件占用概率。
            try:
                with sqlite3.connect(db_path, timeout=1) as conn:
                    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass

            gc.collect()

            if os.path.exists(db_path):
                os.remove(db_path)
            for sidecar in sidecars:
                if os.path.exists(sidecar):
                    try:
                        os.remove(sidecar)
                    except Exception:
                        pass
            return True, ""
        except PermissionError as e:
            last_error = str(e)
            time.sleep(0.2)
        except Exception as e:
            return False, str(e)

    if os.path.exists(db_path):
        reason = last_error or "未知原因"
        return False, f"文件被占用或无权限，无法删除: {db_path} | {reason}"
    return True, ""


def clear_task_results(task_hash):
    db_path = os.path.join(DB_DIR, f"task_{task_hash}.db")
    if not os.path.exists(db_path):
        return False

    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM results")
            conn.execute(
                """
                UPDATE experiments
                SET status = 'idle', output_rows = 0, updated_at = CURRENT_TIMESTAMP
                WHERE id = (SELECT id FROM experiments ORDER BY updated_at DESC, id DESC LIMIT 1)
            """
            )
        return True
    except Exception:
        return False
