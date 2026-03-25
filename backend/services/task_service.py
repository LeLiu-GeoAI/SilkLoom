import re

from backend.core.task_repository import clear_task_results, delete_task_db, get_task_config, list_all_tasks


def format_task_choice(task):
    return f"{task['task_name']} | {task['hash'][:8]} | {task['status']}"


def find_task_by_selector(task_selector):
    if not task_selector:
        return None

    tasks = list_all_tasks()
    selector = str(task_selector).strip()
    parts = [p.strip() for p in selector.split("|")]
    if len(parts) < 2:
        return None

    hash_hint = parts[1]
    if not re.fullmatch(r"[0-9a-fA-F]{4,32}", hash_hint):
        return None

    for task in tasks:
        if task["hash"].startswith(hash_hint):
            return task
    return None


def get_task_selector_choices():
    tasks = list_all_tasks()
    return [format_task_choice(t) for t in tasks]


def get_task_list_dataframe():
    tasks = list_all_tasks()
    columns = ["任务名", "状态", "数据文件", "总行数", "✅成功", "❌失败", "更新时间"]
    if not tasks:
        return []

    data = []
    for task in tasks:
        file_name = task["file_path"].split("\\")[-1] if task["file_path"] else ""
        data.append({
            columns[0]: task["task_name"],
            columns[1]: task["status"],
            columns[2]: file_name[-30:] if len(file_name) > 30 else file_name,
            columns[3]: task["input_rows"],
            columns[4]: task["success"],
            columns[5]: task["failed"],
            columns[6]: task["updated_at"][-19:] if task["updated_at"] else "",
        })

    return data


def load_task_config(task_selector):
    target_task = find_task_by_selector(task_selector)
    if not target_task:
        return None, None

    config = get_task_config(target_task["hash"])
    if not config:
        return None, None
    return config["yaml_config"], config["file_path"]


def delete_task_action(task_selector, active_task_hash, is_running):
    target_task = find_task_by_selector(task_selector)
    full_hash = target_task["hash"] if target_task else None

    if not full_hash:
        return f"❌ 任务 {task_selector} 不存在"
    if is_running and active_task_hash == full_hash:
        return "❌ 任务正在运行，无法删除。请先停止运行。"

    if delete_task_db(full_hash):
        return "✅ 任务已删除"
    return "❌ 删除失败"


def clear_task_results_action(task_selector, active_task_hash, is_running):
    target_task = find_task_by_selector(task_selector)
    full_hash = target_task["hash"] if target_task else None

    if not full_hash:
        return f"❌ 任务 {task_selector} 不存在"
    if is_running and active_task_hash == full_hash:
        return "❌ 任务正在运行，无法清空。请先停止运行。"

    if clear_task_results(full_hash):
        return "✅ 任务结果已清空"
    return "❌ 清空失败"


class TaskService:
    def find_task(self, task_selector):
        return find_task_by_selector(task_selector)

    def load_config(self, task_selector):
        return load_task_config(task_selector)

    def get_selector_choices(self):
        return get_task_selector_choices()

    def get_list_dataframe(self):
        return get_task_list_dataframe()

    def delete_task(self, task_selector, active_task_hash, is_running):
        return delete_task_action(task_selector, active_task_hash, is_running)

    def clear_results(self, task_selector, active_task_hash, is_running):
        return clear_task_results_action(task_selector, active_task_hash, is_running)
