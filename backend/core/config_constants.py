"""
应用级常量定义，集中管理魔法值和配置参数
"""
import os
from pathlib import Path

# ==================== 文件系统 ====================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db_cache")
EXPORT_DIR = os.path.join(DB_DIR, "exports")
LOG_DIR = os.path.join(BASE_DIR, "logs")

# 创建必要目录
for directory in [DB_DIR, EXPORT_DIR, LOG_DIR]:
    os.makedirs(directory, exist_ok=True)

# ==================== 数据处理 ====================
# 支持的文件格式
SUPPORTED_FILE_FORMATS = {".csv": "CSV", ".xlsx": "Excel", ".xls": "Excel", ".jsonl": "JSONL"}
SUPPORTED_EXPORT_FORMATS = ["CSV", "Excel", "JSONL"]

# 数据读取配置
FILE_READ_RETRY_ATTEMPTS = 10
FILE_READ_RETRY_DELAY = 1  # 秒
FILE_CHUNK_SIZE = 4096 * 1024  # 4MB

# ==================== LLM API ====================
# 默认LLM配置
DEFAULT_LLM_CONFIG = {
    "model": "glm-4-flash",
    "base_url": "https://open.bigmodel.cn/api/paas/v4/chat/completions",
    "temperature": 0.0,
    "max_tokens": 2048,
    "timeout": 60,
}

# API 重试策略
API_MAX_RETRIES = 3
API_RETRY_DELAYS = [2, 4, 8]  # 指数退避延迟秒数
API_TIMEOUT_DEFAULT = 60

# ==================== 并发控制 ====================
DEFAULT_MAX_WORKERS = 10
MIN_MAX_WORKERS = 1
MAX_MAX_WORKERS = 50
BATCH_INSERT_SIZE = 100  # 数据库批量插入的行数

# ==================== UI配置 ====================
# 主窗口
WINDOW_WIDTH = 1360
WINDOW_HEIGHT = 860
WINDOW_TITLE = "SilkLoom - PyQt6"

# 控件配置
CARD_BORDER_STYLE = "QFrame { border: 1px solid #dddddd; border-radius: 8px; }"
TITLE_FONT_SIZE = "22px"
SECTION_TITLE_FONT_SIZE = "16px"

# 通知提示
NOTIFY_DURATION = 2500  # 毫秒
POLLING_INTERVAL = 800  # 毫秒

# ==================== 数据库 ====================
DB_TIMEOUT = 30  # 秒
DB_CHECK_INTERVAL = 0.2  # 秒
DB_COMMIT_INTERVAL = 100  # 处理多少条后提交

# 保留任务数量
MAX_TASK_HISTORY = 50

# ==================== 日志 ====================
LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_LEVEL = "INFO"

# ==================== 错误消息 ====================
ERROR_MESSAGES = {
    "NO_FILE": "⚠️ 请先选择有效的数据文件！",
    "FILE_NOT_FOUND": "❌ 输入文件不存在或无法访问",
    "FILE_OCCUPIED": "❌ 文件被系统占用！",
    "CONFIG_ERROR": "❌ 配置语法错误",
    "YAML_ERROR": "❌ YAML 格式错误",
    "API_KEY_MISSING": "❌ 未配置 API Key",
    "API_ERROR": "❌ 大模型 API 请求失败",
    "MISSING_COLUMNS": "❌ 数据缺少必填列",
    "PARSE_ERROR": "❌ 数据解析失败",
    "ALREADY_RUNNING": "⚠️ 已有任务在运行中",
    "SAME_TASK_RUNNING": "⚠️ 同一任务已在运行中",
}

# ==================== 任务状态 ====================
TASK_STATUS = {
    "IDLE": "idle",
    "RUNNING": "running",
    "STOPPING": "stopping",
    "COMPLETED": "completed",
    "STOPPED": "stopped",
    "ERROR": "error",
    "INTERRUPTED": "interrupted",
}

# ==================== 结果状态 ====================
RESULT_STATUS = {
    "SUCCESS": "success",
    "FAILED": "failed",
    "PENDING": "pending",
}

# ==================== 模板验证 ====================
TEMPLATE_JINJA_PATTERNS = {
    "variable": r"{{.*?}}",  # {{ variable }}
    "if_block": r"{%\s*if.*?%}.*?{%\s*endif\s*%}",  # {% if ... %} ... {% endif %}
    "for_block": r"{%\s*for.*?%}.*?{%\s*endfor\s*%}",  # {% for ... %} ... {% endfor %}
}
