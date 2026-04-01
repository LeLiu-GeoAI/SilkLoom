import os

from backend.app_paths import ensure_app_dirs
from backend.security.secrets_codec import decrypt_config_yaml, encrypt_config_yaml


CONFIG_FILE = str(ensure_app_dirs()["config_file"])

DEFAULT_YAML = """llm:
  api_key: \"YOUR_API_KEY_OR_USE_ENV\"
  base_url: \"https://open.bigmodel.cn/api/paas/v4/chat/completions\"
  model: \"glm-4-flash\"
  max_tokens: 2048
  temperature: 0.0
  max_try: 3
  timeout: 60
  retry_delay: 2

run:
  max_workers: 10
  shuffle_before_run: true

task:
  # 多图请求可单独配置token上限；留空则继承max_tokens
  image_max_tokens: 2048
  # 输入数据中表示图片路径的列名（相对于输入数据文件所在目录），不使用图片可留空
  # 是否在带图请求中发送temperature
  include_temperature_with_images: true
  # 是否在带图请求中发送reasoning（enable_think=true时）
  include_reasoning_with_images: false
  image_path_field: ""
  # 接口报图片超限时是否自动降采样重试
  image_limit_retry_enabled: true
  # 触发自动降采样的错误匹配规则
  image_limit_error_patterns: ["输入图片数量超过限制"]
  # 每次重试图片保留比例（0-1）
  image_limit_reduction_factor: 0.5

  # 输入数据中表示图片路径的列名（相对于输入数据文件所在目录），不使用图片可留空
    你是一名专业的信息抽取专家，擅长从学术论文标题和摘要中提炼关键信息。
    【字段说明】
    - 研究背景：该研究所处的领域背景或已有问题
    - 研究意义：该研究的价值或作用（为什么重要）
    - 研究问题：论文具体要解决的核心问题
    - 研究结论：最终得到的主要发现或结果
    【输入数据】
    标题：{{ row.get('Article Title') }}
    摘要：{{ row.get('Abstract') }}
    【输出格式】
    {{ schema }}
"""


def save_yaml(yaml_str):
    try:
        yaml_to_save = encrypt_config_yaml(yaml_str)
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            f.write(yaml_to_save)
            f.flush()
            os.fsync(f.fileno())
        return True
    except Exception:
        return False


def load_default_yaml():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                stored_yaml = f.read()
            return decrypt_config_yaml(stored_yaml)
        except Exception:
            pass

    save_yaml(DEFAULT_YAML)
    return DEFAULT_YAML
