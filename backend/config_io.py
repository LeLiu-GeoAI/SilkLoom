import os
import sys
from pathlib import Path


def _get_config_dir() -> Path:
    """Return config directory, supports portable mode."""
    if env_config := os.getenv("SILKLOOM_CONFIG_DIR"):
        return Path(env_config)

    if sys.platform == "win32":
        appdata = os.getenv("APPDATA", str(Path.home()))
        user_config_dir = Path(appdata) / "SilkLoom"
    else:
        if sys.platform == "darwin":
            user_config_dir = Path.home() / "Library" / "Application Support" / "SilkLoom"
        else:
            user_config_dir = Path.home() / ".config" / "SilkLoom"

    user_config_dir.mkdir(parents=True, exist_ok=True)
    return user_config_dir


def _get_config_file() -> Path:
    base_dir = Path(__file__).resolve().parent.parent
    portable_config = base_dir / "config.yml"

    # Prefer workspace-local config when available or writable.
    if portable_config.exists():
        return portable_config

    if os.access(base_dir, os.W_OK):
        return portable_config

    config_dir = _get_config_dir()
    config_file = config_dir / "config.yml"
    if config_file.exists():
        return config_file

    return config_file


CONFIG_FILE = str(_get_config_file())

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
  target_schema:
    研究背景: 一句话概括研究背景（领域+现有问题）
    研究意义: 一句话说明研究价值（解决什么/带来什么）
    研究问题: 一句话说明核心研究问题（要解决什么）
    研究结论: 一句话总结主要结论（发现/方法/效果）

  prompt_template: |
    你是一名专业的信息抽取专家，擅长从学术论文标题和摘要中提炼关键信息。
    请根据提供的【标题】和【摘要】，填写下方 JSON 中的各字段内容。
    【填写要求】
    1. 每个字段用一句话概括（不超过30字）。
    2. 必须基于原文，不得编造。
    3. 若信息缺失，请填写“未明确说明”。
    4. 表达简洁、客观，避免重复。
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
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            f.write(yaml_str)
            f.flush()
            os.fsync(f.fileno())
        return True
    except Exception:
        return False


def load_default_yaml():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                return f.read()
        except Exception:
            pass

    save_yaml(DEFAULT_YAML)
    return DEFAULT_YAML
