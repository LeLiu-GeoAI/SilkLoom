"""Application metadata used by build/release scripts."""

APP_NAME = "SilkLoom"

# Prefer CI-generated version so packaged app version matches release tag.
try:
    from backend.core._generated_version import VERSION as APP_VERSION
except Exception:
    APP_VERSION = "1.0.0"

APP_EXECUTABLE = "SilkLoom"
APP_DESCRIPTION = "批量文本提取、分类与总结的跨平台桌面 LLM 工具"
APP_LICENSE = "GPLv3"
APP_COPYRIGHT = "版权所有 © 2025 LeLiu-GeoAI"
APP_SUPPORT_EMAIL = "liule@lreis.ac.cn"
APP_REPO_URL = "https://github.com/LeLiu-GeoAI/SilkLoom"
