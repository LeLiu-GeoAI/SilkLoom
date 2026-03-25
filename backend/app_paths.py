import os
import sys
from pathlib import Path

APP_DIR_NAME = "SilkLoom"


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def _is_portable_mode() -> bool:
    return os.getenv("SILKLOOM_PORTABLE", "").strip() == "1"


def _documents_root() -> Path:
    # Cross-platform default: keep all runtime files under Documents/SilkLoom.
    return Path.home() / "Documents" / APP_DIR_NAME


def _default_config_root() -> Path:
    return _documents_root()


def _default_data_root() -> Path:
    return _documents_root()


def get_config_root() -> Path:
    if env_config_dir := os.getenv("SILKLOOM_CONFIG_DIR"):
        return Path(env_config_dir)

    if _is_portable_mode():
        return _project_root()

    return _default_config_root()


def get_data_root() -> Path:
    if env_data_dir := os.getenv("SILKLOOM_DATA_DIR"):
        return Path(env_data_dir)

    if _is_portable_mode():
        return _project_root()

    return _default_data_root()


def ensure_app_dirs() -> dict:
    config_root = get_config_root()
    data_root = get_data_root()

    config_root.mkdir(parents=True, exist_ok=True)
    data_root.mkdir(parents=True, exist_ok=True)

    db_dir = data_root / "db_cache"
    export_dir = db_dir / "exports"
    log_dir = data_root / "logs"

    db_dir.mkdir(parents=True, exist_ok=True)
    export_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    return {
        "config_root": config_root,
        "config_file": config_root / "config.yml",
        "data_root": data_root,
        "db_dir": db_dir,
        "export_dir": export_dir,
        "log_dir": log_dir,
    }
