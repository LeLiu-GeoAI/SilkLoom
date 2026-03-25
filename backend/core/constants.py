import os

from backend.app_paths import ensure_app_dirs

_PATHS = ensure_app_dirs()
BASE_DIR = str(_PATHS["data_root"])
DB_DIR = str(_PATHS["db_dir"])
EXPORT_DIR = str(_PATHS["export_dir"])

if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)

if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)
