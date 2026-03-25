import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_DIR = os.path.join(BASE_DIR, "db_cache")
EXPORT_DIR = os.path.join(DB_DIR, "exports")

if not os.path.exists(DB_DIR):
    os.makedirs(DB_DIR)

if not os.path.exists(EXPORT_DIR):
    os.makedirs(EXPORT_DIR)
