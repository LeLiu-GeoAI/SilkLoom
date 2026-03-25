import os
import sqlite3
import uuid
from typing import Dict, List, Optional

from backend.core.constants import DB_DIR

MODEL_DB_PATH = os.path.join(DB_DIR, "models.db")


def _conn():
    conn = sqlite3.connect(MODEL_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_model_db():
    with _conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS model_profiles (
                row_id TEXT PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                api_key TEXT NOT NULL,
                base_url TEXT NOT NULL,
                model TEXT NOT NULL,
                proxy_url TEXT DEFAULT '',
                is_default INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        try:
            conn.execute("ALTER TABLE model_profiles ADD COLUMN proxy_url TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass

        count = conn.execute("SELECT COUNT(*) FROM model_profiles").fetchone()[0]
        if count == 0:
            conn.execute(
                """
                INSERT INTO model_profiles (row_id, name, api_key, base_url, model, proxy_url, is_default)
                VALUES (?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    str(uuid.uuid4()),
                    "默认模型",
                    "YOUR_API_KEY_OR_USE_ENV",
                    "https://open.bigmodel.cn/api/paas/v4/chat/completions",
                    "glm-4-flash",
                    "",
                ),
            )


def list_models() -> List[Dict]:
    init_model_db()
    with _conn() as conn:
        rows = conn.execute(
            """
            SELECT row_id, name, api_key, base_url, model, proxy_url, is_default, created_at, updated_at
            FROM model_profiles
            ORDER BY is_default DESC, updated_at DESC
            """
        ).fetchall()
    return [dict(r) for r in rows]


def get_model_by_name(name: str) -> Optional[Dict]:
    if not name:
        return None
    init_model_db()
    with _conn() as conn:
        row = conn.execute(
            """
            SELECT row_id, name, api_key, base_url, model, proxy_url, is_default
            FROM model_profiles
            WHERE name = ?
            """,
            (name,),
        ).fetchone()
    return dict(row) if row else None


def get_default_model() -> Optional[Dict]:
    init_model_db()
    with _conn() as conn:
        row = conn.execute(
            """
            SELECT row_id, name, api_key, base_url, model, proxy_url, is_default
            FROM model_profiles
            ORDER BY is_default DESC, updated_at DESC
            LIMIT 1
            """
        ).fetchone()
    return dict(row) if row else None


def upsert_model(name: str, api_key: str, base_url: str, model: str, proxy_url: str = "", row_id: str = "", make_default: bool = False) -> bool:
    init_model_db()
    if not name.strip() or not base_url.strip() or not model.strip() or not api_key.strip():
        return False
    try:
        with _conn() as conn:
            if make_default:
                conn.execute("UPDATE model_profiles SET is_default = 0")

            if row_id:
                exists = conn.execute("SELECT 1 FROM model_profiles WHERE row_id = ?", (row_id,)).fetchone()
                if exists:
                    conn.execute(
                        """
                        UPDATE model_profiles
                        SET name = ?, api_key = ?, base_url = ?, model = ?, proxy_url = ?,
                            is_default = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE row_id = ?
                        """,
                        (name.strip(), api_key.strip(), base_url.strip(), model.strip(), proxy_url.strip(), 1 if make_default else 0, row_id),
                    )
                    return True

            conn.execute(
                """
                INSERT INTO model_profiles (row_id, name, api_key, base_url, model, proxy_url, is_default)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (str(uuid.uuid4()), name.strip(), api_key.strip(), base_url.strip(), model.strip(), proxy_url.strip(), 1 if make_default else 0),
            )
        return True
    except sqlite3.IntegrityError:
        return False


def delete_model(row_id: str) -> bool:
    if not row_id:
        return False
    init_model_db()
    with _conn() as conn:
        row = conn.execute("SELECT is_default FROM model_profiles WHERE row_id = ?", (row_id,)).fetchone()
        if not row:
            return False

        conn.execute("DELETE FROM model_profiles WHERE row_id = ?", (row_id,))

        # 删除默认项后，自动提升一个为默认
        count = conn.execute("SELECT COUNT(*) FROM model_profiles").fetchone()[0]
        if count > 0:
            default_count = conn.execute("SELECT COUNT(*) FROM model_profiles WHERE is_default = 1").fetchone()[0]
            if default_count == 0:
                conn.execute(
                    """
                    UPDATE model_profiles
                    SET is_default = 1, updated_at = CURRENT_TIMESTAMP
                    WHERE row_id = (
                        SELECT row_id FROM model_profiles ORDER BY updated_at DESC LIMIT 1
                    )
                    """
                )
    return True
