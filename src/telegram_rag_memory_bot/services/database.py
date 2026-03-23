"""
????: database.py
????????? ???? SQLite: ??????? ?????, ?????? ?????????, ?????,
????????? ?????????????, ??????, ??????, ????????? ? ??????????.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path
from typing import Any

import numpy as np

from telegram_rag_memory_bot.schemas import SearchHit
from telegram_rag_memory_bot.utils.dates import format_russian_date_range, parse_iso_date


class _PostgresCursorResult:
    def __init__(self, rows: list[dict[str, Any]] | None = None, rowcount: int = 0, lastrowid: int | None = None) -> None:
        self._rows = list(rows or [])
        self.rowcount = rowcount
        self.lastrowid = lastrowid

    def fetchone(self) -> dict[str, Any] | None:
        return self._rows[0] if self._rows else None

    def fetchall(self) -> list[dict[str, Any]]:
        return list(self._rows)


class _PostgresConnectionAdapter:
    SERIAL_ID_TABLES = {
        "items",
        "chunks",
        "user_events",
        "access_requests",
        "promo_redemptions",
        "managed_answer_options",
        "pending_material_uploads",
        "shifts",
        "site_support_messages",
    }

    def __init__(self, database_url: str) -> None:
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:  # pragma: no cover - depends on optional runtime package
            raise RuntimeError(
                "Для PostgreSQL нужен пакет psycopg. Установите зависимости проекта заново."
            ) from exc
        self._conn = psycopg.connect(database_url, row_factory=dict_row)

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] | None = None) -> _PostgresCursorResult:
        special = self._handle_special_sql(sql)
        if special is not None:
            return special
        translated_sql, returning_id = self._translate_dml_sql(sql)
        with self._conn.cursor() as cursor:
            cursor.execute(translated_sql, tuple(params or ()))
            rows = cursor.fetchall() if cursor.description is not None else []
            lastrowid = None
            if returning_id and rows:
                first = rows[0]
                if isinstance(first, dict) and first.get("id") is not None:
                    lastrowid = int(first["id"])
            return _PostgresCursorResult(rows=rows, rowcount=cursor.rowcount, lastrowid=lastrowid)

    def executescript(self, sql_script: str) -> _PostgresCursorResult:
        for statement in self._split_script(sql_script):
            special = self._handle_special_sql(statement)
            if special is not None:
                continue
            translated = self._translate_ddl_sql(statement)
            if not translated.strip():
                continue
            with self._conn.cursor() as cursor:
                cursor.execute(translated)
        return _PostgresCursorResult()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()

    def _handle_special_sql(self, sql: str) -> _PostgresCursorResult | None:
        stripped = sql.strip().rstrip(";")
        if not stripped:
            return _PostgresCursorResult()
        upper = stripped.upper()
        if not upper.startswith("PRAGMA"):
            return None
        if upper.startswith("PRAGMA TABLE_INFO("):
            match = re.search(r"PRAGMA\s+table_info\(([^)]+)\)", stripped, flags=re.IGNORECASE)
            table_name = match.group(1).strip().strip('"').strip("'") if match else ""
            if not table_name:
                return _PostgresCursorResult()
            with self._conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT column_name AS name
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )
                rows = cursor.fetchall()
            return _PostgresCursorResult(rows=rows, rowcount=len(rows))
        return _PostgresCursorResult()

    @staticmethod
    def _split_script(sql_script: str) -> list[str]:
        return [chunk.strip() for chunk in sql_script.split(";") if chunk.strip()]

    @staticmethod
    def _translate_common_sql(sql: str) -> str:
        translated = re.sub(r"\bCURRENT_TIMESTAMP\b", "CURRENT_TIMESTAMP::text", sql, flags=re.IGNORECASE)
        return translated.replace("?", "%s")

    def _translate_dml_sql(self, sql: str) -> tuple[str, bool]:
        translated = self._translate_common_sql(sql)
        stripped = translated.strip().rstrip(";")
        if not stripped:
            return translated, False
        upper = stripped.upper()
        if not upper.startswith("INSERT INTO") or "RETURNING" in upper:
            return translated, False
        match = re.match(r"INSERT\s+INTO\s+([a-zA-Z_][\w]*)", stripped, flags=re.IGNORECASE)
        if not match:
            return translated, False
        table_name = match.group(1).lower()
        if table_name not in self.SERIAL_ID_TABLES:
            return translated, False
        return f"{stripped} RETURNING id", True

    def _translate_ddl_sql(self, sql: str) -> str:
        translated = self._translate_common_sql(sql)
        translated = re.sub(
            r"INTEGER\s+PRIMARY\s+KEY\s+AUTOINCREMENT",
            "BIGSERIAL PRIMARY KEY",
            translated,
            flags=re.IGNORECASE,
        )
        return translated


class Database:
    def __init__(self, database_path: Path | None, database_url: str = "") -> None:
        self.database_path = database_path
        self.database_url = database_url.strip()
        self.using_postgres = bool(self.database_url)
        if self.using_postgres:
            self.connection = _PostgresConnectionAdapter(self.database_url)
        else:
            if database_path is None:
                raise RuntimeError("Не задан путь к SQLite-базе данных.")
            self.connection = sqlite3.connect(self.database_path)
            self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON;")
        self._initialize()

    def _initialize(self) -> None:
        self.connection.executescript(
            """
            PRAGMA journal_mode = WAL;
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                content_date TEXT NOT NULL,
                content_scope TEXT NOT NULL DEFAULT 'dated',
                source_chat_id INTEGER NOT NULL,
                source_message_id INTEGER NOT NULL,
                source_sender_id INTEGER,
                ingested_by_user_id INTEGER NOT NULL,
                telegram_message_date TEXT,
                item_type TEXT NOT NULL,
                file_name TEXT,
                mime_type TEXT,
                file_size INTEGER,
                caption TEXT,
                summary TEXT NOT NULL,
                knowledge_text TEXT NOT NULL,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_chat_id, source_message_id)
            );

            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                item_id INTEGER NOT NULL,
                chunk_index INTEGER NOT NULL,
                text TEXT NOT NULL,
                embedding_json TEXT NOT NULL,
                FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS daily_user_usage (
                user_id INTEGER NOT NULL,
                usage_date TEXT NOT NULL,
                message_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, usage_date)
            );

            CREATE TABLE IF NOT EXISTS user_preferences (
                user_id INTEGER PRIMARY KEY,
                api_key TEXT,
                custom_prompt TEXT,
                prompt_profile TEXT,
                api_key_validated_at TEXT,
                api_key_last_error TEXT,
                welcome_sent_at TEXT,
                access_password_verified_at TEXT,
                department TEXT,
                bonus_requests INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS user_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                charged INTEGER NOT NULL DEFAULT 0,
                event_date TEXT NOT NULL,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                details_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS daily_department_usage (
                user_id INTEGER NOT NULL,
                usage_date TEXT NOT NULL,
                mode_bucket TEXT NOT NULL,
                usage_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (user_id, usage_date, mode_bucket)
            );

            CREATE TABLE IF NOT EXISTS user_mode_credits (
                user_id INTEGER NOT NULL,
                mode_bucket TEXT NOT NULL,
                credits INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (user_id, mode_bucket)
            );

            CREATE TABLE IF NOT EXISTS access_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                platform TEXT NOT NULL DEFAULT 'telegram',
                request_type TEXT NOT NULL,
                request_name TEXT NOT NULL,
                reason TEXT NOT NULL,
                mode_bucket TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                requested_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                reviewed_at TEXT,
                reviewed_by_user_id INTEGER,
                decision_note TEXT,
                granted_bonus_requests INTEGER NOT NULL DEFAULT 0,
                granted_mode_credits INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                reason TEXT NOT NULL DEFAULT '',
                banned_by_user_id INTEGER,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS promo_codes (
                code TEXT PRIMARY KEY,
                bonus_requests INTEGER NOT NULL,
                note TEXT,
                max_redemptions INTEGER,
                expires_at TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS promo_redemptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                bonus_requests INTEGER NOT NULL,
                redeemed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(code, user_id),
                FOREIGN KEY(code) REFERENCES promo_codes(code) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS custom_commands (
                command_name TEXT PRIMARY KEY,
                response_text TEXT NOT NULL DEFAULT '',
                media_path TEXT,
                notify_admin INTEGER NOT NULL DEFAULT 1,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS managed_answer_options (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                trigger_text TEXT NOT NULL,
                normalized_trigger TEXT NOT NULL,
                match_mode TEXT NOT NULL DEFAULT 'exact',
                option_label TEXT NOT NULL,
                response_text TEXT NOT NULL DEFAULT '',
                media_path TEXT,
                sort_order INTEGER NOT NULL DEFAULT 100,
                enabled INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS pending_material_uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform TEXT NOT NULL DEFAULT 'telegram',
                admin_user_id INTEGER NOT NULL,
                content_date TEXT NOT NULL DEFAULT '',
                content_scope TEXT NOT NULL DEFAULT 'dated',
                description TEXT NOT NULL,
                source_text TEXT NOT NULL DEFAULT '',
                local_file_path TEXT,
                original_file_name TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                claimed_chat_id INTEGER,
                claimed_message_id INTEGER,
                item_id INTEGER,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                claimed_at TEXT,
                completed_at TEXT
            );

            CREATE TABLE IF NOT EXISTS shifts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                normalized_name TEXT NOT NULL UNIQUE,
                date_from TEXT NOT NULL,
                date_to TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS site_accounts (
                username TEXT PRIMARY KEY,
                password_hash TEXT NOT NULL,
                display_name TEXT NOT NULL DEFAULT '',
                platform TEXT NOT NULL,
                platform_user_id INTEGER NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(platform, platform_user_id)
            );

            CREATE TABLE IF NOT EXISTS site_support_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                site_user_id INTEGER NOT NULL DEFAULT 0,
                display_name TEXT NOT NULL DEFAULT '',
                sender_role TEXT NOT NULL,
                message_text TEXT NOT NULL,
                admin_seen INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_items_date ON items(content_date);
            CREATE INDEX IF NOT EXISTS idx_items_scope ON items(content_scope);
            CREATE INDEX IF NOT EXISTS idx_chunks_item_id ON chunks(item_id);
            CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_user_events_date ON user_events(event_date);
            CREATE INDEX IF NOT EXISTS idx_access_requests_status ON access_requests(status, requested_at DESC);
            CREATE INDEX IF NOT EXISTS idx_access_requests_user_id ON access_requests(user_id, requested_at DESC);
            CREATE INDEX IF NOT EXISTS idx_promo_redemptions_code ON promo_redemptions(code);
            CREATE INDEX IF NOT EXISTS idx_custom_commands_enabled ON custom_commands(enabled);
            CREATE INDEX IF NOT EXISTS idx_managed_answer_match ON managed_answer_options(normalized_trigger, match_mode, enabled);
            CREATE INDEX IF NOT EXISTS idx_pending_material_uploads_status ON pending_material_uploads(status, created_at, id);
            CREATE INDEX IF NOT EXISTS idx_shifts_dates ON shifts(date_from, date_to);
            CREATE INDEX IF NOT EXISTS idx_site_accounts_platform ON site_accounts(platform, is_active, username);
            CREATE INDEX IF NOT EXISTS idx_site_support_messages_username ON site_support_messages(username, id);
            CREATE INDEX IF NOT EXISTS idx_site_support_messages_admin_seen ON site_support_messages(admin_seen, sender_role, created_at DESC);
            """
        )
        self._migrate()
        self.connection.commit()

    def _migrate(self) -> None:
        self._ensure_column("items", "content_scope", "TEXT NOT NULL DEFAULT 'dated'")
        self._ensure_column("user_preferences", "welcome_sent_at", "TEXT")
        self._ensure_column("user_preferences", "access_password_verified_at", "TEXT")
        self._ensure_column("user_preferences", "department", "TEXT")
        self._ensure_column("user_preferences", "bonus_requests", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column("user_preferences", "prompt_profile", "TEXT")
        self._ensure_column("pending_material_uploads", "local_file_path", "TEXT")
        self._ensure_column("pending_material_uploads", "original_file_name", "TEXT")
        self._ensure_column("shifts", "normalized_name", "TEXT")
        shift_rows = self.connection.execute("SELECT id, name, date_from, date_to, normalized_name FROM shifts").fetchall()
        for row in shift_rows:
            normalized_name = str(row["normalized_name"] or "").strip()
            if normalized_name:
                continue
            name = str(row["name"] or "").strip() or format_russian_date_range(str(row["date_from"]), str(row["date_to"]))
            self.connection.execute(
                "UPDATE shifts SET name = ?, normalized_name = ? WHERE id = ?",
                (name, self.normalize_text(name), int(row["id"])),
            )
        self._decouple_site_accounts_from_platform_users()

    def _decouple_site_accounts_from_platform_users(self) -> None:
        rows = self.connection.execute(
            """
            SELECT username, platform_user_id
            FROM site_accounts
            WHERE platform_user_id > 0
            ORDER BY username ASC
            """
        ).fetchall()
        if not rows:
            return
        next_site_user_id = self.next_site_platform_user_id()
        for row in rows:
            self.connection.execute(
                """
                UPDATE site_accounts
                SET platform_user_id = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE username = ?
                """,
                (next_site_user_id, str(row["username"])),
            )
            next_site_user_id -= 1

    def _ensure_column(self, table_name: str, column_name: str, column_type: str) -> None:
        rows = self.connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        if column_name in existing_columns:
            return
        self.connection.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def close(self) -> None:
        self.connection.close()

    def upsert_site_account(
        self,
        *,
        username: str,
        password_hash: str,
        display_name: str,
        platform: str,
        platform_user_id: int,
        is_active: bool = True,
    ) -> None:
        normalized_username = username.strip().lower()
        if not normalized_username:
            raise ValueError("Username cannot be empty.")
        normalized_platform = platform.strip().lower()
        if not normalized_platform:
            raise ValueError("Platform cannot be empty.")
        self.connection.execute(
            """
            INSERT INTO site_accounts (
                username,
                password_hash,
                display_name,
                platform,
                platform_user_id,
                is_active
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(username)
            DO UPDATE SET
                password_hash = excluded.password_hash,
                display_name = excluded.display_name,
                platform = excluded.platform,
                platform_user_id = excluded.platform_user_id,
                is_active = excluded.is_active,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                normalized_username,
                password_hash.strip(),
                display_name.strip(),
                normalized_platform,
                int(platform_user_id),
                1 if is_active else 0,
            ),
        )
        self.connection.commit()

    def get_site_account(self, username: str) -> dict[str, Any] | None:
        normalized_username = username.strip().lower()
        if not normalized_username:
            return None
        row = self.connection.execute(
            "SELECT * FROM site_accounts WHERE username = ? AND is_active = 1",
            (normalized_username,),
        ).fetchone()
        return dict(row) if row else None

    def get_site_account_any(self, username: str) -> dict[str, Any] | None:
        normalized_username = username.strip().lower()
        if not normalized_username:
            return None
        row = self.connection.execute(
            "SELECT * FROM site_accounts WHERE username = ?",
            (normalized_username,),
        ).fetchone()
        return dict(row) if row else None

    def next_site_platform_user_id(self) -> int:
        row = self.connection.execute(
            "SELECT MIN(platform_user_id) AS min_user_id FROM site_accounts WHERE platform_user_id < 0"
        ).fetchone()
        min_user_id = row["min_user_id"] if row else None
        if min_user_id is None:
            return -1
        return int(min_user_id) - 1

    def list_site_accounts(self, *, platform: str | None = None, limit: int = 300) -> list[dict[str, Any]]:
        if platform:
            rows = self.connection.execute(
                """
                SELECT * FROM site_accounts
                WHERE platform = ?
                ORDER BY username ASC
                LIMIT ?
                """,
                (platform.strip().lower(), limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """
                SELECT * FROM site_accounts
                ORDER BY platform ASC, username ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def deactivate_site_account(self, username: str, *, platform: str | None = None) -> bool:
        normalized_username = username.strip().lower()
        if not normalized_username:
            return False
        if platform:
            cursor = self.connection.execute(
                """
                UPDATE site_accounts
                SET is_active = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE username = ? AND platform = ?
                """,
                (normalized_username, platform.strip().lower()),
            )
        else:
            cursor = self.connection.execute(
                """
                UPDATE site_accounts
                SET is_active = 0,
                    updated_at = CURRENT_TIMESTAMP
                WHERE username = ?
                """,
                (normalized_username,),
            )
        self.connection.commit()
        return cursor.rowcount > 0

    def create_site_support_message(
        self,
        *,
        username: str,
        site_user_id: int,
        display_name: str,
        sender_role: str,
        message_text: str,
    ) -> int:
        normalized_username = username.strip().lower()
        clean_role = sender_role.strip().lower()
        clean_text = message_text.strip()
        if not normalized_username:
            raise ValueError("Username cannot be empty.")
        if clean_role not in {"user", "admin"}:
            raise ValueError("Unsupported sender role.")
        if not clean_text:
            raise ValueError("Message text cannot be empty.")
        cursor = self.connection.execute(
            """
            INSERT INTO site_support_messages (
                username,
                site_user_id,
                display_name,
                sender_role,
                message_text,
                admin_seen
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_username,
                int(site_user_id),
                display_name.strip(),
                clean_role,
                clean_text,
                1 if clean_role == "admin" else 0,
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def list_site_support_messages(self, username: str, *, limit: int = 200) -> list[dict[str, Any]]:
        normalized_username = username.strip().lower()
        if not normalized_username:
            return []
        rows = self.connection.execute(
            """
            SELECT *
            FROM (
                SELECT *
                FROM site_support_messages
                WHERE username = ?
                ORDER BY id DESC
                LIMIT ?
            )
            ORDER BY id ASC
            """,
            (normalized_username, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_site_support_threads(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT
                base.username,
                COALESCE((
                    SELECT sm.display_name
                    FROM site_support_messages sm
                    WHERE sm.username = base.username
                      AND TRIM(COALESCE(sm.display_name, '')) <> ''
                    ORDER BY sm.id DESC
                    LIMIT 1
                ), '') AS display_name,
                COALESCE((
                    SELECT sm.site_user_id
                    FROM site_support_messages sm
                    WHERE sm.username = base.username
                      AND sm.site_user_id != 0
                    ORDER BY sm.id DESC
                    LIMIT 1
                ), 0) AS site_user_id,
                (
                    SELECT sm.sender_role
                    FROM site_support_messages sm
                    WHERE sm.username = base.username
                    ORDER BY sm.id DESC
                    LIMIT 1
                ) AS last_sender_role,
                (
                    SELECT sm.message_text
                    FROM site_support_messages sm
                    WHERE sm.username = base.username
                    ORDER BY sm.id DESC
                    LIMIT 1
                ) AS last_message_text,
                (
                    SELECT sm.created_at
                    FROM site_support_messages sm
                    WHERE sm.username = base.username
                    ORDER BY sm.id DESC
                    LIMIT 1
                ) AS last_message_at,
                SUM(CASE WHEN base.sender_role = 'user' AND base.admin_seen = 0 THEN 1 ELSE 0 END) AS unread_count,
                COUNT(*) AS message_count
            FROM site_support_messages base
            GROUP BY base.username
            ORDER BY MAX(base.id) DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_site_support_read_by_admin(self, username: str) -> int:
        normalized_username = username.strip().lower()
        if not normalized_username:
            return 0
        cursor = self.connection.execute(
            """
            UPDATE site_support_messages
            SET admin_seen = 1
            WHERE username = ?
              AND sender_role = 'user'
              AND admin_seen = 0
            """,
            (normalized_username,),
        )
        self.connection.commit()
        return int(cursor.rowcount or 0)

    def upsert_item(self, payload: dict[str, Any]) -> int:
        existing = self.connection.execute(
            "SELECT id FROM items WHERE source_chat_id = ? AND source_message_id = ?",
            (payload["source_chat_id"], payload["source_message_id"]),
        ).fetchone()
        content_date = payload.get("content_date") or ""
        content_scope = payload.get("content_scope") or "dated"

        if existing:
            item_id = int(existing["id"])
            self.connection.execute(
                """
                UPDATE items
                SET content_date = ?,
                    content_scope = ?,
                    source_sender_id = ?,
                    ingested_by_user_id = ?,
                    telegram_message_date = ?,
                    item_type = ?,
                    file_name = ?,
                    mime_type = ?,
                    file_size = ?,
                    caption = ?,
                    summary = ?,
                    knowledge_text = ?,
                    metadata_json = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    content_date,
                    content_scope,
                    payload.get("source_sender_id"),
                    payload["ingested_by_user_id"],
                    payload.get("telegram_message_date"),
                    payload["item_type"],
                    payload.get("file_name"),
                    payload.get("mime_type"),
                    payload.get("file_size"),
                    payload.get("caption"),
                    payload["summary"],
                    payload["knowledge_text"],
                    json.dumps(payload.get("metadata", {}), ensure_ascii=False),
                    item_id,
                ),
            )
        else:
            cursor = self.connection.execute(
                """
                INSERT INTO items (
                    content_date,
                    content_scope,
                    source_chat_id,
                    source_message_id,
                    source_sender_id,
                    ingested_by_user_id,
                    telegram_message_date,
                    item_type,
                    file_name,
                    mime_type,
                    file_size,
                    caption,
                    summary,
                    knowledge_text,
                    metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    content_date,
                    content_scope,
                    payload["source_chat_id"],
                    payload["source_message_id"],
                    payload.get("source_sender_id"),
                    payload["ingested_by_user_id"],
                    payload.get("telegram_message_date"),
                    payload["item_type"],
                    payload.get("file_name"),
                    payload.get("mime_type"),
                    payload.get("file_size"),
                    payload.get("caption"),
                    payload["summary"],
                    payload["knowledge_text"],
                    json.dumps(payload.get("metadata", {}), ensure_ascii=False),
                ),
            )
            item_id = int(cursor.lastrowid)

        self.connection.commit()
        return item_id

    def replace_chunks(self, item_id: int, chunks: list[str], embeddings: list[list[float]]) -> None:
        self.connection.execute("DELETE FROM chunks WHERE item_id = ?", (item_id,))
        rows = []
        for index, (chunk_text, embedding) in enumerate(zip(chunks, embeddings)):
            normalized = self._normalize_embedding(embedding)
            rows.append((item_id, index, chunk_text, json.dumps(normalized)))
        self.connection.executemany(
            "INSERT INTO chunks (item_id, chunk_index, text, embedding_json) VALUES (?, ?, ?, ?)",
            rows,
        )
        self.connection.commit()

    def attach_item_source(
        self,
        item_id: int,
        *,
        source_chat_id: int,
        source_message_id: int,
        source_sender_id: int | None = None,
        telegram_message_date: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> bool:
        existing = self.connection.execute(
            "SELECT metadata_json FROM items WHERE id = ?",
            (int(item_id),),
        ).fetchone()
        if existing is None:
            return False
        merged_metadata: dict[str, Any] = {}
        raw_metadata = str(existing["metadata_json"] or "").strip()
        if raw_metadata:
            try:
                parsed = json.loads(raw_metadata)
                if isinstance(parsed, dict):
                    merged_metadata.update(parsed)
            except json.JSONDecodeError:
                pass
        if metadata:
            merged_metadata.update(metadata)
        cursor = self.connection.execute(
            """
            UPDATE items
            SET source_chat_id = ?,
                source_message_id = ?,
                source_sender_id = ?,
                telegram_message_date = COALESCE(?, telegram_message_date),
                metadata_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (
                int(source_chat_id),
                int(source_message_id),
                source_sender_id,
                telegram_message_date,
                json.dumps(merged_metadata, ensure_ascii=False),
                int(item_id),
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def consume_daily_user_message(self, user_id: int, usage_date: str, daily_limit: int) -> tuple[bool, int, int]:
        row = self.connection.execute(
            "SELECT message_count FROM daily_user_usage WHERE user_id = ? AND usage_date = ?",
            (user_id, usage_date),
        ).fetchone()
        current_count = int(row["message_count"]) if row else 0
        if current_count >= daily_limit:
            return False, current_count, 0

        next_count = current_count + 1
        self.connection.execute(
            """
            INSERT INTO daily_user_usage (user_id, usage_date, message_count)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, usage_date)
            DO UPDATE SET message_count = excluded.message_count
            """,
            (user_id, usage_date, next_count),
        )
        self.connection.commit()
        remaining = max(daily_limit - next_count, 0)
        return True, next_count, remaining

    def consume_daily_department_mode(self, user_id: int, usage_date: str, mode_bucket: str, daily_limit: int = 1) -> tuple[bool, int, int]:
        row = self.connection.execute(
            "SELECT usage_count FROM daily_department_usage WHERE user_id = ? AND usage_date = ? AND mode_bucket = ?",
            (user_id, usage_date, mode_bucket),
        ).fetchone()
        current_count = int(row["usage_count"]) if row else 0
        if current_count >= daily_limit:
            return False, current_count, 0
        next_count = current_count + 1
        self.connection.execute(
            """
            INSERT INTO daily_department_usage (user_id, usage_date, mode_bucket, usage_count)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, usage_date, mode_bucket)
            DO UPDATE SET usage_count = excluded.usage_count
            """,
            (user_id, usage_date, mode_bucket, next_count),
        )
        self.connection.commit()
        remaining = max(daily_limit - next_count, 0)
        return True, next_count, remaining

    def add_mode_credits(self, user_id: int, mode_bucket: str, credits: int) -> None:
        safe_credits = max(int(credits), 0)
        if safe_credits <= 0:
            return
        self.connection.execute(
            """
            INSERT INTO user_mode_credits (user_id, mode_bucket, credits)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, mode_bucket)
            DO UPDATE SET
                credits = COALESCE(user_mode_credits.credits, 0) + excluded.credits,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, mode_bucket, safe_credits),
        )
        self.connection.commit()

    def get_mode_credits(self, user_id: int, mode_bucket: str) -> int:
        row = self.connection.execute(
            "SELECT credits FROM user_mode_credits WHERE user_id = ? AND mode_bucket = ?",
            (user_id, mode_bucket),
        ).fetchone()
        return max(int(row["credits"] or 0), 0) if row else 0

    def consume_mode_credit(self, user_id: int, mode_bucket: str) -> tuple[bool, int]:
        current_credits = self.get_mode_credits(user_id, mode_bucket)
        if current_credits <= 0:
            return False, 0
        next_credits = current_credits - 1
        self.connection.execute(
            """
            INSERT INTO user_mode_credits (user_id, mode_bucket, credits)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id, mode_bucket)
            DO UPDATE SET
                credits = excluded.credits,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, mode_bucket, next_credits),
        )
        self.connection.commit()
        return True, next_credits

    def log_user_event(
        self,
        *,
        user_id: int,
        chat_id: int,
        event_type: str,
        event_date: str,
        charged: bool = False,
        username: str | None = None,
        first_name: str | None = None,
        last_name: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO user_events (
                user_id,
                chat_id,
                event_type,
                charged,
                event_date,
                username,
                first_name,
                last_name,
                details_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user_id,
                chat_id,
                event_type,
                int(charged),
                event_date,
                username,
                first_name,
                last_name,
                json.dumps(details or {}, ensure_ascii=False),
            ),
        )
        self.connection.commit()

    def get_user_statistics(self, stats_date: str, limit: int = 300, user_id: int | None = None) -> list[dict[str, Any]]:
        filters = []
        params: list[Any] = [stats_date, stats_date]
        if user_id is not None:
            filters.append("user_id = ?")
            params.append(user_id)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        rows = self.connection.execute(
            f"""
            WITH aggregated AS (
                SELECT
                    user_id,
                    MIN(created_at) AS first_seen_at,
                    MAX(created_at) AS last_seen_at,
                    COUNT(*) AS total_event_count,
                    SUM(CASE WHEN event_type = 'ask' THEN 1 ELSE 0 END) AS ask_count,
                    SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS search_count,
                    SUM(CASE WHEN event_type = 'list' THEN 1 ELSE 0 END) AS list_count,
                    SUM(CASE WHEN event_type = 'file' THEN 1 ELSE 0 END) AS file_count,
                    SUM(CASE WHEN event_type = 'manual_add' THEN 1 ELSE 0 END) AS manual_add_count,
                    SUM(CASE WHEN event_type = 'settings' THEN 1 ELSE 0 END) AS settings_count,
                    SUM(CASE WHEN event_type = 'unknown_command' THEN 1 ELSE 0 END) AS unknown_command_count,
                    SUM(CASE WHEN event_type = 'delivery_prompt' THEN 1 ELSE 0 END) AS delivery_prompt_count,
                    SUM(CASE WHEN event_type = 'delivery_choice' THEN 1 ELSE 0 END) AS delivery_choice_count,
                    SUM(CASE WHEN event_type = 'text_answer' THEN 1 ELSE 0 END) AS text_answer_count,
                    SUM(CASE WHEN event_type = 'media_delivery' THEN 1 ELSE 0 END) AS media_delivery_count,
                    SUM(CASE WHEN charged = 1 THEN 1 ELSE 0 END) AS charged_total_count,
                    SUM(CASE WHEN event_date = ? THEN 1 ELSE 0 END) AS total_today_count,
                    SUM(CASE WHEN event_date = ? AND charged = 1 THEN 1 ELSE 0 END) AS charged_today_count
                FROM user_events
                {where_sql}
                GROUP BY user_id
            ),
            latest AS (
                SELECT ue.user_id, ue.username, ue.first_name, ue.last_name
                FROM user_events ue
                INNER JOIN (
                    SELECT user_id, MAX(id) AS max_id
                    FROM user_events
                    GROUP BY user_id
                ) latest_events ON latest_events.max_id = ue.id
            )
            SELECT
                aggregated.*,
                COALESCE(latest.username, '') AS username,
                COALESCE(latest.first_name, '') AS first_name,
                COALESCE(latest.last_name, '') AS last_name,
                COALESCE(up.department, '') AS department,
                COALESCE(up.bonus_requests, 0) AS bonus_requests,
                COALESCE(up.prompt_profile, '') AS prompt_profile,
                CASE WHEN TRIM(COALESCE(up.api_key, '')) <> '' THEN 1 ELSE 0 END AS has_api,
                CASE WHEN TRIM(COALESCE(up.custom_prompt, '')) <> '' THEN 1 ELSE 0 END AS has_prompt,
                CASE WHEN bu.is_active = 1 THEN 1 ELSE 0 END AS is_banned,
                COALESCE(bu.reason, '') AS ban_reason
            FROM aggregated
            LEFT JOIN latest ON latest.user_id = aggregated.user_id
            LEFT JOIN user_preferences up ON up.user_id = aggregated.user_id
            LEFT JOIN banned_users bu ON bu.user_id = aggregated.user_id AND bu.is_active = 1
            ORDER BY aggregated.last_seen_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_user_preferences(self, user_id: int) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT * FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row is None:
            return {
                "user_id": user_id,
                "api_key": None,
                "custom_prompt": None,
                "prompt_profile": None,
                "api_key_validated_at": None,
                "api_key_last_error": None,
                "welcome_sent_at": None,
                "access_password_verified_at": None,
                "department": None,
                "bonus_requests": 0,
            }
        return dict(row)

    def set_user_api_key(self, user_id: int, api_key: str) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, api_key, api_key_validated_at, api_key_last_error)
            VALUES (?, ?, CURRENT_TIMESTAMP, NULL)
            ON CONFLICT(user_id)
            DO UPDATE SET
                api_key = excluded.api_key,
                api_key_validated_at = CURRENT_TIMESTAMP,
                api_key_last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, api_key),
        )
        self.connection.commit()

    def clear_user_api_key(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, api_key, api_key_validated_at, api_key_last_error)
            VALUES (?, NULL, NULL, NULL)
            ON CONFLICT(user_id)
            DO UPDATE SET
                api_key = NULL,
                api_key_validated_at = NULL,
                api_key_last_error = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id,),
        )
        self.connection.commit()

    def set_user_api_key_error(self, user_id: int, error_text: str) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, api_key_last_error)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                api_key_last_error = excluded.api_key_last_error,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, error_text[:500]),
        )
        self.connection.commit()

    def set_user_prompt(self, user_id: int, prompt: str) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, custom_prompt)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                custom_prompt = excluded.custom_prompt,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, prompt),
        )
        self.connection.commit()

    def set_user_prompt_profile(self, user_id: int, prompt_profile: str) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, prompt_profile)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                prompt_profile = excluded.prompt_profile,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, prompt_profile[:120]),
        )
        self.connection.commit()

    def clear_user_prompt_profile(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, prompt_profile)
            VALUES (?, NULL)
            ON CONFLICT(user_id)
            DO UPDATE SET
                prompt_profile = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id,),
        )
        self.connection.commit()

    def clear_user_prompt(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, custom_prompt)
            VALUES (?, NULL)
            ON CONFLICT(user_id)
            DO UPDATE SET
                custom_prompt = NULL,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id,),
        )
        self.connection.commit()

    def set_user_department(self, user_id: int, department: str) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, department)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                department = excluded.department,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, department[:120]),
        )
        self.connection.commit()

    def add_bonus_requests(self, user_id: int, bonus_requests: int) -> None:
        safe_bonus = max(int(bonus_requests), 0)
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, bonus_requests)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                bonus_requests = COALESCE(user_preferences.bonus_requests, 0) + excluded.bonus_requests,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, safe_bonus),
        )
        self.connection.commit()

    def get_user_bonus_requests(self, user_id: int) -> int:
        row = self.connection.execute(
            "SELECT bonus_requests FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if row is None:
            return 0
        return max(int(row["bonus_requests"] or 0), 0)

    def consume_bonus_request(self, user_id: int) -> tuple[bool, int]:
        current_bonus = self.get_user_bonus_requests(user_id)
        if current_bonus <= 0:
            return False, 0
        next_bonus = current_bonus - 1
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, bonus_requests)
            VALUES (?, ?)
            ON CONFLICT(user_id)
            DO UPDATE SET
                bonus_requests = ?,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, next_bonus, next_bonus),
        )
        self.connection.commit()
        return True, next_bonus

    def create_promo_code(
        self,
        code: str,
        *,
        bonus_requests: int,
        note: str = "",
        max_redemptions: int | None = None,
        expires_at: str | None = None,
        enabled: bool = True,
    ) -> None:
        normalized_code = code.strip().upper()
        self.connection.execute(
            """
            INSERT INTO promo_codes (code, bonus_requests, note, max_redemptions, expires_at, enabled)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(code)
            DO UPDATE SET
                bonus_requests = excluded.bonus_requests,
                note = excluded.note,
                max_redemptions = excluded.max_redemptions,
                expires_at = excluded.expires_at,
                enabled = excluded.enabled,
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized_code, int(bonus_requests), note.strip(), max_redemptions, expires_at, int(enabled)),
        )
        self.connection.commit()

    def delete_promo_code(self, code: str) -> bool:
        normalized_code = code.strip().upper()
        cursor = self.connection.execute("DELETE FROM promo_codes WHERE code = ?", (normalized_code,))
        self.connection.commit()
        return cursor.rowcount > 0

    def list_promo_codes(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT pc.*, COUNT(pr.id) AS redeemed_count
            FROM promo_codes pc
            LEFT JOIN promo_redemptions pr ON pr.code = pc.code
            GROUP BY pc.code
            ORDER BY pc.created_at DESC, pc.code ASC
            """
        ).fetchall()
        return [dict(row) for row in rows]

    def redeem_promo_code(self, user_id: int, code: str, today_iso: str) -> tuple[bool, str, int]:
        normalized_code = code.strip().upper()
        row = self.connection.execute(
            """
            SELECT pc.*, COUNT(pr.id) AS redeemed_count
            FROM promo_codes pc
            LEFT JOIN promo_redemptions pr ON pr.code = pc.code
            WHERE pc.code = ?
            GROUP BY pc.code
            """,
            (normalized_code,),
        ).fetchone()
        if row is None:
            return False, "Промокод не найден.", 0
        if not int(row["enabled"] or 0):
            return False, "Промокод отключен.", 0
        expires_at = str(row["expires_at"] or "").strip()
        if expires_at and expires_at < today_iso:
            return False, f"Срок действия промокода истек: {expires_at}.", 0
        if row["max_redemptions"] is not None and int(row["redeemed_count"] or 0) >= int(row["max_redemptions"] or 0):
            return False, "Лимит активаций этого промокода исчерпан.", 0
        existing = self.connection.execute(
            "SELECT 1 FROM promo_redemptions WHERE code = ? AND user_id = ?",
            (normalized_code, user_id),
        ).fetchone()
        if existing is not None:
            return False, "Вы уже активировали этот промокод.", 0
        bonus_requests = max(int(row["bonus_requests"] or 0), 0)
        self.connection.execute(
            "INSERT INTO promo_redemptions (code, user_id, bonus_requests) VALUES (?, ?, ?)",
            (normalized_code, user_id, bonus_requests),
        )
        self.add_bonus_requests(user_id, bonus_requests)
        self.connection.commit()
        return True, f"Промокод активирован. Добавлено запросов: {bonus_requests}.", bonus_requests

    def create_access_request(
        self,
        *,
        user_id: int,
        platform: str,
        request_type: str,
        request_name: str,
        reason: str,
        mode_bucket: str | None = None,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO access_requests (user_id, platform, request_type, request_name, reason, mode_bucket)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (user_id, platform.strip().lower() or "telegram", request_type.strip(), request_name.strip()[:200], reason.strip()[:2000], mode_bucket),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def list_access_requests(self, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        if status:
            rows = self.connection.execute(
                """
                SELECT *
                FROM access_requests
                WHERE status = ?
                ORDER BY requested_at DESC, id DESC
                LIMIT ?
                """,
                (status.strip().lower(), limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """
                SELECT *
                FROM access_requests
                ORDER BY requested_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def review_access_request(
        self,
        request_id: int,
        *,
        status: str,
        reviewed_by_user_id: int,
        decision_note: str = "",
        granted_bonus_requests: int = 0,
        granted_mode_credits: int = 0,
    ) -> dict[str, Any] | None:
        row = self.connection.execute("SELECT * FROM access_requests WHERE id = ?", (request_id,)).fetchone()
        if row is None:
            return None
        request = dict(row)
        normalized_status = status.strip().lower()
        self.connection.execute(
            """
            UPDATE access_requests
            SET status = ?,
                reviewed_at = CURRENT_TIMESTAMP,
                reviewed_by_user_id = ?,
                decision_note = ?,
                granted_bonus_requests = ?,
                granted_mode_credits = ?
            WHERE id = ?
            """,
            (
                normalized_status,
                reviewed_by_user_id,
                decision_note.strip()[:1000],
                max(int(granted_bonus_requests), 0),
                max(int(granted_mode_credits), 0),
                request_id,
            ),
        )
        if normalized_status == "approved":
            if granted_bonus_requests > 0:
                self.add_bonus_requests(int(request["user_id"]), int(granted_bonus_requests))
            if granted_mode_credits > 0 and request.get("mode_bucket"):
                self.add_mode_credits(int(request["user_id"]), str(request["mode_bucket"]), int(granted_mode_credits))
        self.connection.commit()
        updated = self.connection.execute("SELECT * FROM access_requests WHERE id = ?", (request_id,)).fetchone()
        return dict(updated) if updated else None

    def set_ban(self, user_id: int, *, reason: str, banned_by_user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO banned_users (user_id, reason, banned_by_user_id, is_active)
            VALUES (?, ?, ?, 1)
            ON CONFLICT(user_id)
            DO UPDATE SET
                reason = excluded.reason,
                banned_by_user_id = excluded.banned_by_user_id,
                is_active = 1,
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id, reason.strip()[:1000], banned_by_user_id),
        )
        self.connection.commit()

    def clear_ban(self, user_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE banned_users
            SET is_active = 0,
                updated_at = CURRENT_TIMESTAMP
            WHERE user_id = ? AND is_active = 1
            """,
            (user_id,),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def is_user_banned(self, user_id: int) -> tuple[bool, str]:
        row = self.connection.execute(
            "SELECT reason FROM banned_users WHERE user_id = ? AND is_active = 1",
            (user_id,),
        ).fetchone()
        if row is None:
            return False, ""
        return True, str(row["reason"] or "")

    def list_banned_users(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT *
            FROM banned_users
            WHERE is_active = 1
            ORDER BY updated_at DESC, user_id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def create_or_update_custom_command(
        self,
        command_name: str,
        *,
        platform: str = "telegram",
        response_text: str,
        media_path: str | None,
        notify_admin: bool = True,
        enabled: bool = True,
    ) -> None:
        normalized_command = self.build_custom_command_key(platform, command_name)
        self.connection.execute(
            """
            INSERT INTO custom_commands (command_name, response_text, media_path, notify_admin, enabled)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(command_name)
            DO UPDATE SET
                response_text = excluded.response_text,
                media_path = excluded.media_path,
                notify_admin = excluded.notify_admin,
                enabled = excluded.enabled,
                updated_at = CURRENT_TIMESTAMP
            """,
            (normalized_command, response_text.strip(), media_path, int(notify_admin), int(enabled)),
        )
        self.connection.commit()

    def delete_custom_command(self, command_name: str, platform: str = "telegram") -> bool:
        normalized_command = self.build_custom_command_key(platform, command_name)
        cursor = self.connection.execute("DELETE FROM custom_commands WHERE command_name = ?", (normalized_command,))
        self.connection.commit()
        return cursor.rowcount > 0

    def get_custom_command(self, command_name: str, platform: str = "telegram") -> dict[str, Any] | None:
        normalized_command = self.build_custom_command_key(platform, command_name)
        row = self.connection.execute(
            "SELECT * FROM custom_commands WHERE command_name = ? AND enabled = 1",
            (normalized_command,),
        ).fetchone()
        if not row:
            return None
        return self._decode_custom_command_row(dict(row))

    def list_custom_commands(self, platform: str | None = None) -> list[dict[str, Any]]:
        if platform:
            rows = self.connection.execute(
                "SELECT * FROM custom_commands WHERE command_name LIKE ? ORDER BY updated_at DESC, command_name ASC",
                (f"{platform.strip().lower()}:%",),
            ).fetchall()
        else:
            rows = self.connection.execute(
                "SELECT * FROM custom_commands ORDER BY updated_at DESC, command_name ASC"
            ).fetchall()
        return [self._decode_custom_command_row(dict(row)) for row in rows]

    def create_managed_answer_option(
        self,
        *,
        trigger_text: str,
        match_mode: str,
        option_label: str,
        response_text: str,
        media_path: str | None,
        sort_order: int = 100,
        enabled: bool = True,
    ) -> int:
        normalized_trigger = self.normalize_text(trigger_text)
        cursor = self.connection.execute(
            """
            INSERT INTO managed_answer_options (
                trigger_text,
                normalized_trigger,
                match_mode,
                option_label,
                response_text,
                media_path,
                sort_order,
                enabled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trigger_text.strip(),
                normalized_trigger,
                match_mode,
                option_label.strip(),
                response_text.strip(),
                media_path,
                int(sort_order),
                int(enabled),
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def delete_managed_answer_option(self, option_id: int) -> bool:
        cursor = self.connection.execute("DELETE FROM managed_answer_options WHERE id = ?", (option_id,))
        self.connection.commit()
        return cursor.rowcount > 0

    def list_managed_answer_options(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM managed_answer_options ORDER BY trigger_text ASC, sort_order ASC, id ASC"
        ).fetchall()
        return [dict(row) for row in rows]

    def find_managed_answer_options(self, query_text: str) -> list[dict[str, Any]]:
        normalized_query = self.normalize_text(query_text)
        rows = self.connection.execute(
            "SELECT * FROM managed_answer_options WHERE enabled = 1 ORDER BY match_mode ASC, sort_order ASC, id ASC"
        ).fetchall()
        matches: list[dict[str, Any]] = []
        for row in rows:
            candidate = dict(row)
            trigger = str(candidate.get("normalized_trigger") or "").strip()
            if not trigger:
                continue
            match_mode = str(candidate.get("match_mode") or "exact").strip().lower()
            if match_mode == "exact" and normalized_query == trigger:
                matches.append(candidate)
            elif match_mode == "contains" and trigger in normalized_query:
                matches.append(candidate)
        return matches

    def list_recent_items(self, limit: int = 120) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT id, content_date, content_scope, item_type, file_name, summary, updated_at FROM items ORDER BY updated_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def create_shift(self, *, name: str, date_from: str, date_to: str) -> int:
        safe_date_from = parse_iso_date(date_from)
        safe_date_to = parse_iso_date(date_to)
        if safe_date_from > safe_date_to:
            raise ValueError("Дата начала смены должна быть не позже даты окончания.")
        self._ensure_shift_range_available(safe_date_from, safe_date_to)
        shift_name = str(name or "").strip() or format_russian_date_range(safe_date_from, safe_date_to)
        normalized_name = self.normalize_text(shift_name)
        cursor = self.connection.execute(
            """
            INSERT INTO shifts (name, normalized_name, date_from, date_to)
            VALUES (?, ?, ?, ?)
            """,
            (shift_name, normalized_name, safe_date_from, safe_date_to),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def list_shifts(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT * FROM shifts ORDER BY date_from ASC, date_to ASC, id ASC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def update_shift(self, shift_id: int, *, name: str, date_from: str, date_to: str) -> bool:
        safe_shift_id = int(shift_id)
        safe_date_from = parse_iso_date(date_from)
        safe_date_to = parse_iso_date(date_to)
        if safe_date_from > safe_date_to:
            raise ValueError("Дата начала смены должна быть не позже даты окончания.")
        self._ensure_shift_range_available(safe_date_from, safe_date_to, ignore_shift_id=safe_shift_id)
        shift_name = str(name or "").strip() or format_russian_date_range(safe_date_from, safe_date_to)
        normalized_name = self.normalize_text(shift_name)
        cursor = self.connection.execute(
            """
            UPDATE shifts
            SET name = ?, normalized_name = ?, date_from = ?, date_to = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (shift_name, normalized_name, safe_date_from, safe_date_to, safe_shift_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def delete_shift(self, shift_id: int) -> bool:
        cursor = self.connection.execute("DELETE FROM shifts WHERE id = ?", (int(shift_id),))
        self.connection.commit()
        return cursor.rowcount > 0

    def find_shift_by_query(self, query_text: str) -> dict[str, Any] | None:
        normalized_query = self.normalize_text(query_text)
        if not normalized_query:
            return None
        rows = self.connection.execute("SELECT * FROM shifts ORDER BY date_from ASC, date_to ASC, id ASC").fetchall()
        matches: list[dict[str, Any]] = []
        for row in rows:
            normalized_name = str(row["normalized_name"] or "").strip()
            if not normalized_name:
                continue
            if normalized_query == normalized_name or normalized_name in normalized_query:
                matches.append(dict(row))
        if not matches:
            return None
        matches.sort(
            key=lambda item: (
                len(str(item.get("normalized_name") or "")),
                len(str(item.get("name") or "")),
            ),
            reverse=True,
        )
        return matches[0]

    def find_shift_for_date(self, content_date: str) -> dict[str, Any] | None:
        safe_date = parse_iso_date(content_date)
        row = self.connection.execute(
            """
            SELECT *
            FROM shifts
            WHERE date_from <= ? AND date_to >= ?
            ORDER BY date_from ASC, date_to ASC, id ASC
            LIMIT 1
            """,
            (safe_date, safe_date),
        ).fetchone()
        return dict(row) if row else None

    def _ensure_shift_range_available(self, date_from: str, date_to: str, ignore_shift_id: int | None = None) -> None:
        params: list[Any] = [date_to, date_from]
        sql = """
            SELECT id, name, date_from, date_to
            FROM shifts
            WHERE NOT (date_to < ? OR date_from > ?)
        """
        if ignore_shift_id is not None:
            sql += " AND id != ?"
            params.append(int(ignore_shift_id))
        sql += " ORDER BY date_from ASC, date_to ASC, id ASC LIMIT 1"
        row = self.connection.execute(sql, params).fetchone()
        if row is None:
            return
        existing_name = str(row["name"] or "").strip() or format_russian_date_range(str(row["date_from"]), str(row["date_to"]))
        raise ValueError(
            f"Смена пересекается с уже существующей: {existing_name} ({row['date_from']}..{row['date_to']})."
        )

    def create_pending_material_upload(
        self,
        *,
        platform: str,
        admin_user_id: int,
        content_date: str,
        content_scope: str,
        description: str,
        source_text: str = "",
        local_file_path: str | None = None,
        original_file_name: str | None = None,
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO pending_material_uploads (
                platform,
                admin_user_id,
                content_date,
                content_scope,
                description,
                source_text,
                local_file_path,
                original_file_name
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(platform or "telegram").strip().lower() or "telegram",
                int(admin_user_id),
                str(content_date or "").strip(),
                str(content_scope or "dated").strip() or "dated",
                str(description or "").strip(),
                str(source_text or "").strip(),
                str(local_file_path or "").strip() or None,
                str(original_file_name or "").strip() or None,
            ),
        )
        self.connection.commit()
        return int(cursor.lastrowid)

    def list_pending_material_uploads(self, *, status: str = "pending", platform: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        params: list[Any] = [status.strip().lower()]
        where_sql = "WHERE status = ?"
        if platform:
            where_sql += " AND platform = ?"
            params.append(platform.strip().lower())
        params.append(limit)
        rows = self.connection.execute(
            f"""
            SELECT *
            FROM pending_material_uploads
            {where_sql}
            ORDER BY created_at ASC, id ASC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]

    def consume_pending_material_upload(
        self,
        *,
        claimed_chat_id: int,
        claimed_message_id: int,
        preferred_admin_user_id: int | None = None,
        preferred_platform: str | None = None,
    ) -> dict[str, Any] | None:
        candidates: list[tuple[str, list[Any]]] = []
        if preferred_admin_user_id is not None and preferred_platform:
            candidates.append(
                (
                    "SELECT * FROM pending_material_uploads WHERE status = 'pending' AND admin_user_id = ? AND platform = ? ORDER BY created_at ASC, id ASC LIMIT 1",
                    [int(preferred_admin_user_id), preferred_platform.strip().lower()],
                )
            )
        if preferred_admin_user_id is not None:
            candidates.append(
                (
                    "SELECT * FROM pending_material_uploads WHERE status = 'pending' AND admin_user_id = ? ORDER BY created_at ASC, id ASC LIMIT 1",
                    [int(preferred_admin_user_id)],
                )
            )
        if preferred_platform:
            candidates.append(
                (
                    "SELECT * FROM pending_material_uploads WHERE status = 'pending' AND platform = ? ORDER BY created_at ASC, id ASC LIMIT 1",
                    [preferred_platform.strip().lower()],
                )
            )
        candidates.append(
            (
                "SELECT * FROM pending_material_uploads WHERE status = 'pending' ORDER BY created_at ASC, id ASC LIMIT 1",
                [],
            )
        )

        row: sqlite3.Row | None = None
        for sql, params in candidates:
            row = self.connection.execute(sql, params).fetchone()
            if row is not None:
                break
        if row is None:
            return None

        pending_id = int(row["id"])
        self.connection.execute(
            """
            UPDATE pending_material_uploads
            SET status = 'matched',
                claimed_chat_id = ?,
                claimed_message_id = ?,
                claimed_at = CURRENT_TIMESTAMP
            WHERE id = ? AND status = 'pending'
            """,
            (int(claimed_chat_id), int(claimed_message_id), pending_id),
        )
        updated = self.connection.execute("SELECT * FROM pending_material_uploads WHERE id = ?", (pending_id,)).fetchone()
        self.connection.commit()
        return dict(updated) if updated else None

    def restore_pending_material_upload(self, pending_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE pending_material_uploads
            SET status = 'pending',
                claimed_chat_id = NULL,
                claimed_message_id = NULL,
                claimed_at = NULL
            WHERE id = ? AND status = 'matched'
            """,
            (int(pending_id),),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def set_pending_material_upload_item(
        self,
        pending_id: int,
        *,
        item_id: int,
        local_file_path: str | None = None,
    ) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE pending_material_uploads
            SET item_id = ?,
                local_file_path = ?,
                claimed_chat_id = NULL,
                claimed_message_id = NULL,
                claimed_at = NULL
            WHERE id = ? AND status = 'pending'
            """,
            (
                int(item_id),
                str(local_file_path or "").strip() or None,
                int(pending_id),
            ),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def complete_pending_material_upload(self, pending_id: int, *, item_id: int) -> bool:
        cursor = self.connection.execute(
            """
            UPDATE pending_material_uploads
            SET status = 'completed',
                item_id = ?,
                completed_at = CURRENT_TIMESTAMP
            WHERE id = ? AND status IN ('pending', 'matched')
            """,
            (int(item_id), int(pending_id)),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def delete_pending_material_upload(self, pending_id: int) -> bool:
        cursor = self.connection.execute(
            "DELETE FROM pending_material_uploads WHERE id = ? AND status = 'pending'",
            (int(pending_id),),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def list_items_in_date_range(self, date_from: str, date_to: str, limit: int = 80) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT *
            FROM items
            WHERE content_scope = 'timeless'
               OR (content_scope != 'timeless' AND content_date >= ? AND content_date <= ?)
            ORDER BY content_date DESC, updated_at DESC
            LIMIT ?
            """,
            (date_from, date_to, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def list_user_events(self, *, user_id: int | None = None, limit: int = 500) -> list[dict[str, Any]]:
        if user_id is not None:
            rows = self.connection.execute(
                """
                SELECT *
                FROM user_events
                WHERE user_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (user_id, limit),
            ).fetchall()
        else:
            rows = self.connection.execute(
                """
                SELECT *
                FROM user_events
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        parsed: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            try:
                payload["details"] = json.loads(payload.pop("details_json", "{}") or "{}")
            except Exception:
                payload["details"] = {}
            parsed.append(payload)
        return parsed

    def has_sent_welcome(self, user_id: int) -> bool:
        row = self.connection.execute(
            "SELECT welcome_sent_at FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return bool(row and row["welcome_sent_at"])

    def has_verified_access_password(self, user_id: int) -> bool:
        row = self.connection.execute(
            "SELECT access_password_verified_at FROM user_preferences WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        return bool(row and row["access_password_verified_at"])

    def mark_access_password_verified(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, access_password_verified_at)
            VALUES (?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id)
            DO UPDATE SET
                access_password_verified_at = COALESCE(user_preferences.access_password_verified_at, excluded.access_password_verified_at),
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id,),
        )
        self.connection.commit()

    def mark_welcome_sent(self, user_id: int) -> None:
        self.connection.execute(
            """
            INSERT INTO user_preferences (user_id, welcome_sent_at)
            VALUES (?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id)
            DO UPDATE SET
                welcome_sent_at = COALESCE(user_preferences.welcome_sent_at, excluded.welcome_sent_at),
                updated_at = CURRENT_TIMESTAMP
            """,
            (user_id,),
        )
        self.connection.commit()

    def delete_item_by_id(self, item_id: int) -> bool:
        cursor = self.connection.execute("DELETE FROM items WHERE id = ?", (item_id,))
        self.connection.commit()
        return cursor.rowcount > 0

    def delete_item_by_source(self, source_chat_id: int, source_message_id: int) -> bool:
        cursor = self.connection.execute(
            "DELETE FROM items WHERE source_chat_id = ? AND source_message_id = ?",
            (source_chat_id, source_message_id),
        )
        self.connection.commit()
        return cursor.rowcount > 0

    def get_item(self, item_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()

    def get_item_by_source(self, source_chat_id: int, source_message_id: int) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM items WHERE source_chat_id = ? AND source_message_id = ?",
            (source_chat_id, source_message_id),
        ).fetchone()

    def list_items_by_date(self, content_date: str, limit: int = 30) -> list[sqlite3.Row]:
        rows = self.connection.execute(
            """
            SELECT *
            FROM items
            WHERE content_scope != 'timeless' AND content_date = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (content_date, limit),
        ).fetchall()
        return list(rows)

    def semantic_search(
        self,
        query_embedding: list[float],
        limit: int,
        date_from: str | None = None,
        date_to: str | None = None,
        min_score: float = 0.15,
    ) -> list[SearchHit]:
        clauses = []
        params: list[Any] = []
        date_clause_parts = []
        date_params: list[Any] = []
        if date_from:
            date_clause_parts.append("items.content_date >= ?")
            date_params.append(date_from)
        if date_to:
            date_clause_parts.append("items.content_date <= ?")
            date_params.append(date_to)
        if date_clause_parts:
            clauses.append(f"(items.content_scope = 'timeless' OR ({' AND '.join(date_clause_parts)}))")
            params.extend(date_params)

        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = self.connection.execute(
            f"""
            SELECT
                chunks.text AS chunk_text,
                chunks.embedding_json,
                items.id AS item_id,
                items.content_date,
                items.content_scope,
                items.item_type,
                items.file_name,
                items.summary,
                items.source_chat_id,
                items.source_message_id,
                items.metadata_json
            FROM chunks
            INNER JOIN items ON items.id = chunks.item_id
            {where_sql}
            """,
            params,
        ).fetchall()

        normalized_query = np.array(self._normalize_embedding(query_embedding), dtype=np.float32)
        hits: list[SearchHit] = []
        for row in rows:
            embedding = np.array(json.loads(row["embedding_json"]), dtype=np.float32)
            score = float(np.dot(normalized_query, embedding))
            if score < min_score:
                continue
            hits.append(
                SearchHit(
                    item_id=int(row["item_id"]),
                    score=score,
                    content_date=row["content_date"],
                    item_type=row["item_type"],
                    file_name=row["file_name"],
                    summary=row["summary"],
                    chunk_text=row["chunk_text"],
                    source_chat_id=int(row["source_chat_id"]),
                    source_message_id=int(row["source_message_id"]),
                    metadata=json.loads(row["metadata_json"] or "{}"),
                    content_scope=str(row["content_scope"] or "dated"),
                )
            )

        hits.sort(key=lambda hit: hit.score, reverse=True)
        return hits[:limit]

    @staticmethod
    def _normalize_embedding(embedding: list[float]) -> list[float]:
        vector = np.array(embedding, dtype=np.float32)
        norm = np.linalg.norm(vector)
        if norm == 0:
            return vector.tolist()
        return (vector / norm).tolist()

    @staticmethod
    def normalize_text(text: str) -> str:
        return " ".join(text.lower().strip().split())

    @classmethod
    def normalize_command_name(cls, command_name: str) -> str:
        raw = command_name.strip().lower()
        if not raw.startswith("/"):
            raw = "/" + raw
        return cls.normalize_text(raw)

    @classmethod
    def build_custom_command_key(cls, platform: str, command_name: str) -> str:
        platform_key = cls.normalize_text(platform or "telegram")
        return f"{platform_key}:{cls.normalize_command_name(command_name)}"

    @classmethod
    def _decode_custom_command_row(cls, row: dict[str, Any]) -> dict[str, Any]:
        raw_key = str(row.get("command_name") or "")
        if ":" not in raw_key:
            row["platform"] = "telegram"
            row["command_name"] = cls.normalize_command_name(raw_key)
            return row
        platform_key, command_name = raw_key.split(":", 1)
        row["platform"] = platform_key
        row["command_name"] = cls.normalize_command_name(command_name)
        return row




