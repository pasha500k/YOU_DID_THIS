"""
Файл: migrate_sqlite_to_postgres.py
Переносит существующую SQLite-базу проекта в PostgreSQL, сохраняя материалы,
лимиты, настройки, смены, индексы и служебные таблицы. Скрипт нужен для
install.sh, чтобы сервер мог обновиться без потери накопленных данных.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


SERIAL_ID_TABLES = [
    "items",
    "chunks",
    "user_events",
    "access_requests",
    "promo_redemptions",
    "managed_answer_options",
    "pending_material_uploads",
    "shifts",
    "site_support_messages",
]

COPY_ORDER = [
    "promo_codes",
    "site_accounts",
    "user_preferences",
    "daily_user_usage",
    "daily_department_usage",
    "user_mode_credits",
    "banned_users",
    "custom_commands",
    "managed_answer_options",
    "access_requests",
    "shifts",
    "pending_material_uploads",
    "items",
    "chunks",
    "promo_redemptions",
    "user_events",
    "site_support_messages",
]


def _sqlite_tables(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def _sqlite_columns(connection: sqlite3.Connection, table_name: str) -> list[str]:
    rows = connection.execute(f"PRAGMA table_info({table_name})").fetchall()
    return [str(row[1]) for row in rows]


def _postgres_columns(pg_connection, table_name: str) -> list[str]:
    with pg_connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _postgres_has_data(pg_connection) -> bool:
    probe_tables = [
        "items",
        "chunks",
        "user_events",
        "site_accounts",
        "promo_codes",
        "access_requests",
        "site_support_messages",
    ]
    with pg_connection.cursor() as cursor:
        for table_name in probe_tables:
            cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
            if cursor.fetchone() is not None:
                return True
    return False


def _copy_table(sqlite_connection: sqlite3.Connection, pg_connection, table_name: str) -> int:
    sqlite_columns = _sqlite_columns(sqlite_connection, table_name)
    postgres_columns = _postgres_columns(pg_connection, table_name)
    common_columns = [column for column in sqlite_columns if column in postgres_columns]
    if not common_columns:
        return 0

    sqlite_rows = sqlite_connection.execute(
        f"SELECT {', '.join(common_columns)} FROM {table_name}"
    ).fetchall()
    if not sqlite_rows:
        return 0

    column_sql = ", ".join(common_columns)
    placeholders = ", ".join(["%s"] * len(common_columns))
    insert_sql = f"INSERT INTO {table_name} ({column_sql}) VALUES ({placeholders})"

    with pg_connection.cursor() as cursor:
        for row in sqlite_rows:
            cursor.execute(insert_sql, [row[column] for column in common_columns])
    return len(sqlite_rows)


def _reset_sequences(pg_connection) -> None:
    with pg_connection.cursor() as cursor:
        for table_name in SERIAL_ID_TABLES:
            cursor.execute(
                f"SELECT setval(pg_get_serial_sequence(%s, 'id'), COALESCE(MAX(id), 1), MAX(id) IS NOT NULL) FROM {table_name}",
                (table_name,),
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite-path", required=True)
    parser.add_argument("--database-url", required=True)
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite_path)
    database_url = args.database_url.strip()
    if not sqlite_path.exists():
        print(f"[migrate] SQLite база не найдена, пропускаю: {sqlite_path}")
        return 0
    if not database_url:
        print("[migrate] DATABASE_URL пустой, перенос пропущен.")
        return 0

    from psycopg import connect
    from telegram_rag_memory_bot.services.database import Database

    bootstrap = Database(None, database_url)
    bootstrap.close()

    sqlite_connection = sqlite3.connect(sqlite_path)
    sqlite_connection.row_factory = sqlite3.Row
    postgres_connection = connect(database_url)

    try:
        if _postgres_has_data(postgres_connection):
            print("[migrate] PostgreSQL уже содержит данные, автоматический перенос пропускаю.")
            return 0

        existing_tables = _sqlite_tables(sqlite_connection)
        copied_total = 0
        for table_name in COPY_ORDER:
            if table_name not in existing_tables:
                continue
            copied = _copy_table(sqlite_connection, postgres_connection, table_name)
            if copied:
                print(f"[migrate] {table_name}: перенесено {copied} строк")
                copied_total += copied

        _reset_sequences(postgres_connection)
        postgres_connection.commit()
        print(f"[migrate] Готово, всего перенесено строк: {copied_total}")
        return 0
    finally:
        postgres_connection.close()
        sqlite_connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
