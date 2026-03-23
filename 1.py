import re
import sqlite3
import time
from datetime import datetime, timezone

import pydirectinput
import pyperclip


DB_PATH = "players.db"

START_DELAY_SECONDS = 5
START_TAB_COUNT = 2
MAX_TAB_COUNT = 500
MAX_EMPTY_IN_A_ROW = 5

OPEN_CHAT_KEY = "t"
CLOSE_CHAT_KEY = "esc"
TAB_KEY = "tab"
BACKSPACE_KEY = "backspace"

CHAT_OPEN_DELAY = 0.45
TAB_DELAY = 0.16
HOTKEY_DELAY = 0.10
CUT_RESULT_DELAY = 0.20
BETWEEN_GROUPS_DELAY = 0.25

SOURCE_NAME = "chat_tab_cut"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS players (
            username   TEXT PRIMARY KEY,
            first_seen TEXT NOT NULL,
            last_seen  TEXT NOT NULL,
            seen_count INTEGER NOT NULL DEFAULT 1,
            source     TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sightings_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            username    TEXT NOT NULL,
            detected_at TEXT NOT NULL,
            source      TEXT NOT NULL
        )
        """
    )
    conn.commit()


def upsert_player(conn: sqlite3.Connection, username: str, source: str) -> None:
    now = utc_now_iso()
    conn.execute(
        """
        INSERT INTO players (username, first_seen, last_seen, seen_count, source)
        VALUES (?, ?, ?, 1, ?)
        ON CONFLICT(username) DO UPDATE SET
            last_seen = excluded.last_seen,
            seen_count = players.seen_count + 1,
            source = excluded.source
        """,
        (username, now, now, source),
    )
    conn.execute(
        """
        INSERT INTO sightings_history (username, detected_at, source)
        VALUES (?, ?, ?)
        """,
        (username, now, source),
    )
    conn.commit()


def normalize_username(raw_text: str) -> str | None:
    if not raw_text:
        return None

    text = raw_text.strip()

    if re.fullmatch(r"[A-Za-z0-9_]{3,16}", text):
        return text

    matches = re.findall(r"(?<![A-Za-z0-9_])([A-Za-z0-9_]{3,16})(?![A-Za-z0-9_])", text)
    if not matches:
        return None

    return matches[-1]


def press_key(key: str, count: int = 1, delay: float = 0.0) -> None:
    for _ in range(count):
        pydirectinput.press(key)
        if delay > 0:
            time.sleep(delay)


def hotkey_ctrl(key: str) -> None:
    pydirectinput.keyDown("ctrl")
    time.sleep(HOTKEY_DELAY)
    pydirectinput.press(key)
    time.sleep(HOTKEY_DELAY)
    pydirectinput.keyUp("ctrl")


def cut_chat_text() -> str:
    marker = f"__MARKER__{time.time()}__"
    pyperclip.copy(marker)
    time.sleep(CUT_RESULT_DELAY)

    hotkey_ctrl("a")
    time.sleep(CUT_RESULT_DELAY)

    hotkey_ctrl("x")
    time.sleep(CUT_RESULT_DELAY)

    return pyperclip.paste()


def open_chat() -> None:
    press_key(OPEN_CHAT_KEY)
    time.sleep(CHAT_OPEN_DELAY)


def close_chat() -> None:
    press_key(CLOSE_CHAT_KEY)
    time.sleep(0.15)


def main():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    seen_this_run = set()
    unique_found = 0
    empty_in_a_row = 0

    print("Скрипт готов.")
    print("Зайди в Minecraft и открой нужный сервер.")
    print("Окно Minecraft должно быть активным.")
    input("Нажми Enter, потом будет 5 секунд на переключение в игру... ")

    for sec in range(START_DELAY_SECONDS, 0, -1):
        print(f"Старт через {sec}...")
        time.sleep(1)

    try:
        for tab_count in range(START_TAB_COUNT, MAX_TAB_COUNT + 1):
            print(f"\n=== Серия: {tab_count} Tab ===")

            open_chat()
            press_key(TAB_KEY, count=tab_count, delay=TAB_DELAY)

            raw_text = cut_chat_text()
            username = normalize_username(raw_text)

            print(f"Вырезано: {raw_text!r}")

            close_chat()

            if not username:
                empty_in_a_row += 1
                print("Ник не распознан.")

                if empty_in_a_row >= MAX_EMPTY_IN_A_ROW:
                    print("Слишком много пустых попыток подряд. Остановка.")
                    break

                time.sleep(BETWEEN_GROUPS_DELAY)
                continue

            empty_in_a_row = 0
            print(f"Распознан ник: {username}")

            if username in seen_this_run:
                print(f"Повтор ника: {username}")
                print("Остановка.")
                break

            seen_this_run.add(username)
            unique_found += 1
            upsert_player(conn, username, SOURCE_NAME)

            time.sleep(BETWEEN_GROUPS_DELAY)

        print("\nГотово.")
        print(f"Уникальных ников: {unique_found}")
        print(f"База: {DB_PATH}")

    finally:
        try:
            close_chat()
        except Exception:
            pass
        conn.close()


if __name__ == "__main__":
    main()