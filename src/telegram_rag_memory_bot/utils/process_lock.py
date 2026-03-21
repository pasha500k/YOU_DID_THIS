"""
Файл: utils/process_lock.py
Держит простой межпроцессный lock-файл, чтобы не запускать несколько копий
одного и того же runtime одновременно и не ловить конфликты Telegram polling.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TextIO


class ProcessLock:
    """
    File-based process lock used to prevent duplicate bot instances.
    """

    def __init__(self, path: Path, *, label: str) -> None:
        self.path = path
        self.label = label
        self._handle: TextIO | None = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            self._lock_handle(handle)
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()))
            handle.flush()
            self._handle = handle
        except Exception:
            handle.close()
            raise RuntimeError(
                f"{self.label} is already running. Stop the other python.exe process and start only one copy."
            )

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            self._unlock_handle(handle)
        finally:
            handle.close()
            self._handle = None

    @staticmethod
    def _lock_handle(handle: TextIO) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            return

        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock_handle(handle: TextIO) -> None:
        if os.name == "nt":
            import msvcrt

            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            except OSError:
                pass
            return

        import fcntl

        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
