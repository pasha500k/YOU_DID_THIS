"""
????: domain/models.py
???????? ???????? dataclass-?????? ??????, ????????, ???????
? ????????? ?????????, ???????? ???????????? ???? ??????????.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from telegram_rag_memory_bot.schemas import SearchHit


@dataclass(slots=True)
class SenderProfile:
    username: str | None = None
    first_name: str | None = None
    last_name: str | None = None


@dataclass(slots=True)
class PendingInput:
    action: str
    prompt: str


@dataclass(slots=True)
class DeliveryChoice:
    question: str
    hits: list[SearchHit]
    recent_messages: list[dict[str, str]] = field(default_factory=list)
    api_key: str | None = None
    custom_prompt: str | None = None
    remaining: int = 0
    unlimited_mode: bool = False


@dataclass(slots=True)
class ManagedAnswerOption:
    option_id: int
    trigger_text: str
    match_mode: str
    option_label: str
    response_text: str = ""
    media_path: str | None = None


@dataclass(slots=True)
class ManagedAnswerChoice:
    question: str
    options: list[ManagedAnswerOption]
    remaining: int = 0
    unlimited_mode: bool = False


@dataclass(slots=True)
class ChatSession:
    recent_messages: deque[dict[str, str]]
    pending_input: PendingInput | None = None
    pending_delivery: DeliveryChoice | None = None
    pending_managed_choice: ManagedAnswerChoice | None = None
    state: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class LocalUploadRequest:
    admin_user_id: int
    content_date: str = ""
    description: str = ""
    source_text: str = ""
    local_file_path: Path | None = None
    original_file_name: str | None = None
    content_scope: str = "dated"


@dataclass(slots=True)
class LocalUploadResult:
    item_id: int
    item_type: str
    file_name: str | None
    content_date: str
    summary: str
    storage_chat_id: int
    storage_message_id: int


@dataclass(slots=True)
class PendingMaterialUpload:
    pending_id: int
    platform: str
    admin_user_id: int
    content_date: str = ""
    description: str = ""
    source_text: str = ""
    content_scope: str = "dated"
