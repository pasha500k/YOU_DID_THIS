"""
????: schemas.py
???????? ?????????????? ????? ? ?????????? ??????, ???????
???????????? ????? ?????????, ???????????? ? ??????.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field


class FileAnalysis(BaseModel):
    title: str = Field(default="")
    summary: str = Field(default="")
    knowledge_text: str = Field(default="")
    keywords: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)
    transcript: str = Field(default="")
    ocr_text: str = Field(default="")
    notes: str = Field(default="")
    language: str = Field(default="ru")


class IngestedItem(BaseModel):
    item_id: int
    item_type: str
    file_name: str | None = None
    content_date: str
    summary: str


@dataclass(slots=True)
class SearchHit:
    item_id: int
    score: float
    content_date: str
    item_type: str
    file_name: str | None
    summary: str
    chunk_text: str
    source_chat_id: int
    source_message_id: int
    metadata: dict[str, Any]
    content_scope: str = "dated"


@dataclass(slots=True)
class PendingDeliveryRequest:
    question: str
    hits: list[SearchHit]
    recent_messages: list[dict[str, str]] = field(default_factory=list)
    api_key: str | None = None
    custom_prompt: str | None = None
    remaining: int = 0
    unlimited_mode: bool = False


@dataclass(slots=True)
class PendingAdminAddRequest:
    stage: str
    content_message: Any | None = None
    content_date: str | None = None
    description: str = ""
"""
Этот файл описывает общие структуры данных
для анализа файлов, поиска по памяти и индексации.
"""
