"""
????: openai_service.py
??????????? ?????? ? OpenAI API: ??????????? ????? ? ?????,
?????? ??????????, ???????? ?? ??????? ? ????????? API-?????.
"""

from __future__ import annotations

import base64
import json
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

from openai import OpenAI

from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.schemas import FileAnalysis, SearchHit
from telegram_rag_memory_bot.utils.dates import format_display_date
from telegram_rag_memory_bot.utils.text import trim_text

LOGGER = logging.getLogger(__name__)


class OpenAIService:
    ANALYSIS_SCHEMA = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "title": {"type": "string"},
            "summary": {"type": "string"},
            "knowledge_text": {"type": "string"},
            "keywords": {
                "type": "array",
                "items": {"type": "string"},
            },
            "entities": {
                "type": "array",
                "items": {"type": "string"},
            },
            "transcript": {"type": "string"},
            "ocr_text": {"type": "string"},
            "notes": {"type": "string"},
            "language": {"type": "string"},
        },
        "required": [
            "title",
            "summary",
            "knowledge_text",
            "keywords",
            "entities",
            "transcript",
            "ocr_text",
            "notes",
            "language",
        ],
    }

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._default_client = OpenAI(api_key=settings.openai_api_key)
        self._client_cache: dict[str, OpenAI] = {}

    def embed_texts(self, texts: list[str], api_key: str | None = None) -> list[list[float]]:
        client = self._get_client(api_key)
        response = client.embeddings.create(
            model=self.settings.embedding_model,
            input=texts,
        )
        return [item.embedding for item in response.data]

    def validate_user_api_key(self, api_key: str) -> tuple[bool, str | None]:
        try:
            client = self._get_client(api_key)
            client.embeddings.create(
                model=self.settings.embedding_model,
                input=["ping"],
            )
            client.responses.create(
                model=self.settings.answer_model,
                input="ping",
                max_output_tokens=16,
            )
        except Exception as exc:
            return False, str(exc)
        return True, None

    def transcribe_audio(self, audio_paths: list[Path]) -> str:
        transcript_parts: list[str] = []
        for index, audio_path in enumerate(audio_paths, start=1):
            with audio_path.open("rb") as audio_file:
                response = self._default_client.audio.transcriptions.create(
                    model=self.settings.transcription_model,
                    file=audio_file,
                )
            segment_text = self._normalize_letovo_text(getattr(response, "text", None) or str(response))
            transcript_parts.append(f"Сегмент {index}:\n{segment_text.strip()}")
        return "\n\n".join(part for part in transcript_parts if part.strip())

    def analyze_text_document(
        self,
        *,
        extracted_text: str,
        file_name: str | None,
        caption: str,
        content_date: str,
    ) -> FileAnalysis:
        excerpt = trim_text(extracted_text, self.settings.max_index_chars)
        prompt = (
            "Ты анализируешь файлы для RAG-памяти Telegram-бота. "
            "Используй только переданный контент. "
            "Сформируй краткое, но насыщенное фактами описание для будущего семантического поиска. "
            "Назначенная дата материала считается основной."
        )
        content = [
            self._text_block(
                "\n".join(
                    [
                        f"Назначенная дата: {self._date_prompt_text(content_date)}",
                        f"Имя файла: {file_name or 'unknown'}",
                        f"Подпись: {caption or '-'}",
                        "Задача: кратко перескажи документ, извлеки ключевые факты, сущности, важные числа и детали для поиска.",
                        "Текст документа:",
                        excerpt or "Извлеченный текст отсутствует.",
                    ]
                )
            )
        ]
        return self._structured_analysis(model=self.settings.analysis_model, system_prompt=prompt, content=content)

    def analyze_image(
        self,
        *,
        image_path: Path,
        file_name: str | None,
        caption: str,
        content_date: str,
    ) -> FileAnalysis:
        prompt = (
            "Ты анализируешь изображения для RAG-памяти Telegram-бота. "
            "Опиши сцену, видимый текст, объекты, людей, действия и факты, полезные для последующего поиска. "
            "Не выдумывай нечитаемый текст. Назначенная дата материала считается основной."
        )
        content = [
            self._text_block(
                "\n".join(
                    [
                        f"Назначенная дата: {self._date_prompt_text(content_date)}",
                        f"Имя файла: {file_name or 'unknown'}",
                        f"Подпись: {caption or '-'}",
                        "Извлекай OCR, когда это возможно, и держи описание строго фактическим.",
                    ]
                )
            ),
            self._image_block(image_path),
        ]
        return self._structured_analysis(model=self.settings.vision_model, system_prompt=prompt, content=content)

    def analyze_audio(
        self,
        *,
        transcript: str,
        file_name: str | None,
        caption: str,
        content_date: str,
    ) -> FileAnalysis:
        prompt = (
            "Ты анализируешь аудиотранскрипции для RAG-памяти Telegram-бота. "
            "Кратко перескажи содержание, сохрани ключевые факты, имена, события и важные детали. "
            "Назначенная дата материала считается основной."
        )
        content = [
            self._text_block(
                "\n".join(
                    [
                        f"Назначенная дата: {self._date_prompt_text(content_date)}",
                        f"Имя файла: {file_name or 'unknown'}",
                        f"Подпись: {caption or '-'}",
                        "Транскрипт:",
                        trim_text(transcript, self.settings.max_index_chars) or "Транскрипт недоступен.",
                    ]
                )
            )
        ]
        result = self._structured_analysis(model=self.settings.analysis_model, system_prompt=prompt, content=content)
        if transcript.strip():
            result.transcript = trim_text(transcript, self.settings.max_index_chars)
        return result

    def analyze_video(
        self,
        *,
        frame_paths: list[Path],
        transcript: str,
        file_name: str | None,
        caption: str,
        content_date: str,
    ) -> FileAnalysis:
        prompt = (
            "Ты анализируешь видео для RAG-памяти Telegram-бота. "
            "Используй и ключевые кадры, и транскрипт. "
            "Кратко перескажи, что происходит, что говорится, какой текст виден, какие есть люди, объекты и важные факты. "
            "Назначенная дата материала считается основной."
        )
        content = [
            self._text_block(
                "\n".join(
                    [
                        f"Назначенная дата: {self._date_prompt_text(content_date)}",
                        f"Имя файла: {file_name or 'unknown'}",
                        f"Подпись: {caption or '-'}",
                        "Транскрипт:",
                        trim_text(transcript, self.settings.max_index_chars) or "Транскрипт недоступен.",
                    ]
                )
            )
        ]
        for frame_path in frame_paths[: self.settings.max_video_frames]:
            content.append(self._image_block(frame_path))

        result = self._structured_analysis(model=self.settings.vision_model, system_prompt=prompt, content=content)
        if transcript.strip():
            result.transcript = trim_text(transcript, self.settings.max_index_chars)
        return result

    def answer_question(
        self,
        question: str,
        hits: list[SearchHit],
        recent_messages: list[dict[str, str]] | None = None,
        api_key: str | None = None,
        custom_prompt: str | None = None,
        model: str | None = None,
    ) -> str:
        context_parts: list[str] = []
        for hit in hits:
            shift_name = str(hit.metadata.get("shift_name") or "").strip()
            context_parts.append(
                "\n".join(
                    [
                        f"Item ID: {hit.item_id}",
                        f"Date: {self._display_hit_date(hit)}",
                        *( [f"Shift: {shift_name}"] if shift_name else [] ),
                        f"Type: {hit.item_type}",
                        f"File: {hit.file_name or '-'}",
                        f"Summary: {hit.summary}",
                        f"Chunk: {hit.chunk_text}",
                    ]
                )
            )

        dialogue_parts: list[str] = []
        for message in recent_messages or []:
            role = "Пользователь" if message.get("role") == "user" else "Ассистент"
            content = (message.get("content") or "").strip()
            if content:
                dialogue_parts.append(f"{role}: {content}")

        memory_context = "\n\n---\n\n".join(context_parts) if context_parts else "В памяти релевантных фрагментов не найдено."
        dialogue_context = "\n".join(dialogue_parts) if dialogue_parts else "Нет предыдущего диалога."
        system_parts = [
            "You answer user questions for a Telegram RAG assistant.",
            "Answer in Russian, briefly and clearly.",
            "Use recent chat history only to resolve references and follow-up questions.",
            "Prioritize the memory context for facts about stored materials, dates, shifts, events, and media contents.",
            "If needed, you may use web search for current information, public facts, or missing context.",
            "Web search is a fallback and helper, not the primary source when memory already contains the needed project facts.",
            "Do not limit yourself only to the memory context.",
            "If memory is partial or empty, still give the best helpful answer using general knowledge, reasoning, and the current dialogue context.",
            "If you used internet sources, clearly separate them from memory-backed facts and do not invent URLs or citations.",
            "If a part of the answer is not directly confirmed by memory, briefly mark it as an inference, assumption, or general explanation.",
            "If memory and general knowledge conflict, prioritize memory for project-specific facts and explicitly mention the mismatch.",
            "Mention exact dates in DD-MM-YYYY format whenever relevant.",
            "If the context refers to different shifts, explicitly distinguish facts by shift name.",
            "If memory is insufficient, say that honestly, but do not refuse the answer for that reason alone.",
            "End with a short sources line in the format: Источники: #12 (01-07-2025), #15 (03-07-2025).",
            "If there are no memory hits, write: Источники: нет.",
        ]
        if custom_prompt:
            system_parts.append("Additional user prompt:")
            system_parts.append(custom_prompt.strip())

        client = self._get_client(api_key)
        request_kwargs: dict[str, object] = {
            "model": model or self.settings.answer_model,
            "input": [
                {
                    "role": "system",
                    "content": [
                        self._text_block("\n".join(system_parts))
                    ],
                },
                {
                    "role": "user",
                    "content": [
                        self._text_block(
                            "\n\n".join(
                                [
                                    f"Текущий вопрос: {question}",
                                    "Последние сообщения диалога:",
                                    dialogue_context,
                                    "Контекст из памяти:",
                                    memory_context,
                                ]
                            )
                        )
                    ],
                },
            ],
            "max_output_tokens": 800,
        }
        use_web_search = self.settings.openai_web_search_enabled
        if use_web_search:
            request_kwargs["include"] = ["web_search_call.action.sources"]
            request_kwargs["tools"] = [self._build_web_search_tool()]
            request_kwargs["max_tool_calls"] = self.settings.openai_web_search_max_tool_calls

        response = self._create_answer_response(
            client=client,
            request_kwargs=request_kwargs,
            use_web_search=use_web_search,
        )
        answer_text = response.output_text.strip()
        if use_web_search:
            answer_text = self._append_web_sources(answer_text, self._extract_web_source_urls(response))
        return answer_text

    def _structured_analysis(self, *, model: str, system_prompt: str, content: list[dict[str, str]]) -> FileAnalysis:
        response = self._default_client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": [
                        self._text_block(
                            "\n\n".join(
                                [
                                    system_prompt,
                                    (
                                        "Верни только корректный JSON по схеме. "
                                        "Все описательные поля должны быть только на русском языке: "
                                        "title, summary, knowledge_text, keywords, entities, notes и language. "
                                        "Поле language заполняй значением 'ru'. "
                                        "Поле transcript сохраняй как текст речи без перевода, если он есть. "
                                        "Поле ocr_text сохраняй как исходный видимый текст без перевода, если он есть."
                                    ),
                                ]
                            )
                        )
                    ],
                },
                {"role": "user", "content": content},
            ],
            text={
                "format": {
                    "type": "json_schema",
                    "name": "file_analysis",
                    "schema": self.ANALYSIS_SCHEMA,
                    "strict": True,
                }
            },
            max_output_tokens=2000,
        )
        analysis = FileAnalysis.model_validate(json.loads(response.output_text))
        return self._normalize_analysis(analysis)

    @staticmethod
    def _text_block(text: str) -> dict[str, str]:
        return {"type": "input_text", "text": text}

    def _image_block(self, image_path: Path) -> dict[str, str]:
        mime_type = "image/jpeg"
        encoded = base64.b64encode(image_path.read_bytes()).decode("ascii")
        return {"type": "input_image", "image_url": f"data:{mime_type};base64,{encoded}"}

    @staticmethod
    def _date_prompt_text(content_date: str | None) -> str:
        return format_display_date(content_date) or "без фиксированной даты"

    @staticmethod
    def _display_hit_date(hit: SearchHit) -> str:
        if getattr(hit, "content_scope", "dated") == "timeless":
            return "без даты"
        return format_display_date(hit.content_date) or "без даты"

    def _normalize_analysis(self, analysis: FileAnalysis) -> FileAnalysis:
        analysis.title = self._normalize_letovo_text(analysis.title)
        analysis.summary = self._normalize_letovo_text(analysis.summary)
        analysis.knowledge_text = self._normalize_letovo_text(analysis.knowledge_text)
        analysis.transcript = self._normalize_letovo_text(analysis.transcript)
        analysis.ocr_text = self._normalize_letovo_text(analysis.ocr_text)
        analysis.notes = self._normalize_letovo_text(analysis.notes)
        analysis.keywords = [self._normalize_letovo_text(keyword) for keyword in analysis.keywords]
        analysis.entities = [self._normalize_letovo_text(entity) for entity in analysis.entities]
        return analysis

    @staticmethod
    def _normalize_letovo_text(text: str | None) -> str:
        if not text:
            return ""
        replacements = (
            (r"\bLETOVA\b", "LETOVO"),
            (r"\bLetova\b", "Letovo"),
            (r"\bletova\b", "letovo"),
            (r"\bЛЕТОВА\b", "ЛЕТОВО"),
            (r"\bЛетова\b", "Летово"),
            (r"\bлетова\b", "летово"),
        )
        normalized = text
        for pattern, replacement in replacements:
            normalized = re.sub(pattern, replacement, normalized)
        return normalized

    def _build_web_search_tool(self) -> dict[str, object]:
        return {
            "type": "web_search",
            "search_context_size": self.settings.openai_web_search_context_size,
        }

    def _create_answer_response(
        self,
        *,
        client: OpenAI,
        request_kwargs: dict[str, object],
        use_web_search: bool,
    ):
        try:
            return client.responses.create(**request_kwargs)
        except Exception as exc:
            if not use_web_search:
                raise
            LOGGER.warning(
                "Web search answer fallback triggered; retrying without internet tool: %s",
                exc,
            )
            fallback_kwargs = dict(request_kwargs)
            fallback_kwargs.pop("include", None)
            fallback_kwargs.pop("tools", None)
            fallback_kwargs.pop("max_tool_calls", None)
            return client.responses.create(**fallback_kwargs)

    @staticmethod
    def _extract_web_source_urls(response: object) -> list[str]:
        urls: list[str] = []
        for item in getattr(response, "output", []) or []:
            if getattr(item, "type", "") != "web_search_call":
                continue
            action = getattr(item, "action", None)
            action_type = getattr(action, "type", "")
            if action_type == "search":
                for source in getattr(action, "sources", []) or []:
                    url = str(getattr(source, "url", "") or "").strip()
                    if url and url not in urls:
                        urls.append(url)
            elif action_type == "open_page":
                url = str(getattr(action, "url", "") or "").strip()
                if url and url not in urls:
                    urls.append(url)
        return urls

    @classmethod
    def _append_web_sources(cls, answer_text: str, urls: list[str]) -> str:
        clean_text = answer_text.strip()
        if not urls or "Интернет:" in clean_text:
            return clean_text
        rendered = ", ".join(cls._format_web_source(url) for url in urls[:3])
        return f"{clean_text}\nИнтернет: {rendered}"

    @staticmethod
    def _format_web_source(url: str) -> str:
        parsed = urlparse(url)
        if not parsed.netloc:
            return url
        return url

    def _get_client(self, api_key: str | None = None) -> OpenAI:
        if not api_key or api_key == self.settings.openai_api_key:
            return self._default_client
        if api_key not in self._client_cache:
            self._client_cache[api_key] = OpenAI(api_key=api_key)
        return self._client_cache[api_key]


