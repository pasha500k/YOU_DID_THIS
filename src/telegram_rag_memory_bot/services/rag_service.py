"""
????: rag_service.py
????????? RAG-?????? ???????: ??????????? ?????????, ?????? ?????
? ??????????, ????????? ?? ? ???? ? ???? ??????????? ????????.
"""

from __future__ import annotations

from collections import OrderedDict
import logging
from pathlib import Path
from typing import Any

from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.schemas import FileAnalysis, IngestedItem, SearchHit
from telegram_rag_memory_bot.services.database import Database
from telegram_rag_memory_bot.services.media_service import MediaService
from telegram_rag_memory_bot.services.openai_service import OpenAIService
from telegram_rag_memory_bot.utils.dates import extract_date_filters
from telegram_rag_memory_bot.utils.text import split_into_chunks, trim_text

LOGGER = logging.getLogger(__name__)


class RagService:
    def __init__(
        self,
        *,
        settings: Settings,
        database: Database,
        media_service: MediaService,
        openai_service: OpenAIService,
    ) -> None:
        self.settings = settings
        self.database = database
        self.media_service = media_service
        self.openai_service = openai_service

    async def ingest_message(
        self,
        *,
        client: object,
        message: object,
        content_date: str,
        content_scope: str = "dated",
        ingested_by_user_id: int,
        local_media_path: Path | None = None,
        caption_override: str | None = None,
        source_text_hint: str = "",
    ) -> IngestedItem:
        work_dir = self.media_service.create_work_dir()
        try:
            use_local_media_metadata = local_media_path is not None and local_media_path.exists()
            if use_local_media_metadata:
                item_type = self.media_service.detect_path_type(local_media_path)
                file_name = self.media_service.get_file_name_from_path(local_media_path)
                mime_type = self.media_service.get_mime_type_from_path(local_media_path)
                file_size = self.media_service.get_file_size_from_path(local_media_path)
            else:
                item_type = self.media_service.detect_message_type(message)
                file_name = self.media_service.get_file_name(message, item_type)
                mime_type = self.media_service.get_mime_type(message)
                file_size = self.media_service.get_file_size(message)
            caption = caption_override if caption_override is not None else self.media_service.get_caption(message)
            LOGGER.info(
                "Индексация: начинаю обработку | тип=%s | файл=%s | дата=%s | scope=%s | размер=%s",
                item_type,
                file_name or '-',
                content_date or 'без даты',
                content_scope,
                self.media_service._format_bytes(file_size or 0) if file_size else '-',
            )

            downloaded_path: Path | None = None
            if local_media_path is not None and local_media_path.exists():
                downloaded_path = local_media_path
                LOGGER.info(
                    "Индексация: использую локальный файл без повторной скачки | %s",
                    downloaded_path.name,
                )
            else:
                downloaded_path = await self.media_service.download_message_media(client, message, work_dir)
                if downloaded_path is not None:
                    LOGGER.info("Индексация: локальная копия готова | %s", downloaded_path.name)

            if downloaded_path is None:
                LOGGER.info("Индексация: локальная копия не требуется, работаю с текстом/метаданными")

            LOGGER.info("Индексация: запускаю анализ контента")
            analysis = await self._analyze_message(
                item_type=item_type,
                message=message,
                downloaded_path=downloaded_path,
                work_dir=work_dir,
                file_name=file_name,
                caption=caption,
                content_date=content_date,
            )

            LOGGER.info("Индексация: формирую knowledge text и чанки")
            knowledge_text = self._build_knowledge_text(
                item_type=item_type,
                file_name=file_name,
                content_date=content_date,
                content_scope=content_scope,
                caption=caption,
                analysis=analysis,
                source_text_hint=source_text_hint,
            )
            chunks = split_into_chunks(
                knowledge_text,
                max_chars=self.settings.max_chunk_chars,
                overlap_chars=self.settings.chunk_overlap_chars,
            ) or [knowledge_text or f"Date: {content_date}"]

            LOGGER.info("Индексация: строю эмбеддинги | чанков=%s", len(chunks))
            embeddings = self.openai_service.embed_texts(chunks)

            metadata = {
                "title": analysis.title,
                "keywords": analysis.keywords,
                "entities": analysis.entities,
                "notes": analysis.notes,
                "language": analysis.language,
                "content_scope": content_scope,
                "manual_source_text": trim_text(source_text_hint, self.settings.max_index_chars),
                "transcript": trim_text(analysis.transcript, self.settings.max_index_chars),
                "ocr_text": trim_text(analysis.ocr_text, self.settings.max_index_chars),
            }
            metadata.update(self._message_metadata(message))
            LOGGER.info("Индексация: сохраняю материал в SQLite")
            item_id = self.database.upsert_item(
                {
                    "content_date": content_date,
                    "content_scope": content_scope,
                    "source_chat_id": int(getattr(message, "chat_id")),
                    "source_message_id": int(getattr(message, "id")),
                    "source_sender_id": getattr(message, "sender_id", None),
                    "ingested_by_user_id": ingested_by_user_id,
                    "telegram_message_date": getattr(message, "date").isoformat() if getattr(message, "date", None) else None,
                    "item_type": item_type,
                    "file_name": file_name,
                    "mime_type": mime_type,
                    "file_size": file_size,
                    "caption": caption,
                    "summary": analysis.summary or analysis.title or "No summary",
                    "knowledge_text": knowledge_text,
                    "metadata": metadata,
                }
            )
            self.database.replace_chunks(item_id, chunks, embeddings)
            LOGGER.info("Индексация: завершена успешно | item_id=%s | файл=%s", item_id, file_name or '-')
            return IngestedItem(
                item_id=item_id,
                item_type=item_type,
                file_name=file_name,
                content_date=content_date,
                summary=analysis.summary or analysis.title or "No summary",
            )
        finally:
            self.media_service.cleanup_work_dir(work_dir)

    def search(self, query: str, limit: int = 5, api_key: str | None = None) -> list[SearchHit]:
        return self.retrieve_relevant_hits(query, api_key=api_key, limit=limit, unique_by_item=True)

    def answer(
        self,
        question: str,
        recent_messages: list[dict[str, str]] | None = None,
        api_key: str | None = None,
        custom_prompt: str | None = None,
    ) -> tuple[str, list[SearchHit]]:
        hits = self.retrieve_relevant_hits(question, recent_messages=recent_messages, api_key=api_key)
        context_hits = hits[: self.settings.max_context_chunks]
        answer = self.answer_from_hits(
            question,
            context_hits,
            recent_messages=recent_messages,
            api_key=api_key,
            custom_prompt=custom_prompt,
        )
        return answer, context_hits

    def answer_from_hits(
        self,
        question: str,
        hits: list[SearchHit],
        recent_messages: list[dict[str, str]] | None = None,
        api_key: str | None = None,
        custom_prompt: str | None = None,
        model: str | None = None,
    ) -> str:
        context_hits = hits[: self.settings.max_context_chunks]
        return self.openai_service.answer_question(
            question,
            context_hits,
            recent_messages=recent_messages,
            api_key=api_key,
            custom_prompt=custom_prompt,
            model=model,
        )

    def retrieve_relevant_hits(
        self,
        query: str,
        recent_messages: list[dict[str, str]] | None = None,
        api_key: str | None = None,
        limit: int | None = None,
        unique_by_item: bool = False,
    ) -> list[SearchHit]:
        hits = self._semantic_hits(query, recent_messages=recent_messages, api_key=api_key)
        if unique_by_item:
            top_by_item: OrderedDict[int, SearchHit] = OrderedDict()
            for hit in hits:
                if hit.item_id not in top_by_item:
                    top_by_item[hit.item_id] = hit
                if limit is not None and len(top_by_item) >= limit:
                    break
            return list(top_by_item.values())
        if limit is None:
            return hits
        return hits[:limit]

    def list_by_date(self, content_date: str) -> list[dict[str, Any]]:
        rows = self.database.list_items_by_date(content_date)
        return [dict(row) for row in rows]

    def list_items_in_date_range(self, date_from: str, date_to: str, limit: int = 80) -> list[dict[str, Any]]:
        return self.database.list_items_in_date_range(date_from, date_to, limit=limit)

    def list_shifts(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.database.list_shifts(limit=limit)

    def create_shift(self, *, name: str, date_from: str, date_to: str) -> int:
        return self.database.create_shift(name=name, date_from=date_from, date_to=date_to)

    def update_shift(self, shift_id: int, *, name: str, date_from: str, date_to: str) -> bool:
        return self.database.update_shift(shift_id, name=name, date_from=date_from, date_to=date_to)

    def delete_shift(self, shift_id: int) -> bool:
        return self.database.delete_shift(shift_id)

    def find_shift_by_query(self, query_text: str) -> dict[str, Any] | None:
        return self.database.find_shift_by_query(query_text)

    def find_shift_for_date(self, content_date: str) -> dict[str, Any] | None:
        return self.database.find_shift_for_date(content_date)

    def get_item(self, item_id: int) -> dict[str, Any] | None:
        row = self.database.get_item(item_id)
        return dict(row) if row else None

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
        return self.database.create_pending_material_upload(
            platform=platform,
            admin_user_id=admin_user_id,
            content_date=content_date,
            content_scope=content_scope,
            description=description,
            source_text=source_text,
            local_file_path=local_file_path,
            original_file_name=original_file_name,
        )

    def list_pending_material_uploads(self, *, status: str = "pending", platform: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.database.list_pending_material_uploads(status=status, platform=platform, limit=limit)

    def consume_pending_material_upload(
        self,
        *,
        claimed_chat_id: int,
        claimed_message_id: int,
        preferred_admin_user_id: int | None = None,
        preferred_platform: str | None = None,
    ) -> dict[str, Any] | None:
        return self.database.consume_pending_material_upload(
            claimed_chat_id=claimed_chat_id,
            claimed_message_id=claimed_message_id,
            preferred_admin_user_id=preferred_admin_user_id,
            preferred_platform=preferred_platform,
        )

    def restore_pending_material_upload(self, pending_id: int) -> bool:
        return self.database.restore_pending_material_upload(pending_id)

    def set_pending_material_upload_item(
        self,
        pending_id: int,
        *,
        item_id: int,
        local_file_path: str | None = None,
    ) -> bool:
        return self.database.set_pending_material_upload_item(
            pending_id,
            item_id=item_id,
            local_file_path=local_file_path,
        )

    def complete_pending_material_upload(self, pending_id: int, *, item_id: int) -> bool:
        return self.database.complete_pending_material_upload(pending_id, item_id=item_id)

    def delete_pending_material_upload(self, pending_id: int) -> bool:
        return self.database.delete_pending_material_upload(pending_id)

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
        return self.database.attach_item_source(
            item_id,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            source_sender_id=source_sender_id,
            telegram_message_date=telegram_message_date,
            metadata=metadata,
        )

    @staticmethod
    def _message_metadata(message: object) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        file_obj = getattr(message, "file", None)
        file_id = getattr(file_obj, "file_id", None) if file_obj else None
        file_name = getattr(file_obj, "name", None) if file_obj else None
        mime_type = getattr(file_obj, "mime_type", None) if file_obj else None
        file_size = getattr(file_obj, "size", None) if file_obj else None
        if not file_id:
            photo = getattr(message, "photo", None)
            if isinstance(photo, list) and photo:
                candidate = photo[-1]
                file_id = getattr(candidate, "file_id", None)
                file_size = file_size or getattr(candidate, "file_size", None)
                file_name = file_name or f"photo_{int(getattr(message, 'id', 0) or 0)}.jpg"
                mime_type = mime_type or "image/jpeg"
        if file_id:
            metadata["telegram_file_id"] = str(file_id)
        if file_name:
            metadata["telegram_file_name"] = str(file_name)
        if mime_type:
            metadata["telegram_mime_type"] = str(mime_type)
        if file_size:
            metadata["telegram_file_size"] = int(file_size)
        return metadata

    def get_item_by_source(self, source_chat_id: int, source_message_id: int) -> dict[str, Any] | None:
        row = self.database.get_item_by_source(source_chat_id, source_message_id)
        return dict(row) if row else None

    def get_user_statistics(self, stats_date: str, limit: int = 300, user_id: int | None = None) -> list[dict[str, Any]]:
        return self.database.get_user_statistics(stats_date, limit=limit, user_id=user_id)

    def list_user_events(self, *, user_id: int | None = None, limit: int = 500) -> list[dict[str, Any]]:
        return self.database.list_user_events(user_id=user_id, limit=limit)

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
        self.database.log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type=event_type,
            event_date=event_date,
            charged=charged,
            username=username,
            first_name=first_name,
            last_name=last_name,
            details=details,
        )

    def get_user_preferences(self, user_id: int) -> dict[str, Any]:
        return self.database.get_user_preferences(user_id)

    def set_user_api_key(self, user_id: int, api_key: str) -> None:
        self.database.set_user_api_key(user_id, api_key)

    def clear_user_api_key(self, user_id: int) -> None:
        self.database.clear_user_api_key(user_id)

    def set_user_api_key_error(self, user_id: int, error_text: str) -> None:
        self.database.set_user_api_key_error(user_id, error_text)

    def set_user_prompt(self, user_id: int, prompt: str) -> None:
        self.database.set_user_prompt(user_id, prompt)

    def clear_user_prompt(self, user_id: int) -> None:
        self.database.clear_user_prompt(user_id)

    def set_user_prompt_profile(self, user_id: int, prompt_profile: str) -> None:
        self.database.set_user_prompt_profile(user_id, prompt_profile)

    def clear_user_prompt_profile(self, user_id: int) -> None:
        self.database.clear_user_prompt_profile(user_id)

    def set_user_department(self, user_id: int, department: str) -> None:
        self.database.set_user_department(user_id, department)

    def has_sent_welcome(self, user_id: int) -> bool:
        return self.database.has_sent_welcome(user_id)

    def mark_welcome_sent(self, user_id: int) -> None:
        self.database.mark_welcome_sent(user_id)

    def has_verified_access_password(self, user_id: int) -> bool:
        return self.database.has_verified_access_password(user_id)

    def mark_access_password_verified(self, user_id: int) -> None:
        self.database.mark_access_password_verified(user_id)

    def consume_daily_department_mode(self, user_id: int, usage_date: str, mode_bucket: str, daily_limit: int = 1) -> tuple[bool, int, int]:
        return self.database.consume_daily_department_mode(user_id, usage_date, mode_bucket, daily_limit=daily_limit)

    def add_mode_credits(self, user_id: int, mode_bucket: str, credits: int) -> None:
        self.database.add_mode_credits(user_id, mode_bucket, credits)

    def get_mode_credits(self, user_id: int, mode_bucket: str) -> int:
        return self.database.get_mode_credits(user_id, mode_bucket)

    def consume_mode_credit(self, user_id: int, mode_bucket: str) -> tuple[bool, int]:
        return self.database.consume_mode_credit(user_id, mode_bucket)

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
        return self.database.create_access_request(
            user_id=user_id,
            platform=platform,
            request_type=request_type,
            request_name=request_name,
            reason=reason,
            mode_bucket=mode_bucket,
        )

    def list_access_requests(self, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        return self.database.list_access_requests(status=status, limit=limit)

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
        return self.database.review_access_request(
            request_id,
            status=status,
            reviewed_by_user_id=reviewed_by_user_id,
            decision_note=decision_note,
            granted_bonus_requests=granted_bonus_requests,
            granted_mode_credits=granted_mode_credits,
        )

    def set_ban(self, user_id: int, *, reason: str, banned_by_user_id: int) -> None:
        self.database.set_ban(user_id, reason=reason, banned_by_user_id=banned_by_user_id)

    def clear_ban(self, user_id: int) -> bool:
        return self.database.clear_ban(user_id)

    def is_user_banned(self, user_id: int) -> tuple[bool, str]:
        return self.database.is_user_banned(user_id)

    def list_banned_users(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.database.list_banned_users(limit=limit)

    def validate_user_api_key(self, api_key: str) -> tuple[bool, str | None]:
        return self.openai_service.validate_user_api_key(api_key)

    def delete_item_by_id(self, item_id: int) -> bool:
        return self.database.delete_item_by_id(item_id)

    def delete_item_by_source(self, source_chat_id: int, source_message_id: int) -> bool:
        return self.database.delete_item_by_source(source_chat_id, source_message_id)

    async def _analyze_message(
        self,
        *,
        item_type: str,
        message: object,
        downloaded_path: Path | None,
        work_dir: Path,
        file_name: str | None,
        caption: str,
        content_date: str,
    ) -> FileAnalysis:
        if item_type == "text":
            LOGGER.info("Анализ: текстовое сообщение, готовлю summary")
            extracted_text = getattr(message, "raw_text", "") or caption
            return self.openai_service.analyze_text_document(
                extracted_text=extracted_text,
                file_name=file_name or "text_message.txt",
                caption=caption,
                content_date=content_date,
            )

        if item_type == "document":
            LOGGER.info("Анализ: извлекаю текст из документа")
            extracted_text = ""
            if downloaded_path:
                try:
                    extracted_text = self.media_service.extract_document_text(downloaded_path)
                except Exception as exc:
                    extracted_text = f"Text extraction error: {exc}"
            if not extracted_text.strip():
                extracted_text = "\n".join(
                    [
                        f"File name: {file_name or 'unknown'}",
                        f"Caption: {caption or '-'}",
                        "The original file was kept in Telegram, but local text extraction did not return useful content.",
                    ]
                )
            return self.openai_service.analyze_text_document(
                extracted_text=extracted_text,
                file_name=file_name,
                caption=caption,
                content_date=content_date,
            )

        if item_type == "image":
            if not downloaded_path:
                raise RuntimeError("Image download failed.")
            LOGGER.info("Анализ: отправляю изображение в vision-модель")
            return self.openai_service.analyze_image(
                image_path=downloaded_path,
                file_name=file_name,
                caption=caption,
                content_date=content_date,
            )

        if item_type == "audio":
            if not downloaded_path:
                raise RuntimeError("Audio download failed.")
            LOGGER.info("Анализ: подготавливаю аудио к транскрипции")
            audio_segments = self.media_service.split_audio_if_needed(downloaded_path, work_dir)
            LOGGER.info("Анализ: запускаю транскрипцию аудио | сегментов=%s", len(audio_segments))
            transcript = self.openai_service.transcribe_audio(audio_segments)
            LOGGER.info("Анализ: транскрипция готова, собираю summary аудио")
            return self.openai_service.analyze_audio(
                transcript=transcript,
                file_name=file_name,
                caption=caption,
                content_date=content_date,
            )

        if item_type == "video":
            if not downloaded_path:
                raise RuntimeError("Video download failed.")
            LOGGER.info("Анализ: извлекаю ключевые кадры из видео")
            frame_dir = work_dir / "frames"
            frame_dir.mkdir(parents=True, exist_ok=True)
            frames = self.media_service.extract_video_keyframes(
                downloaded_path,
                frame_dir,
                self.settings.max_video_frames,
            )
            LOGGER.info("Анализ: ключевые кадры готовы | кадров=%s", len(frames))
            transcript = ""
            LOGGER.info("Анализ: извлекаю аудио из видео")
            audio_path = self.media_service.extract_audio_from_video(downloaded_path, work_dir)
            if audio_path is not None:
                audio_segments = self.media_service.split_audio_if_needed(audio_path, work_dir)
                LOGGER.info("Анализ: запускаю транскрипцию видео | сегментов=%s", len(audio_segments))
                transcript = self.openai_service.transcribe_audio(audio_segments)
            else:
                LOGGER.info("Анализ: аудио-дорожка не найдена, продолжаю по кадрам")
            LOGGER.info("Анализ: отправляю видео в vision-модель")
            return self.openai_service.analyze_video(
                frame_paths=frames,
                transcript=transcript,
                file_name=file_name,
                caption=caption,
                content_date=content_date,
            )

        LOGGER.info("Анализ: неизвестный тип, сохраняю только базовые метаданные")
        fallback_text = "\n".join(
            [
                f"File name: {file_name or 'unknown'}",
                f"Caption: {caption or '-'}",
                "Unsupported message type. Only basic metadata was available.",
            ]
        )
        return self.openai_service.analyze_text_document(
            extracted_text=fallback_text,
            file_name=file_name,
            caption=caption,
            content_date=content_date,
        )

    def _semantic_hits(
        self,
        query: str,
        recent_messages: list[dict[str, str]] | None = None,
        api_key: str | None = None,
    ) -> list[SearchHit]:
        recent_context = []
        for message in recent_messages or []:
            role = message.get("role") or "user"
            content = (message.get("content") or "").strip()
            if content:
                recent_context.append(f"{role}: {content}")

        retrieval_query = query
        if recent_context:
            retrieval_query = "\n".join(
                [
                    f"current_question: {query}",
                    "recent_messages:",
                    *recent_context,
                ]
            )

        date_from, date_to = extract_date_filters(retrieval_query)
        if not date_from and not date_to:
            shift_row = self.database.find_shift_by_query(retrieval_query)
            if shift_row is not None:
                date_from = str(shift_row.get("date_from") or "") or None
                date_to = str(shift_row.get("date_to") or "") or None
        embedding = self.openai_service.embed_texts([retrieval_query], api_key=api_key)[0]
        hits = self.database.semantic_search(
            embedding,
            limit=self.settings.query_top_k,
            date_from=date_from,
            date_to=date_to,
        )
        for hit in hits:
            self._attach_shift_metadata(hit)
        return hits

    def _attach_shift_metadata(self, hit: SearchHit) -> None:
        if getattr(hit, "content_scope", "dated") == "timeless":
            return
        content_date = str(hit.content_date or "").strip()
        if not content_date:
            return
        shift_row = self.database.find_shift_for_date(content_date)
        if shift_row is None:
            return
        hit.metadata["shift_id"] = int(shift_row.get("id") or 0)
        hit.metadata["shift_name"] = str(shift_row.get("name") or "").strip()
        hit.metadata["shift_date_from"] = str(shift_row.get("date_from") or "").strip()
        hit.metadata["shift_date_to"] = str(shift_row.get("date_to") or "").strip()

    def _build_knowledge_text(
        self,
        *,
        item_type: str,
        file_name: str | None,
        content_date: str,
        content_scope: str,
        caption: str,
        analysis: FileAnalysis,
        source_text_hint: str = "",
    ) -> str:
        date_text = content_date or "without fixed date"
        parts = [
            f"Date: {date_text}",
            f"Content scope: {content_scope}",
            f"Type: {item_type}",
            f"File: {file_name or '-'}",
            f"Title: {analysis.title or '-'}",
            f"Summary: {analysis.summary or '-'}",
        ]
        if caption:
            parts.append(f"Caption: {caption}")
        if analysis.keywords:
            parts.append("Keywords: " + ", ".join(analysis.keywords))
        if analysis.entities:
            parts.append("Entities: " + ", ".join(analysis.entities))
        if source_text_hint:
            parts.append("Manual notes:\n" + trim_text(source_text_hint, self.settings.max_index_chars))
        if analysis.knowledge_text:
            parts.append("Knowledge:\n" + trim_text(analysis.knowledge_text, self.settings.max_index_chars))
        if analysis.transcript:
            parts.append("Transcript:\n" + trim_text(analysis.transcript, self.settings.max_index_chars))
        if analysis.ocr_text:
            parts.append("OCR:\n" + trim_text(analysis.ocr_text, self.settings.max_index_chars))
        if analysis.notes:
            parts.append("Notes: " + analysis.notes)
        return "\n\n".join(part for part in parts if part.strip())




