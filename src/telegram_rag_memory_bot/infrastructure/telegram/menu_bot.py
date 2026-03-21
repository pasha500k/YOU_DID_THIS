"""
????: infrastructure/telegram/menu_bot.py
????????? ???????? Telegram-??? ?? aiogram: ????????? ?????????,
?????????? reply-??????, ????????? ???????? ? ????????? ? ???????? ? ?????.
"""

from __future__ import annotations

import asyncio
import json
import logging
import mimetypes
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from typing import Any, AsyncGenerator, Coroutine, cast

import aiohttp
import requests
from aiogram import Bot, Dispatcher, Router
from aiogram.client.session.base import BaseSession
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError
from aiogram.methods import TelegramMethod
from aiogram.types import FSInputFile, KeyboardButton, Message, ReplyKeyboardMarkup
from aiogram.types.input_file import InputFile

from telegram_rag_memory_bot.application.assistant_service import AssistantApplicationService
from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.domain.models import ChatSession, DeliveryChoice, ManagedAnswerChoice, ManagedAnswerOption, PendingInput, SenderProfile
from telegram_rag_memory_bot.domain.ports import NotificationGateway, StorageGateway
from telegram_rag_memory_bot.schemas import SearchHit
from telegram_rag_memory_bot.utils.dates import infer_content_date
from telegram_rag_memory_bot.utils.text import split_for_telegram

LOGGER = logging.getLogger(__name__)


class _RequestsTelegramSession(BaseSession):
    """
    This aiogram session uses requests under the hood instead of aiohttp.
    It exists as a compatibility workaround for environments where aiohttp
    cannot complete TLS handshakes with api.telegram.org but requests can.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._session = requests.Session()

    async def close(self) -> None:
        await asyncio.to_thread(self._session.close)

    async def make_request(
        self,
        bot: Bot,
        method: TelegramMethod[Any],
        timeout: int | None = None,
    ) -> Any:
        url = self.api.api_url(token=bot.token, method=method.__api_method__)
        data, files = await self._build_payload(bot, method)
        request_timeout = self.timeout if timeout is None else timeout
        try:
            response = await asyncio.to_thread(
                self._session.post,
                url,
                data=data,
                files=files or None,
                timeout=request_timeout,
            )
        except requests.Timeout as exc:
            raise TelegramNetworkError(method=method, message="Request timeout error") from exc
        except requests.RequestException as exc:
            raise TelegramNetworkError(method=method, message=f"{type(exc).__name__}: {exc}") from exc

        result = self.check_response(
            bot=bot,
            method=method,
            status_code=response.status_code,
            content=response.text,
        )
        return cast(Any, result.result)

    async def stream_content(
        self,
        url: str,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        chunk_size: int = 65536,
        raise_for_status: bool = True,
    ) -> AsyncGenerator[bytes, None]:
        try:
            response = await asyncio.to_thread(
                self._session.get,
                url,
                headers=headers or {},
                timeout=timeout,
            )
            if raise_for_status:
                response.raise_for_status()
            content = response.content
        except requests.Timeout as exc:
            raise RuntimeError("Telegram content download timed out") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"Telegram content download failed: {exc}") from exc

        for index in range(0, len(content), chunk_size):
            yield content[index : index + chunk_size]

    async def _build_payload(self, bot: Bot, method: TelegramMethod[Any]) -> tuple[dict[str, Any], dict[str, tuple[str, bytes]]]:
        data: dict[str, Any] = {}
        files: dict[str, InputFile] = {}
        for key, value in method.model_dump(warnings=False).items():
            prepared = self.prepare_value(value, bot=bot, files=files)
            if not prepared:
                continue
            data[key] = prepared

        request_files: dict[str, tuple[str, bytes]] = {}
        for key, value in files.items():
            request_files[key] = (value.filename or key, await self._read_input_file(value, bot))
        return data, request_files

    @staticmethod
    async def _read_input_file(file: InputFile, bot: Bot) -> bytes:
        chunks: list[bytes] = []
        async for chunk in file.read(bot):
            chunks.append(chunk)
        return b"".join(chunks)


class Button:
    """
    This shim preserves the old Button.text(...) call sites while the bot now
    uses aiogram reply keyboards under the hood.
    """

    @staticmethod
    def text(label: str, resize: bool = True) -> str:
        return str(label)


class errors:
    """
    This compatibility namespace mirrors the old Telethon-style error name that
    the rest of the project already expects during media forwarding.
    """

    class PeerIdInvalidError(RuntimeError):
        pass


class events:
    """
    This compatibility namespace exposes a Telethon-like Event type so the
    shared handler signatures can stay small and familiar.
    """

    class NewMessage:
        Event = object


class _TelegramMessageAdapter(SimpleNamespace):
    """
    This lightweight message adapter gives aiogram messages a Telethon-like
    shape that existing services already know how to consume.
    """


class _AiogramTelegramEvent:
    """
    This wrapper exposes just enough of the old event interface for the shared
    bot handlers: chat ids, sender ids, raw text, reply(), and get_sender().
    """

    def __init__(self, *, bot: "TelegramMenuBot", original_message: Message | None, adapted_message: _TelegramMessageAdapter) -> None:
        self._bot = bot
        self._original_message = original_message
        self.message = adapted_message
        self.chat_id = adapted_message.chat_id
        self.sender_id = adapted_message.sender_id
        self.is_private = adapted_message.is_private
        self.raw_text = adapted_message.raw_text

    async def reply(self, text: str, buttons: list[list[Any]] | ReplyKeyboardMarkup | None = None) -> None:
        await self._bot._send_text(int(self.chat_id), text, buttons=buttons, reply_to=self)

    async def get_sender(self) -> object:
        if self._original_message is not None and self._original_message.from_user is not None:
            user = self._original_message.from_user
            return SimpleNamespace(
                id=getattr(user, "id", None),
                username=getattr(user, "username", None),
                first_name=getattr(user, "first_name", None),
                last_name=getattr(user, "last_name", None),
            )
        if self.sender_id is not None:
            return await self._bot.client.get_entity(int(self.sender_id))
        return SimpleNamespace(id=None, username=None, first_name=None, last_name=None)


events.NewMessage.Event = _AiogramTelegramEvent


class _AiogramTelegramClientAdapter:
    """
    This adapter wraps aiogram's Bot client and exposes the small Telegram API
    surface that the rest of the codebase expects for sending, forwarding,
    caching, resolving, and downloading Telegram messages.
    """

    IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".bmp"}
    AUDIO_EXTENSIONS = {".mp3", ".wav", ".ogg", ".oga", ".m4a", ".aac", ".flac", ".opus", ".wma"}
    VIDEO_EXTENSIONS = {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}

    def __init__(self, *, bot: Bot, settings: Settings, app_service: AssistantApplicationService) -> None:
        self.bot = bot
        self.settings = settings
        self.app_service = app_service
        self._message_cache: dict[tuple[int, int], _TelegramMessageAdapter] = {}

    async def start(self, bot_token: str | None = None) -> None:
        return None

    async def get_me(self) -> object:
        return await self.bot.get_me()

    async def get_entity(self, user_id: int) -> object:
        try:
            chat = await self.bot.get_chat(int(user_id))
            return SimpleNamespace(
                id=getattr(chat, "id", user_id),
                username=getattr(chat, "username", None),
                first_name=getattr(chat, "first_name", None),
                last_name=getattr(chat, "last_name", None),
            )
        except Exception:
            LOGGER.debug("Failed to resolve Telegram entity for %s", user_id, exc_info=True)
            return SimpleNamespace(id=user_id, username=None, first_name=None, last_name=None)

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        buttons: list[list[Any]] | ReplyKeyboardMarkup | None = None,
        reply_to: int | None = None,
    ) -> _TelegramMessageAdapter:
        sent = await self.bot.send_message(
            chat_id=int(chat_id),
            text=text,
            reply_markup=self._build_reply_markup(buttons),
        )
        return self._remember_message(sent)

    async def send_file(
        self,
        chat_id: int,
        *,
        file: str | Path,
        reply_to: int | None = None,
        caption: str | None = None,
        allow_video: bool = False,
    ) -> _TelegramMessageAdapter:
        file_path = Path(file)
        input_file = FSInputFile(file_path)
        suffix = file_path.suffix.lower()
        caption_value = caption or None
        if suffix in self.IMAGE_EXTENSIONS:
            sent = await self.bot.send_photo(chat_id=int(chat_id), photo=input_file, caption=caption_value)
        elif suffix in self.AUDIO_EXTENSIONS:
            sent = await self.bot.send_audio(chat_id=int(chat_id), audio=input_file, caption=caption_value)
        elif allow_video and suffix in self.VIDEO_EXTENSIONS:
            sent = await self.bot.send_video(chat_id=int(chat_id), video=input_file, caption=caption_value, supports_streaming=True)
        else:
            sent = await self.bot.send_document(chat_id=int(chat_id), document=input_file, caption=caption_value)
        return self._remember_message(sent)

    async def forward_messages(self, target_chat_id: int, message_ids: int | list[int], *, from_peer: int) -> None:
        ids = [int(message_ids)] if isinstance(message_ids, int) else [int(message_id) for message_id in message_ids]
        for message_id in ids:
            try:
                await self.bot.forward_message(
                    chat_id=int(target_chat_id),
                    from_chat_id=int(from_peer),
                    message_id=message_id,
                )
            except TelegramBadRequest as exc:
                raise errors.PeerIdInvalidError(str(exc)) from exc

    async def get_messages(self, chat_id: int, ids: int) -> _TelegramMessageAdapter | None:
        key = (int(chat_id), int(ids))
        cached = self._message_cache.get(key)
        if cached is not None:
            return cached
        item = self.app_service.rag_service.get_item_by_source(int(chat_id), int(ids))
        if item is None:
            return None
        adapted = self._build_stored_message(item)
        if adapted is not None:
            self._message_cache[key] = adapted
        return adapted

    async def download_media(self, message: object, *, file: str, progress_callback: object | None = None) -> str | None:
        file_id = self._extract_file_id(message)
        if not file_id:
            return None
        telegram_file = await self.bot.get_file(file_id)
        if not telegram_file.file_path:
            return None
        destination = self._resolve_download_destination(Path(file), message)
        destination.parent.mkdir(parents=True, exist_ok=True)
        url = f"https://api.telegram.org/file/bot{self.settings.bot_token}/{telegram_file.file_path}"
        total = int(getattr(getattr(message, "file", None), "size", 0) or 0)
        await asyncio.to_thread(
            self._download_media_sync,
            url,
            destination,
            total,
            progress_callback,
        )
        return str(destination)

    @staticmethod
    def _download_media_sync(
        url: str,
        destination: Path,
        total: int,
        progress_callback: object | None,
    ) -> None:
        current = 0
        with requests.get(url, stream=True, timeout=(60, 7200)) as response:
            response.raise_for_status()
            content_length = int(response.headers.get("Content-Length") or 0)
            total_bytes = content_length or total
            with destination.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=256 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    current += len(chunk)
                    if callable(progress_callback) and total_bytes > 0:
                        progress_callback(current, total_bytes)

    def _remember_message(self, message: Message) -> _TelegramMessageAdapter:
        adapted = self._adapt_message(message)
        self._message_cache[(adapted.chat_id, adapted.id)] = adapted
        if len(self._message_cache) > 4096:
            oldest_key = next(iter(self._message_cache))
            self._message_cache.pop(oldest_key, None)
        return adapted

    def _adapt_message(self, message: Message) -> _TelegramMessageAdapter:
        chat_id = int(message.chat.id)
        sender_id = int(message.from_user.id) if message.from_user is not None else None
        raw_text = (message.text or message.caption or "").strip()
        file_stub, media, photo, video, audio, voice, document = self._extract_media_fields(message)
        return _TelegramMessageAdapter(
            id=int(message.message_id),
            chat_id=chat_id,
            sender_id=sender_id,
            date=message.date,
            raw_text=raw_text,
            message=raw_text,
            media=media,
            photo=photo,
            video=video,
            audio=audio,
            voice=voice,
            document=document,
            file=file_stub,
            is_private=str(message.chat.type) == "private",
        )

    def _build_stored_message(self, item: dict[str, Any]) -> _TelegramMessageAdapter | None:
        metadata = self._parse_metadata(item.get("metadata_json"))
        file_id = str(metadata.get("telegram_file_id") or "").strip()
        item_type = str(item.get("item_type") or "")
        file_name = str(item.get("file_name") or "").strip() or None
        mime_type = str(item.get("mime_type") or "").strip() or None
        file_size = int(item.get("file_size") or 0) or None

        file_stub = None
        media = None
        photo = None
        video = None
        audio = None
        voice = None
        document = None
        if file_id:
            file_stub = SimpleNamespace(file_id=file_id, name=file_name, mime_type=mime_type, size=file_size, ext=Path(file_name or "").suffix or None)
            media = True
            if item_type == "image":
                photo = [SimpleNamespace(file_id=file_id, file_size=file_size)]
            elif item_type == "video":
                video = SimpleNamespace(file_id=file_id, file_name=file_name, mime_type=mime_type, file_size=file_size)
            elif item_type == "audio":
                audio = SimpleNamespace(file_id=file_id, file_name=file_name, mime_type=mime_type, file_size=file_size)
            elif item_type == "document":
                document = SimpleNamespace(file_id=file_id, file_name=file_name, mime_type=mime_type, file_size=file_size)

        return _TelegramMessageAdapter(
            id=int(item.get("source_message_id") or 0),
            chat_id=int(item.get("source_chat_id") or 0),
            sender_id=int(item.get("source_sender_id") or 0) or None,
            date=item.get("telegram_message_date"),
            raw_text=str(item.get("caption") or ""),
            message=str(item.get("caption") or ""),
            media=media,
            photo=photo,
            video=video,
            audio=audio,
            voice=voice,
            document=document,
            file=file_stub,
            is_private=False,
        )

    @staticmethod
    def _parse_metadata(raw_metadata: Any) -> dict[str, Any]:
        if isinstance(raw_metadata, dict):
            return dict(raw_metadata)
        try:
            return json.loads(str(raw_metadata or "{}"))
        except Exception:
            return {}

    def _extract_media_fields(
        self,
        message: Message,
    ) -> tuple[SimpleNamespace | None, object | None, object | None, object | None, object | None, object | None, object | None]:
        if message.photo:
            photo = list(message.photo)
            largest = photo[-1]
            file_stub = SimpleNamespace(file_id=largest.file_id, name=f"photo_{message.message_id}.jpg", mime_type="image/jpeg", size=largest.file_size, ext=".jpg")
            return file_stub, photo, photo, None, None, None, None
        if message.video:
            video = message.video
            file_name = video.file_name or f"video_{message.message_id}.mp4"
            file_stub = SimpleNamespace(file_id=video.file_id, name=file_name, mime_type=video.mime_type, size=video.file_size, ext=Path(file_name).suffix or ".mp4")
            return file_stub, video, None, video, None, None, None
        if message.audio:
            audio = message.audio
            file_name = audio.file_name or f"audio_{message.message_id}.mp3"
            file_stub = SimpleNamespace(file_id=audio.file_id, name=file_name, mime_type=audio.mime_type, size=audio.file_size, ext=Path(file_name).suffix or ".mp3")
            return file_stub, audio, None, None, audio, None, None
        if message.voice:
            voice = message.voice
            file_stub = SimpleNamespace(file_id=voice.file_id, name=f"voice_{message.message_id}.ogg", mime_type=voice.mime_type or "audio/ogg", size=voice.file_size, ext=".ogg")
            return file_stub, voice, None, None, None, voice, None
        if message.document:
            document = message.document
            file_name = document.file_name or f"document_{message.message_id}"
            guessed_ext = Path(file_name).suffix or mimetypes.guess_extension(document.mime_type or "") or ""
            file_stub = SimpleNamespace(file_id=document.file_id, name=file_name, mime_type=document.mime_type, size=document.file_size, ext=guessed_ext or None)
            return file_stub, document, None, None, None, None, document
        return None, None, None, None, None, None, None

    @staticmethod
    def _build_reply_markup(buttons: list[list[Any]] | ReplyKeyboardMarkup | None) -> ReplyKeyboardMarkup | None:
        if buttons is None:
            return None
        if isinstance(buttons, ReplyKeyboardMarkup):
            return buttons
        keyboard_rows: list[list[KeyboardButton]] = []
        for row in buttons:
            button_row: list[KeyboardButton] = []
            for button in row:
                label = str(button).strip()
                if label:
                    button_row.append(KeyboardButton(text=label))
            if button_row:
                keyboard_rows.append(button_row)
        if not keyboard_rows:
            return None
        return ReplyKeyboardMarkup(keyboard=keyboard_rows, resize_keyboard=True)

    @staticmethod
    def _extract_file_id(message: object) -> str | None:
        file_obj = getattr(message, "file", None)
        file_id = getattr(file_obj, "file_id", None) if file_obj else None
        if file_id:
            return str(file_id)
        photo = getattr(message, "photo", None)
        if isinstance(photo, list) and photo:
            candidate = photo[-1]
            return str(getattr(candidate, "file_id", "") or "") or None
        return None

    @staticmethod
    def _resolve_download_destination(base_path: Path, message: object) -> Path:
        if base_path.exists() and base_path.is_dir():
            file_name = str(getattr(getattr(message, "file", None), "name", "") or "").strip()
            if not file_name:
                file_name = f"telegram_{int(getattr(message, 'id', 0) or 0)}{getattr(getattr(message, 'file', None), 'ext', '') or ''}"
            return base_path / Path(file_name).name
        return base_path


class TelegramStorageGateway(StorageGateway):
    """
    This gateway stores original materials in the configured Telegram storage
    chat and returns aiogram-adapted messages for downstream indexing.
    """

    def __init__(self, client: _AiogramTelegramClientAdapter, settings: Settings, app_service: AssistantApplicationService) -> None:
        self.client = client
        self.settings = settings
        self.app_service = app_service

    async def store_file(
        self,
        *,
        local_file_path: Path,
        caption: str,
        original_file_name: str | None = None,
    ) -> object:
        LOGGER.info(
            "Telegram storage upload via aiogram | file=%s | size=%s bytes",
            original_file_name or local_file_path.name,
            local_file_path.stat().st_size if local_file_path.exists() else 0,
        )
        return await self.client.send_file(
            self.settings.storage_chat_id,
            file=local_file_path,
            caption=caption,
            allow_video=False,
        )

    async def store_text(self, *, text: str) -> object:
        return await self.client.send_message(self.settings.storage_chat_id, text)


class TelegramMenuBot(NotificationGateway):
    """
    This is the aiogram Telegram runtime: it handles private dialogs, watches
    the storage group, drives the reply-keyboard UX, and connects the bot to
    the shared RAG/application services.
    """

    VIDEO_SUFFIXES = {".mp4", ".mov", ".m4v", ".webm", ".avi", ".mkv"}
    BUTTON_ASK = "Спросить"
    BUTTON_SEARCH = "Поиск"
    BUTTON_LIST = "По дате"
    BUTTON_FILE = "Файл по ID"
    BUTTON_SETTINGS = "Настройки"
    BUTTON_HELP = "Помощь"
    BUTTON_BACK = "Назад"
    BUTTON_CANCEL = "Отмена"
    BUTTON_MY_SETTINGS = "Мои настройки"
    BUTTON_SET_API = "Установить API token"
    BUTTON_DELETE_API = "Удалить API token"
    BUTTON_SET_PROMPT = "Установить prompt"
    BUTTON_DELETE_PROMPT = "Удалить prompt"
    BUTTON_PROMT = "Выбрать prompt"
    BUTTON_STATS = "Статистика"
    BUTTON_LOCAL_UPLOAD = "Загрузка через localhost"
    BUTTON_REQUEST_ACCESS = "Заявка админу"

    def __init__(self, settings: Settings, app_service: AssistantApplicationService) -> None:
        self.settings = settings
        self.app_service = app_service
        self.bot = self._create_bot()
        self.dispatcher = Dispatcher()
        self.router = Router()
        self.dispatcher.include_router(self.router)
        self.router.message.register(self._on_message)
        self.client = _AiogramTelegramClientAdapter(bot=self.bot, settings=settings, app_service=app_service)
        self.storage_gateway = TelegramStorageGateway(self.client, settings, app_service)
        self.sessions: dict[int, ChatSession] = {}
        self._chat_locks: dict[int, asyncio.Lock] = {}
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._watched_chat_ids = self.settings.video_download_chat_ids | self.settings.auto_ingest_chat_ids

    def _create_bot(self) -> Bot:
        return Bot(
            token=self.settings.bot_token,
            session=_RequestsTelegramSession(timeout=float(self.settings.telegram_bot_api_read_timeout_seconds)),
        )

    async def run(self) -> None:
        while True:
            try:
                me = await self.bot.get_me(request_timeout=30)
                LOGGER.info("Telegram bot connected as @%s id=%s", getattr(me, "username", None) or "unknown", getattr(me, "id", None))
                try:
                    await self.bot.delete_webhook(drop_pending_updates=False, request_timeout=30)
                except Exception:
                    LOGGER.exception("Failed to switch Telegram bot to polling mode")
                try:
                    await self.dispatcher.start_polling(self.bot, allowed_updates=["message"])
                    return
                finally:
                    await self.bot.session.close()
            except asyncio.CancelledError:
                try:
                    await self.bot.session.close()
                finally:
                    raise
            except TelegramBadRequest as exc:
                LOGGER.error(
                    "Telegram bot startup failed because Bot API returned a bad request: %s. Retrying in 15 seconds.",
                    exc,
                )
                try:
                    await self.bot.session.close()
                except Exception:
                    LOGGER.debug("Failed to close aiogram session after Telegram bad request", exc_info=True)
                self.bot = self._create_bot()
                self.client.bot = self.bot
                await asyncio.sleep(15)
            except Exception as exc:
                if exc.__class__.__name__ == "TelegramConflictError":
                    LOGGER.error(
                        "Telegram bot cannot start polling because another copy of this bot is already running. "
                        "Stop the duplicate python.exe process and restart only one instance. Retrying in 15 seconds."
                    )
                else:
                    LOGGER.error(
                        "Telegram bot is temporarily unavailable: %s. "
                        "Most likely this machine cannot reach https://api.telegram.org right now. Retrying in 15 seconds.",
                        exc,
                    )
                try:
                    await self.bot.session.close()
                except Exception:
                    LOGGER.debug("Failed to close aiogram session after Telegram startup error", exc_info=True)
                self.bot = self._create_bot()
                self.client.bot = self.bot
                await asyncio.sleep(15)

    async def notify_user(self, *, user_id: int, text: str) -> None:
        await self._send_text(int(user_id), text, buttons=self._main_keyboard(is_admin=self.app_service.is_admin(user_id), user_id=user_id))

    def _chat_lock(self, chat_id: int) -> asyncio.Lock:
        if chat_id not in self._chat_locks:
            self._chat_locks[chat_id] = asyncio.Lock()
        return self._chat_locks[chat_id]

    def _schedule_background_task(self, coroutine: Coroutine[Any, Any, None], *, task_name: str) -> None:
        task = asyncio.create_task(coroutine, name=task_name)
        self._background_tasks.add(task)

        def _on_done(done_task: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(done_task)
            try:
                exc = done_task.exception()
            except asyncio.CancelledError:
                return
            if exc is not None:
                LOGGER.exception("Telegram background task failed: %s", task_name, exc_info=exc)

        task.add_done_callback(_on_done)

    async def _run_in_chat_scope(self, chat_id: int, coroutine: Coroutine[Any, Any, None]) -> None:
        async with self._chat_lock(chat_id):
            await coroutine

    def _session(self, chat_id: int) -> ChatSession:
        if chat_id not in self.sessions:
            self.sessions[chat_id] = ChatSession(recent_messages=deque(maxlen=self.settings.conversation_context_messages))
        return self.sessions[chat_id]

    async def _on_message(self, message: Message) -> None:
        if message.from_user is not None and message.from_user.is_bot:
            return
        adapted = self.client._remember_message(message)
        event = _AiogramTelegramEvent(bot=self, original_message=message, adapted_message=adapted)
        LOGGER.info(
            "Telegram incoming message | chat=%s | sender=%s | private=%s | text=%s",
            event.chat_id,
            event.sender_id,
            event.is_private,
            (event.raw_text or "<empty>")[:120],
        )
        if event.is_private:
            self._schedule_background_task(
                self._run_in_chat_scope(event.chat_id, self._handle_private_message(event)),
                task_name=f"telegram-private-{event.chat_id}-{event.message.id}",
            )
            return
        if int(event.chat_id) in self._watched_chat_ids:
            self._schedule_background_task(
                self._run_in_chat_scope(event.chat_id, self._handle_auto_group_message(event)),
                task_name=f"telegram-group-{event.chat_id}-{event.message.id}",
            )

    async def _handle_auto_group_message(self, event: events.NewMessage.Event) -> None:
        message = event.message
        if message is None:
            return
        raw_text = (event.raw_text or "").strip()
        if raw_text.startswith("/"):
            return
        chat_id = int(event.chat_id)
        group_sender_id = int(getattr(message, "sender_id", 0) or 0)
        is_video = self.app_service.rag_service.media_service.is_video_message(message)
        has_supported_content = bool(getattr(message, "media", None) or raw_text)
        if not has_supported_content:
            return

        try:
            if chat_id in self.settings.video_download_chat_ids and is_video:
                await self.app_service.rag_service.media_service.download_video_message(self.client, message)

            if chat_id not in self.settings.auto_ingest_chat_ids:
                return

            pending_upload: dict[str, Any] | None = None
            if self.app_service.is_admin(group_sender_id):
                pending_upload = self.app_service.consume_pending_material_upload(
                    claimed_chat_id=chat_id,
                    claimed_message_id=int(getattr(message, "id", 0) or 0),
                    preferred_admin_user_id=group_sender_id or None,
                    preferred_platform=self.app_service.platform_code,
                )
                if pending_upload is not None:
                    LOGGER.info(
                        "Auto ingest: matched pending material | pending_id=%s | sender_id=%s | message_id=%s",
                        pending_upload.get("id"),
                        group_sender_id,
                        getattr(message, "id", None),
                    )

            if pending_upload is not None:
                existing_item_id = int(pending_upload.get("item_id") or 0)
                if existing_item_id > 0:
                    attached = self.app_service.attach_item_source(
                        existing_item_id,
                        source_chat_id=chat_id,
                        source_message_id=int(getattr(message, "id", 0) or 0),
                        source_sender_id=group_sender_id or None,
                        telegram_message_date=str(getattr(message, "date", "") or "") or None,
                        metadata=self._message_metadata_patch(message),
                    )
                    if not attached:
                        raise RuntimeError(f"Failed to attach source message to item {existing_item_id}")
                    pending_id = int(pending_upload.get("id") or 0)
                    self.app_service.complete_pending_material_upload(pending_id, item_id=existing_item_id)
                    LOGGER.info(
                        "Auto ingest: attached group source to existing item | pending_id=%s | item_id=%s | message_id=%s",
                        pending_id,
                        existing_item_id,
                        getattr(message, "id", None),
                    )
                    if str(pending_upload.get("platform") or "") == self.app_service.platform_code:
                        try:
                            await self.notify_user(
                                user_id=int(pending_upload.get("admin_user_id") or 0),
                                text=(
                                    "Оригинал из группы привязан к уже проиндексированному материалу.\n"
                                    f"Заявка: #{pending_id}\n"
                                    f"Индекс: #{existing_item_id}\n"
                                    f"Источник: {chat_id}/{getattr(message, 'id', 0) or 0}"
                                ),
                            )
                        except Exception:
                            LOGGER.exception("Failed to notify admin about attached source for pending material %s", pending_id)
                    return

            original_caption = self.app_service.rag_service.media_service.get_caption(message)
            content_date = str(pending_upload.get("content_date") or "") if pending_upload else infer_content_date(original_caption)
            content_scope = str(pending_upload.get("content_scope") or "dated") if pending_upload else "dated"
            caption_override = self.app_service.merge_pending_upload_caption(pending_upload, original_caption) if pending_upload else None
            source_text_hint = self.app_service.pending_upload_source_text(pending_upload) if pending_upload else ""

            local_media_path = None
            if pending_upload is not None:
                local_file_value = str(pending_upload.get("local_file_path") or "").strip()
                if local_file_value:
                    candidate_path = Path(local_file_value)
                    if candidate_path.exists():
                        local_media_path = candidate_path

            ingested = await self.app_service.rag_service.ingest_message(
                client=self.client,
                message=message,
                content_date=content_date,
                content_scope=content_scope,
                ingested_by_user_id=int((pending_upload or {}).get("admin_user_id") or group_sender_id or 0),
                local_media_path=local_media_path,
                caption_override=caption_override,
                source_text_hint=source_text_hint,
            )

            if pending_upload is not None:
                pending_id = int(pending_upload.get("id") or 0)
                self.app_service.complete_pending_material_upload(pending_id, item_id=ingested.item_id)
                if local_media_path is not None:
                    try:
                        local_media_path.unlink(missing_ok=True)
                    except Exception:
                        LOGGER.exception("Failed to delete matched local upload file %s", local_media_path)
                if str(pending_upload.get("platform") or "") == self.app_service.platform_code:
                    try:
                        await self.notify_user(
                            user_id=int(pending_upload.get("admin_user_id") or 0),
                            text=(
                                "Материал из ожидающей заявки найден в группе и проиндексирован.\n"
                                f"Заявка: #{pending_id}\n"
                                f"Индекс: #{ingested.item_id}\n"
                                f"Дата: {self.app_service.display_content_with_shift(content_date, content_scope)}\n"
                                f"Тип: {ingested.item_type}\n"
                                f"Файл: {ingested.file_name or '-'}"
                            ),
                        )
                    except Exception:
                        LOGGER.exception("Failed to notify admin about matched pending material %s", pending_id)
        except Exception:
            if chat_id in self.settings.auto_ingest_chat_ids:
                try:
                    pending_upload_id = int(((locals().get("pending_upload") or {}).get("id")) or 0)
                    if pending_upload_id > 0:
                        self.app_service.restore_pending_material_upload(pending_upload_id)
                except Exception:
                    LOGGER.exception("Failed to restore pending material after ingest error")
            LOGGER.exception("Failed to process group message chat=%s message=%s", event.chat_id, getattr(message, "id", None))

    async def _handle_private_message(self, event: events.NewMessage.Event) -> None:
        sender_id = getattr(event, "sender_id", None)
        if not self.app_service.is_authorized(sender_id):
            await event.reply("Доступ запрещен.")
            return
        if sender_id is None:
            await event.reply("Не удалось определить пользователя.")
            return

        sender = await event.get_sender()
        sender_profile = self.app_service.profile_from_sender(sender)
        is_admin = self.app_service.is_admin(sender_id)
        is_banned, ban_reason = self.app_service.is_banned(sender_id)
        if is_banned:
            reason_suffix = f"\nПричина: {ban_reason}" if ban_reason else ""
            await event.reply(f"Доступ к боту ограничен администратором.{reason_suffix}")
            return

        chat_id = int(event.chat_id)
        session = self._session(chat_id)
        text = (event.raw_text or "").strip()
        command = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""
        main_buttons = self._main_keyboard(is_admin=is_admin, user_id=sender_id)
        settings_buttons = self._settings_keyboard(is_admin=is_admin, user_id=sender_id)

        first_welcome = await self._maybe_send_welcome(event, sender_id, is_admin)
        if await self._handle_department_survey(event, sender_id, sender_profile, is_admin, text):
            return

        if text in {"/start", "/help", "/menu", self.BUTTON_HELP} or text.startswith("/start ") or text.startswith("/help "):
            if first_welcome and command in {"/start", "/help"}:
                return
            await self._send_text(chat_id, self._help_text(is_admin=is_admin, user_id=sender_id), buttons=main_buttons, reply_to=event)
            return

        if command.startswith("/"):
            custom_command = self.app_service.get_custom_command(command)
            if custom_command is not None:
                await self._handle_custom_command(event, sender_id, sender_profile, is_admin, custom_command)
                return

        if command == "/homosap":
            await self._handle_homosap(event, sender_id, sender_profile, is_admin)
            return

        if text == self.BUTTON_BACK:
            session.pending_input = None
            session.pending_delivery = None
            session.pending_managed_choice = None
            session.state.clear()
            await self._send_text(chat_id, "Главное меню.", buttons=main_buttons, reply_to=event)
            return

        if session.pending_managed_choice is not None and text and not command.startswith("/"):
            if await self._handle_managed_answer_choice(event, session, sender_id, sender_profile, is_admin, text):
                return

        if session.pending_delivery is not None and text and not command.startswith("/"):
            if await self._handle_delivery_choice(event, session, sender_id, sender_profile, is_admin, text):
                return

        if event.message.media and not text:
            if is_admin:
                await self._send_text(chat_id, self._local_upload_text(sender_id), buttons=main_buttons, reply_to=event)
                return
            await self._send_text(
                chat_id,
                "Этот бот ожидает команды и вопросы. Для загрузки материалов обратитесь к администратору.",
                buttons=main_buttons,
                reply_to=event,
            )
            return

        if text == self.BUTTON_SETTINGS:
            await self._send_text(chat_id, "Раздел настроек.", buttons=settings_buttons, reply_to=event)
            return
        if text == self.BUTTON_ASK or text == "/ask":
            session.pending_input = PendingInput("ask", "Напишите вопрос по памяти.")
            session.pending_input = PendingInput("file", "Отправьте ITEM_ID, чтобы получить текстовое описание материала.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_SEARCH or text == "/search":
            session.pending_input = PendingInput("search", "Напишите запрос для поиска по памяти.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_LIST or text == "/list":
            session.pending_input = PendingInput("list", "Отправьте дату в формате DD-MM-YYYY.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_FILE or text == "/file":
            session.pending_input = PendingInput("file", "Отправьте ITEM_ID для пересылки оригинала.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_SET_API or text == "/set_api":
            session.pending_input = PendingInput("set_api", "Отправьте ваш OpenAI API token.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_SET_PROMPT or text == "/set_prompt":
            session.pending_input = PendingInput("set_prompt", "Отправьте ваш пользовательский prompt.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text in {self.BUTTON_PROMT, "/promt"}:
            session.pending_input = PendingInput("prompt_profile", "Выберите профиль prompt кнопкой ниже.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._prompt_profile_keyboard(), reply_to=event)
            return
        if text == "/promo":
            session.pending_input = PendingInput("promo", "Отправьте промокод для активации дополнительных запросов.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_REQUEST_ACCESS or text == "/request_access":
            request_type = session.state.get("request_type", "daily_limit")
            mode_bucket = session.state.get("mode_bucket", "")
            session.pending_input = PendingInput("request_name", "Напишите имя для заявки.")
            session.state = {"request_type": request_type, "mode_bucket": mode_bucket}
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text == self.BUTTON_CANCEL:
            session.pending_input = None
            session.pending_delivery = None
            session.pending_managed_choice = None
            session.state.clear()
            await self._send_text(chat_id, "Действие отменено.", buttons=main_buttons, reply_to=event)
            return
        if text in {self.BUTTON_MY_SETTINGS, "/my_settings"}:
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/my_settings"})
            await self._send_text(chat_id, self.app_service.build_user_settings_text(sender_id), buttons=settings_buttons, reply_to=event)
            return
        if text in {self.BUTTON_DELETE_API, "/delete_api"}:
            await self._handle_delete_api(event, sender_id, sender_profile, is_admin)
            return
        if text in {self.BUTTON_DELETE_PROMPT, "/delete_prompt"}:
            await self._handle_delete_prompt(event, sender_id, sender_profile, is_admin)
            return
        if text in {self.BUTTON_STATS, "/stats"}:
            if not is_admin:
                self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="unknown_command", sender_profile=sender_profile, details={"command": "/stats"})
                await self._send_text(chat_id, "Команда /stats доступна только администратору.", buttons=main_buttons, reply_to=event)
                return
            await self._handle_stats(event, sender_id, sender_profile, is_admin, raw_arg="")
            return
        if text in {self.BUTTON_LOCAL_UPLOAD, "/upload_local"}:
            if not is_admin:
                await self._send_text(chat_id, "Эта функция доступна только администратору.", buttons=main_buttons, reply_to=event)
                return
            await self._send_text(chat_id, self._local_upload_text(sender_id), buttons=main_buttons, reply_to=event)
            return

        department_action = self.app_service.resolve_department_action_by_label(text, self.app_service.get_user_department(sender_id))
        if department_action is not None:
            if self.app_service.get_user_department(sender_id) == "проект 11" and text == self.app_service.department_button_label(sender_id):
                session.pending_input = PendingInput("department_pick", self.app_service.department_action_picker_prompt())
                await self._send_text(chat_id, session.pending_input.prompt, buttons=self._department_action_keyboard(include_all=True), reply_to=event)
                return
            await self._handle_department_action(event, session, sender_id, sender_profile, is_admin, text, department_action["department"])
            return

        if session.pending_input is not None and not command.startswith("/"):
            await self._handle_pending_input(event, session, sender_id, sender_profile, is_admin, text)
            return

        if command == "/stats":
            if not is_admin:
                self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="unknown_command", sender_profile=sender_profile, details={"command": command})
                await self._send_text(chat_id, "Команда /stats доступна только администратору.", buttons=main_buttons, reply_to=event)
                return
            await self._handle_stats(event, sender_id, sender_profile, is_admin, raw_arg=text[len("/stats") :].strip())
            return
        if command == "/delete_api":
            await self._handle_delete_api(event, sender_id, sender_profile, is_admin)
            return
        if command == "/delete_prompt":
            await self._handle_delete_prompt(event, sender_id, sender_profile, is_admin)
            return
        if command == "/my_settings":
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/my_settings"})
            await self._send_text(chat_id, self.app_service.build_user_settings_text(sender_id), buttons=settings_buttons, reply_to=event)
            return
        if text.startswith("/set_api "):
            await self._handle_set_api(event, sender_id, sender_profile, is_admin, text[len("/set_api ") :].strip())
            return
        if text.startswith("/set_prompt "):
            await self._handle_set_prompt(event, sender_id, sender_profile, is_admin, text[len("/set_prompt ") :].strip())
            return
        if text.startswith("/promo "):
            await self._handle_promo(event, sender_id, sender_profile, is_admin, text[len("/promo ") :].strip())
            return
        if text.startswith("/promt "):
            await self._handle_prompt_profile(event, sender_id, sender_profile, is_admin, text[len("/promt ") :].strip())
            return
        if text.startswith("/request_access "):
            request_type = session.state.get("request_type", "daily_limit")
            mode_bucket = session.state.get("mode_bucket", "")
            session.pending_input = PendingInput("request_reason", "Напишите причину заявки.")
            session.state = {"request_type": request_type, "mode_bucket": mode_bucket, "request_name": text[len("/request_access ") :].strip()}
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if text.startswith("/ask "):
            await self._handle_ask(event, session, sender_id, sender_profile, is_admin, text[len("/ask ") :].strip())
            return
        if text.startswith("/search "):
            await self._handle_search(event, sender_id, sender_profile, is_admin, text[len("/search ") :].strip())
            return
        if text.startswith("/list "):
            await self._handle_list(event, sender_id, sender_profile, is_admin, text[len("/list ") :].strip())
            return
        if text.startswith("/file "):
            await self._handle_file(event, sender_id, sender_profile, is_admin, text[len("/file ") :].strip())
            return
        if command.startswith("/"):
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="unknown_command", sender_profile=sender_profile, details={"command": command})
            await self._send_text(chat_id, self._unknown_command_text(), buttons=main_buttons, reply_to=event)
            return

        await self._handle_ask(event, session, sender_id, sender_profile, is_admin, text)

    async def _handle_pending_input(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        text: str,
    ) -> None:
        action = session.pending_input.action
        session.pending_input = None
        if action == "ask":
            await self._handle_ask(event, session, sender_id, sender_profile, is_admin, text)
            return
        if action == "search":
            await self._handle_search(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "list":
            await self._handle_list(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "file":
            await self._handle_file(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "set_api":
            await self._handle_set_api(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "set_prompt":
            await self._handle_set_prompt(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "promo":
            await self._handle_promo(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "prompt_profile":
            await self._handle_prompt_profile(event, sender_id, sender_profile, is_admin, text)
            return
        if action == "request_name":
            session.state["request_name"] = text.strip()
            session.pending_input = PendingInput("request_reason", "Напишите причину заявки.")
            await self._send_text(int(event.chat_id), session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        if action == "request_reason":
            await self._handle_access_request(event, session, sender_id, sender_profile, is_admin, text)
            return
        if action == "department_pick":
            selected = self.app_service.resolve_department_action_by_label(text, "проект 11")
            if selected is None:
                await self._send_text(int(event.chat_id), "Выберите один из департаментных режимов кнопкой ниже.", buttons=self._department_action_keyboard(include_all=True), reply_to=event)
                return
            await self._handle_department_action(event, session, sender_id, sender_profile, is_admin, selected["button"], selected["department"])
            return
        await self._send_text(int(event.chat_id), "Неизвестное действие.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_search(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        query: str,
    ) -> None:
        chat_id = int(event.chat_id)
        if not query:
            await self._send_text(chat_id, "Напишите запрос для поиска.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "search", "query": query[:300]})
            await self._send_limit_request_offer(chat_id, session=self._session(chat_id), sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="search", sender_profile=sender_profile, charged=not unlimited_mode, details={"query": query[:500]})
        hits = self.app_service.search(query, api_key=personal_api_key)
        if not hits:
            await self._send_text(chat_id, self.app_service.append_remaining("Ничего подходящего не найдено.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        lines = ["Результаты поиска:"]
        for hit in hits:
            date_text = self.app_service.display_content_with_shift(hit.content_date, getattr(hit, "content_scope", "dated"))
            lines.append(f"#{hit.item_id} | {date_text} | {hit.item_type} | {hit.file_name or '-'}\n{hit.summary}")
        await self._send_text(chat_id, self.app_service.append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_list(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        raw_date: str,
    ) -> None:
        chat_id = int(event.chat_id)
        if not raw_date:
            await self._send_text(chat_id, "Отправьте дату в формате DD-MM-YYYY.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "list", "query": raw_date[:100]})
            await self._send_limit_request_offer(chat_id, session=self._session(chat_id), sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        try:
            content_date, items = self.app_service.list_by_date(raw_date)
        except Exception as exc:
            await self._send_text(chat_id, str(exc) or "Укажите дату в формате DD-MM-YYYY.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="list", sender_profile=sender_profile, charged=not unlimited_mode, details={"query": raw_date[:100]})
        if not items:
            await self._send_text(chat_id, self.app_service.append_remaining("Для этой даты ничего не найдено.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        lines = [f"Материалы за {content_date}:"]
        for item in items:
            shift_name = str(item.get("shift_name") or "").strip()
            shift_suffix = f" | смена: {shift_name}" if shift_name else ""
            lines.append(f"#{item['id']} | {item['item_type']} | {item['file_name'] or '-'}{shift_suffix}\n{item['summary']}")
        await self._send_text(chat_id, self.app_service.append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_file(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        raw_item_id: str,
    ) -> None:
        chat_id = int(event.chat_id)
        item_id = raw_item_id.strip()
        if not item_id or not item_id.isdigit():
            await self._send_text(chat_id, "ITEM_ID должен быть числом.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "file", "query": item_id})
            await self._send_limit_request_offer(chat_id, session=self._session(chat_id), sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="file", sender_profile=sender_profile, charged=not unlimited_mode, details={"query": item_id})
        item = self.app_service.get_item(int(item_id))
        if not item:
            await self._send_text(chat_id, self.app_service.append_remaining("Элемент не найден.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        description = self.app_service.describe_item_for_text_only(item)
        await self._send_text(
            chat_id,
            self.app_service.append_remaining(description, remaining, unlimited=unlimited_mode),
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
            reply_to=event,
        )
        return
        if str(item.get("item_type") or "").strip().lower() == "video":
            await self._send_text(chat_id, self.app_service.append_remaining("Видео не отправляется. Бот хранит только извлеченную из видео информацию и отвечает по анализу.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if not self._item_has_real_source(item):
            await self._send_text(chat_id, self.app_service.append_remaining("Оригинал еще не привязан к сообщению в группе хранения.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        ok = await self._send_storage_item(chat_id, item)
        if not ok:
            await self._send_text(chat_id, "Не удалось переслать оригинал из хранилища.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        await self._send_text(chat_id, self.app_service.remaining_line(remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_ask(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        question: str,
    ) -> None:
        chat_id = int(event.chat_id)
        if not question:
            await self._send_text(chat_id, "Напишите вопрос.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        custom_prompt = self.app_service.get_active_prompt(prefs)
        prompt_profile = self.app_service.get_prompt_profile(prefs)
        effective_prompt = self.app_service.build_effective_prompt(
            department=self.app_service.get_user_department(sender_id),
            prompt_profile=prompt_profile,
            custom_prompt=custom_prompt,
        )
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "ask", "question": question[:300]})
            await self._send_limit_request_offer(chat_id, session=session, sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="ask", sender_profile=sender_profile, charged=not unlimited_mode, details={"question": question[:500]})
        managed_options = self.app_service.find_managed_answer_options(question)
        if managed_options:
            if len(managed_options) == 1:
                await self._send_managed_answer_option(event, session, sender_id, sender_profile, is_admin, question, managed_options[0], remaining, unlimited_mode)
                return
            session.pending_managed_choice = ManagedAnswerChoice(question=question, options=managed_options, remaining=remaining, unlimited_mode=unlimited_mode)
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="managed_answer_prompt", sender_profile=sender_profile, details={"count": len(managed_options), "trigger": question[:200]})
            await self._send_text(chat_id, self.app_service.append_remaining(f"Нашел несколько готовых вариантов ответа. Выберите вариант кнопкой ниже или напишите номер от 1 до {len(managed_options)}.", remaining, unlimited=unlimited_mode), buttons=self._managed_answer_keyboard(managed_options), reply_to=event)
            return
        hits = self.app_service.retrieve_answer_hits(question, recent_messages=list(session.recent_messages), api_key=personal_api_key)
        if not hits:
            await self._send_text(chat_id, self.app_service.append_remaining("Подходящих материалов в памяти не найдено.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        answer = self.app_service.answer_from_hits(
            question=question,
            hits=hits,
            recent_messages=list(session.recent_messages),
            api_key=personal_api_key,
            custom_prompt=effective_prompt,
        )
        self._append_history(session, "user", question)
        self._append_history(session, "assistant", answer)
        self.app_service.log_event(
            user_id=sender_id,
            chat_id=chat_id,
            event_type="text_answer",
            sender_profile=sender_profile,
            details={"auto": True, "forced_text_only": True},
        )
        await self._send_text(
            chat_id,
            self.app_service.append_remaining(answer, remaining, unlimited=unlimited_mode),
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
            reply_to=event,
        )
        return
        formats = self.app_service.available_delivery_formats(hits)
        if len(formats) > 1:
            session.pending_delivery = DeliveryChoice(question=question, hits=hits, recent_messages=list(session.recent_messages), api_key=personal_api_key, custom_prompt=effective_prompt, remaining=remaining, unlimited_mode=unlimited_mode)
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="delivery_prompt", sender_profile=sender_profile, details={"formats": formats})
            quoted_formats = " или ".join(f'"{item}"' for item in formats)
            listed_formats = ", ".join(formats)
            await self._send_text(chat_id, self.app_service.append_remaining(f"В памяти есть подходящие материалы в форматах: {listed_formats}. Как отправить ответ? Напишите {quoted_formats}.", remaining, unlimited=unlimited_mode), buttons=self._delivery_keyboard(formats), reply_to=event)
            return
        answer = self.app_service.answer_from_hits(question=question, hits=hits, recent_messages=list(session.recent_messages), api_key=personal_api_key, custom_prompt=effective_prompt)
        self._append_history(session, "user", question)
        self._append_history(session, "assistant", answer)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="text_answer", sender_profile=sender_profile, details={"auto": True})
        await self._send_text(chat_id, self.app_service.append_remaining(answer, remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_department_action(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        question: str,
        action_department: str,
    ) -> None:
        chat_id = int(event.chat_id)
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        custom_prompt = self.app_service.get_active_prompt(prefs)
        prompt_profile = self.app_service.get_prompt_profile(prefs)
        allowed, remaining, used_bonus, mode_bucket = self.app_service.consume_department_action_limit(sender_id, action_department)
        if not allowed:
            session.state = {"request_type": "department_mode", "mode_bucket": mode_bucket}
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="department_limit_block", sender_profile=sender_profile, details={"department": action_department, "question": question[:300], "mode_bucket": mode_bucket})
            await self._send_limit_request_offer(chat_id, session=session, sender_id=sender_id, is_admin=is_admin, department_mode=True)
            return
        answer, hits, date_from = self.app_service.run_department_action(
            user_id=sender_id,
            action_department=action_department,
            question=question,
            recent_messages=list(session.recent_messages),
            api_key=personal_api_key,
            custom_prompt=custom_prompt,
            prompt_profile=prompt_profile,
        )
        self._append_history(session, "user", question)
        self._append_history(session, "assistant", answer)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="department_action", sender_profile=sender_profile, charged=used_bonus, details={"department": action_department, "question": question[:500], "hits": len(hits), "date_from": date_from})
        await self._send_text(chat_id, self.app_service.append_remaining(answer, remaining, unlimited=False), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_prompt_profile(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        raw_value: str,
    ) -> None:
        chat_id = int(event.chat_id)
        profile = self.app_service.normalize_prompt_profile(raw_value)
        if profile is None:
            await self._send_text(chat_id, "Выберите один из профилей prompt кнопкой ниже.", buttons=self._prompt_profile_keyboard(), reply_to=event)
            return
        self.app_service.save_user_prompt_profile(sender_id, profile)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/promt", "profile": profile})
        await self._send_text(chat_id, f"Профиль prompt сохранен: {self.app_service.PROMPT_PROFILE_LABELS.get(profile, profile)}.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_access_request(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        reason: str,
    ) -> None:
        chat_id = int(event.chat_id)
        request_name = session.state.get("request_name", "").strip()
        if not request_name:
            session.pending_input = PendingInput("request_name", "Напишите имя для заявки.")
            await self._send_text(chat_id, session.pending_input.prompt, buttons=self._cancel_keyboard(), reply_to=event)
            return
        request_type = session.state.get("request_type", "daily_limit")
        mode_bucket = session.state.get("mode_bucket", "").strip() or None
        request_id = self.app_service.create_access_request(user_id=sender_id, request_name=request_name, reason=reason, request_type=request_type, mode_bucket=mode_bucket)
        session.state.clear()
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="access_request", sender_profile=sender_profile, details={"request_id": request_id, "reason": reason[:500], "request_type": request_type, "mode_bucket": mode_bucket or ""})
        await self._send_text(chat_id, f"Заявка #{request_id} отправлена администратору.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _send_limit_request_offer(
        self,
        chat_id: int,
        session: ChatSession,
        sender_id: int,
        is_admin: bool,
        *,
        department_mode: bool,
    ) -> None:
        session.pending_input = None
        await self._send_text(chat_id, f"{self.app_service.build_limit_request_prompt(department_mode=department_mode)}\nНажмите кнопку \"{self.BUTTON_REQUEST_ACCESS}\" или используйте /request_access.", buttons=[[self.BUTTON_REQUEST_ACCESS], *self._main_keyboard(is_admin=is_admin, user_id=sender_id)])

    async def _handle_delivery_choice(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        text: str,
    ) -> bool:
        chat_id = int(event.chat_id)
        pending = session.pending_delivery
        if pending is None:
            return False
        choice = self.app_service.normalize_delivery_choice(text)
        if choice == "cancel":
            session.pending_delivery = None
            await self._send_text(
                chat_id,
                "Выбор формата ответа отменен.",
                buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
                reply_to=event,
            )
            return True
        if choice == "cancel":
            session.pending_delivery = None
            await self._send_text(chat_id, "Р’С‹Р±РѕСЂ С„РѕСЂРјР°С‚Р° РѕС‚РІРµС‚Р° РѕС‚РјРµРЅРµРЅ.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return True
        session.pending_delivery = None
        answer = self.app_service.answer_from_hits(
            question=pending.question,
            hits=pending.hits,
            recent_messages=pending.recent_messages,
            api_key=pending.api_key,
            custom_prompt=pending.custom_prompt,
        )
        self._append_history(session, "user", pending.question)
        self._append_history(session, "assistant", answer)
        self.app_service.log_event(
            user_id=sender_id,
            chat_id=chat_id,
            event_type="delivery_choice",
            sender_profile=sender_profile,
            details={"choice": "текст", "forced_text_only": True},
        )
        self.app_service.log_event(
            user_id=sender_id,
            chat_id=chat_id,
            event_type="text_answer",
            sender_profile=sender_profile,
            details={"auto": False, "forced_text_only": True},
        )
        await self._send_text(
            chat_id,
            self.app_service.append_remaining(answer, pending.remaining, unlimited=pending.unlimited_mode),
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
            reply_to=event,
        )
        return True
        formats = self.app_service.available_delivery_formats(pending.hits)
        if choice == "cancel":
            session.pending_delivery = None
            await self._send_text(chat_id, "Выбор формата ответа отменен.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return True
        if choice is None or choice not in formats:
            quoted_formats = " или ".join(f'"{item}"' for item in formats)
            await self._send_text(chat_id, f"Сейчас ожидаю выбор формата ответа. Напишите {quoted_formats}.", buttons=self._delivery_keyboard(formats), reply_to=event)
            return True
        session.pending_delivery = None
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="delivery_choice", sender_profile=sender_profile, details={"choice": choice})
        if choice == "текст":
            answer = self.app_service.answer_from_hits(question=pending.question, hits=pending.hits, recent_messages=pending.recent_messages, api_key=pending.api_key, custom_prompt=pending.custom_prompt)
            self._append_history(session, "user", pending.question)
            self._append_history(session, "assistant", answer)
            self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="text_answer", sender_profile=sender_profile, details={"auto": False})
            await self._send_text(chat_id, self.app_service.append_remaining(answer, pending.remaining, unlimited=pending.unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return True
        media_hits = self.app_service.hits_for_delivery_choice(pending.hits, choice)
        forwardable_hits = [hit for hit in media_hits if self._is_forwardable_hit(hit)]
        if not forwardable_hits:
            await self._send_text(chat_id, "Подходящих медиа для этого выбора не найдено или оригинал еще не привязан.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return True
        self._append_history(session, "user", pending.question)
        self._append_history(session, "assistant", f"Отправил материалы формата: {choice}.")
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="media_delivery", sender_profile=sender_profile, details={"choice": choice, "count": len(forwardable_hits)})
        await self._send_text(chat_id, self.app_service.append_remaining(f"Отправляю материалы в формате: {choice}.", pending.remaining, unlimited=pending.unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
        await self._forward_hits(chat_id, forwardable_hits)
        return True

    async def _handle_promo(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        code: str,
    ) -> None:
        chat_id = int(event.chat_id)
        promo_code = code.strip()
        if not promo_code:
            await self._send_text(chat_id, "Отправьте промокод для активации дополнительных запросов.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        ok, message = self.app_service.redeem_promo_code(sender_id, promo_code)
        bonus_requests = self.app_service.get_user_bonus_requests(sender_id)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="promo_redeem", sender_profile=sender_profile, details={"code": promo_code[:80], "ok": ok, "bonus_requests": bonus_requests})
        suffix = f"\n\nДоступно бонусных запросов: {bonus_requests}." if ok else ""
        await self._send_text(chat_id, f"{message}{suffix}", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_custom_command(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        custom_command: dict[str, Any],
    ) -> None:
        chat_id = int(event.chat_id)
        command_name = str(custom_command.get("command_name") or "").strip() or "/command"
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="custom_command", sender_profile=sender_profile, details={"command": command_name})
        if bool(custom_command.get("notify_admin", 1)):
            await self._notify_custom_command_admins(command_name=command_name, sender_id=sender_id, sender_profile=sender_profile)
        response_text = str(custom_command.get("response_text") or "").strip()
        media_path = Path(str(custom_command.get("media_path") or "")) if custom_command.get("media_path") else None
        if response_text:
            await self._send_text(chat_id, response_text, buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
        if custom_command.get("media_path"):
            if not response_text:
                await self._send_text(chat_id, f"Для команды {command_name} настроен файл, но отправка файлов ботом отключена.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if media_path and media_path.exists() and self._is_video_path(media_path):
            await self._send_text(chat_id, f"Видео для команды {command_name} не отправляется. Бот хранит только извлеченную информацию.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if media_path and media_path.exists():
            await self._send_local_file(chat_id, media_path)
            return
        if custom_command.get("media_path"):
            await self._send_text(chat_id, f"Медиа для команды {command_name} пока недоступно.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if not response_text:
            await self._send_text(chat_id, f"Команда {command_name} выполнена.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _notify_custom_command_admins(self, *, command_name: str, sender_id: int, sender_profile: SenderProfile) -> None:
        admin_ids = sorted(self.app_service.external_admin_user_ids())
        if not admin_ids:
            return
        username = f"@{sender_profile.username}" if sender_profile.username else "-"
        name = " ".join(part for part in [sender_profile.first_name, sender_profile.last_name] if part) or "-"
        text = f"Ввели команду {command_name}.\nПользователь: {name}\nUsername: {username}\nUser ID: {sender_id}"
        for admin_id in admin_ids:
            try:
                await self._send_text(admin_id, text, buttons=self._main_keyboard(is_admin=True))
            except Exception:
                LOGGER.exception("Failed to notify Telegram admin %s about %s", admin_id, command_name)

    async def _handle_managed_answer_choice(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        text: str,
    ) -> bool:
        chat_id = int(event.chat_id)
        pending = session.pending_managed_choice
        if pending is None:
            return False
        normalized = self._normalize_choice_text(text)
        if normalized in {"отмена", "cancel"}:
            session.pending_managed_choice = None
            await self._send_text(chat_id, "Выбор варианта ответа отменен.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return True
        selected_option = self._resolve_managed_answer_option(text, pending.options)
        if selected_option is None:
            await self._send_text(chat_id, f"Сейчас ожидаю выбор готового варианта ответа. Нажмите кнопку ниже или напишите номер от 1 до {len(pending.options)}.", buttons=self._managed_answer_keyboard(pending.options), reply_to=event)
            return True
        session.pending_managed_choice = None
        await self._send_managed_answer_option(event, session, sender_id, sender_profile, is_admin, pending.question, selected_option, pending.remaining, pending.unlimited_mode)
        return True

    async def _send_managed_answer_option(
        self,
        event: events.NewMessage.Event,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        question: str,
        option: ManagedAnswerOption,
        remaining: int,
        unlimited_mode: bool,
    ) -> None:
        chat_id = int(event.chat_id)
        media_path = Path(option.media_path) if option.media_path else None
        media_exists = bool(media_path and media_path.exists())
        video_media_blocked = media_exists and self._is_video_path(media_path)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="managed_answer_choice", sender_profile=sender_profile, details={"option_id": option.option_id, "option_label": option.option_label, "has_media": media_exists, "video_blocked": video_media_blocked})
        self._append_history(session, "user", question)
        assistant_text = option.response_text.strip() or f"Отправил вариант: {option.option_label}."
        self._append_history(session, "assistant", assistant_text)
        if option.response_text.strip():
            await self._send_text(chat_id, self.app_service.append_remaining(option.response_text.strip(), remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if option.media_path:
            await self._send_text(chat_id, self.app_service.append_remaining("Для этого варианта настроен файл, но бот отвечает только текстом.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        elif media_exists and not video_media_blocked:
            await self._send_text(chat_id, self.app_service.append_remaining(f"Отправляю вариант: {option.option_label}.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
        elif video_media_blocked:
            await self._send_text(chat_id, self.app_service.append_remaining("Для этого варианта настроено видео, но бот не отправляет видеофайлы. Используйте текстовый ответ.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        else:
            await self._send_text(chat_id, self.app_service.append_remaining("Для этого варианта пока не настроен ответ.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if media_exists and not video_media_blocked:
            await self._send_local_file(chat_id, media_path)
        elif option.media_path and not video_media_blocked:
            await self._send_text(chat_id, "Медиа для выбранного варианта пока недоступно.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    def _resolve_managed_answer_option(self, raw_text: str, options: list[ManagedAnswerOption]) -> ManagedAnswerOption | None:
        clean = raw_text.strip()
        if clean.isdigit():
            index = int(clean) - 1
            if 0 <= index < len(options):
                return options[index]
        normalized = self._normalize_choice_text(clean)
        for index, option in enumerate(options, start=1):
            candidates = {self._normalize_choice_text(option.option_label), self._normalize_choice_text(f"{index}"), self._normalize_choice_text(f"{index}. {option.option_label}")}
            if normalized in candidates:
                return option
        return None

    @staticmethod
    def _normalize_choice_text(text: str) -> str:
        return text.strip().lower().strip("\"' .,!?:;")

    async def _handle_set_api(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        api_key: str,
    ) -> None:
        chat_id = int(event.chat_id)
        if not api_key:
            await self._send_text(chat_id, "Отправьте API token.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/set_api"})
        ok, error_text = self.app_service.validate_user_api_key(api_key)
        if not ok:
            self.app_service.save_user_api_error(sender_id, error_text or "unknown error")
            await self._send_text(
                chat_id,
                "API token не прошел проверку. Убедитесь, что он действителен и имеет доступ к настроенным моделям.\n\n"
                f"Текст ошибки: {(error_text or 'unknown error')[:400]}",
                buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id),
                reply_to=event,
            )
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        has_saved_prompt = bool((prefs.get("custom_prompt") or "").strip())
        self.app_service.save_user_api_key(sender_id, api_key)
        text = "Ваш API token сохранен и проверен. Для вас включен безлимит."
        if has_saved_prompt:
            text += " Ранее сохраненный prompt снова активирован."
        await self._send_text(chat_id, text, buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_delete_api(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
    ) -> None:
        chat_id = int(event.chat_id)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/delete_api"})
        prefs = self.app_service.get_user_preferences(sender_id)
        had_prompt = bool((prefs.get("custom_prompt") or "").strip())
        self.app_service.clear_user_api_key(sender_id)
        text = "Ваш API token удален. Безлимит отключен."
        if had_prompt:
            text = "Ваш API token удален. Безлимит отключен, пользовательский prompt сохранен, но не будет применяться, пока вы снова не добавите API token."
        await self._send_text(chat_id, text, buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_set_prompt(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        prompt_text: str,
    ) -> None:
        chat_id = int(event.chat_id)
        if not prompt_text:
            await self._send_text(chat_id, "Отправьте ваш пользовательский prompt.", buttons=self._cancel_keyboard(), reply_to=event)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/set_prompt"})
        prefs = self.app_service.get_user_preferences(sender_id)
        if not self.app_service.get_active_api_key(prefs):
            await self._send_text(chat_id, "Сначала добавьте рабочий API token через /set_api.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        self.app_service.save_user_prompt(sender_id, prompt_text)
        await self._send_text(chat_id, "Ваш пользовательский prompt сохранен.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_delete_prompt(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
    ) -> None:
        chat_id = int(event.chat_id)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="settings", sender_profile=sender_profile, details={"command": "/delete_prompt"})
        self.app_service.clear_user_prompt(sender_id)
        await self._send_text(chat_id, "Ваш пользовательский prompt удален.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_stats(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        raw_arg: str,
    ) -> None:
        chat_id = int(event.chat_id)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="stats_view", sender_profile=sender_profile)
        rows = self.app_service.get_user_statistics(raw_arg)
        if raw_arg:
            if not rows:
                await self._send_text(chat_id, "Статистика по этому пользователю не найдена.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
                return
            await self._send_text(chat_id, self.app_service.format_detailed_user_stats(rows[0]), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        if not rows:
            await self._send_text(chat_id, "Статистика пока пуста.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        lines = ["Статистика по пользователям Telegram:"]
        for row in rows:
            lines.append(self.app_service.format_user_stats_row(row))
        await self._send_text(chat_id, "\n".join(lines), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _handle_department_survey(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        text: str,
    ) -> bool:
        chat_id = int(event.chat_id)
        if self.app_service.has_completed_department_survey(sender_id):
            return False
        department = self.app_service.normalize_department(text)
        if department is None:
            await self._send_text(chat_id, self._department_prompt_text(), buttons=self._department_keyboard(), reply_to=event)
            return True
        self.app_service.save_user_department(sender_id, department)
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="department_selected", sender_profile=sender_profile, details={"department": department})
        await self._send_text(chat_id, f"Спасибо! Сохранил ваш департамент: {department}. Теперь можно пользоваться ботом.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
        return True

    async def _handle_homosap(
        self,
        event: events.NewMessage.Event,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
    ) -> None:
        chat_id = int(event.chat_id)
        video_path = self.settings.homosap_video_path
        file_ready = video_path.exists()
        self.app_service.log_event(user_id=sender_id, chat_id=chat_id, event_type="quest_homosap", sender_profile=sender_profile, details={"command": "/HOMOSAP", "file_ready": file_ready})
        await self._notify_homosap_admins(sender_id=sender_id, sender_profile=sender_profile, file_ready=file_ready)
        if not file_ready:
            await self._send_text(chat_id, "Видео HOMOSAP пока не загружено. Попробуйте позже.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)
            return
        await self._send_text(chat_id, "Видео HOMOSAP не отправляется. Бот хранит только извлеченную информацию.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id), reply_to=event)

    async def _notify_homosap_admins(self, *, sender_id: int, sender_profile: SenderProfile, file_ready: bool) -> None:
        admin_ids = sorted(self.app_service.external_admin_user_ids())
        if not admin_ids:
            LOGGER.warning("Telegram /HOMOSAP invoked by %s, but no uploader admins are configured.", sender_id)
            return
        username = f"@{sender_profile.username}" if sender_profile.username else "-"
        name = " ".join(part for part in [sender_profile.first_name, sender_profile.last_name] if part) or "-"
        status_text = "Файл HOMOSAP.mp4 найден, но отправка видео пользователю отключена." if file_ready else "Файл HOMOSAP.mp4 пока отсутствует на диске."
        text = f"Ввели команду /HOMOSAP.\nПользователь: {name}\nUsername: {username}\nUser ID: {sender_id}\nСтатус: {status_text}"
        for admin_id in admin_ids:
            try:
                await self._send_text(admin_id, text, buttons=self._main_keyboard(is_admin=True))
            except Exception:
                LOGGER.exception("Failed to notify Telegram admin %s about /HOMOSAP", admin_id)

    async def _maybe_send_welcome(self, event: events.NewMessage.Event, user_id: int, is_admin: bool) -> bool:
        if self.app_service.has_sent_welcome(user_id):
            return False
        await self._send_text(int(event.chat_id), self._welcome_text(is_admin=is_admin, user_id=user_id), buttons=self._main_keyboard(is_admin=is_admin, user_id=user_id), reply_to=event)
        self.app_service.mark_welcome_sent(user_id)
        return True

    def _append_history(self, session: ChatSession, role: str, content: str) -> None:
        clean = content.strip()
        if clean:
            session.recent_messages.append({"role": role, "content": clean})

    async def _send_text(
        self,
        chat_id: int,
        text: str,
        *,
        buttons: list[list[Any]] | ReplyKeyboardMarkup | None = None,
        reply_to: events.NewMessage.Event | int | None = None,
    ) -> None:
        parts = split_for_telegram(text)
        if not parts:
            return
        reply_to_id = None
        if isinstance(reply_to, int):
            reply_to_id = reply_to
        elif reply_to is not None:
            reply_to_id = int(getattr(getattr(reply_to, "message", None), "id", 0) or 0) or None
        for index, part in enumerate(parts):
            await self.client.send_message(int(chat_id), part, buttons=buttons if index == len(parts) - 1 else None, reply_to=reply_to_id if index == 0 else None)

    async def _send_local_file(self, chat_id: int, file_path: Path, *, caption: str | None = None) -> None:
        await self._send_text(int(chat_id), caption or "Отправка файлов ботом отключена. Используйте текстовый ответ.")

    async def _send_storage_item(self, chat_id: int, item: dict[str, Any]) -> bool:
        LOGGER.info("Telegram file sending is disabled | item_id=%s", item.get("id"))
        return False

    async def _forward_hits(self, chat_id: int, hits: list[SearchHit]) -> None:
        LOGGER.info("Telegram hit forwarding is disabled | count=%s", len(hits))
        await self._send_text(int(chat_id), "Бот не отправляет файлы. Могу ответить только текстом.")
        return
        seen_pairs: set[tuple[int, int]] = set()
        sent_any = False
        for hit in hits:
            if not self._is_forwardable_hit(hit):
                continue
            key = (int(hit.source_chat_id), int(hit.source_message_id))
            if key in seen_pairs:
                continue
            seen_pairs.add(key)
            try:
                await self.client.forward_messages(int(chat_id), int(hit.source_message_id), from_peer=int(hit.source_chat_id))
                sent_any = True
            except errors.PeerIdInvalidError:
                LOGGER.warning("Skipping hit with invalid Telegram source peer | item_id=%s", hit.item_id)
            except Exception:
                LOGGER.exception("Failed to forward Telegram hit item_id=%s", hit.item_id)
        if not sent_any:
            await self._send_text(int(chat_id), "Оригиналы для пересылки пока недоступны или еще не привязаны к сообщению в группе.")

    @staticmethod
    def _message_metadata_patch(message: object) -> dict[str, Any]:
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

    @staticmethod
    def _item_has_real_source(item: dict[str, Any]) -> bool:
        source_chat_id = int(item.get("source_chat_id") or 0)
        source_message_id = int(item.get("source_message_id") or 0)
        return source_chat_id != 0 and source_message_id > 0

    @staticmethod
    def _is_forwardable_hit(hit: SearchHit) -> bool:
        return int(hit.source_chat_id or 0) != 0 and int(hit.source_message_id or 0) > 0 and str(hit.item_type or "").strip().lower() != "video"

    @classmethod
    def _is_video_path(cls, media_path: Path | None) -> bool:
        return bool(media_path and media_path.suffix.lower() in cls.VIDEO_SUFFIXES)

    def _main_keyboard(self, *, is_admin: bool, user_id: int | None = None) -> list[list[str]]:
        rows = [[self.BUTTON_ASK, self.BUTTON_SEARCH], [self.BUTTON_LIST, self.BUTTON_FILE]]
        department_button = self.app_service.department_button_label(user_id) if user_id is not None else None
        if department_button:
            rows.append([department_button, self.BUTTON_HELP])
        else:
            rows.append([self.BUTTON_SETTINGS, self.BUTTON_HELP])
        rows.append([self.BUTTON_SETTINGS, self.BUTTON_REQUEST_ACCESS])
        if is_admin:
            rows.append([self.BUTTON_STATS, self.BUTTON_LOCAL_UPLOAD])
        return rows

    def _settings_keyboard(self, *, is_admin: bool, user_id: int | None = None) -> list[list[str]]:
        rows = [
            [self.BUTTON_SET_API, self.BUTTON_DELETE_API],
            [self.BUTTON_SET_PROMPT, self.BUTTON_DELETE_PROMPT],
            [self.BUTTON_PROMT, self.BUTTON_MY_SETTINGS],
            [self.BUTTON_BACK, self.BUTTON_REQUEST_ACCESS],
        ]
        if is_admin:
            rows.append([self.BUTTON_LOCAL_UPLOAD, self.BUTTON_STATS])
        return rows

    def _cancel_keyboard(self) -> list[list[str]]:
        return [[self.BUTTON_CANCEL, self.BUTTON_BACK]]

    def _department_keyboard(self) -> list[list[str]]:
        options = self.app_service.department_options()
        rows: list[list[str]] = []
        current: list[str] = []
        for option in options:
            current.append(option)
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        return rows

    def _department_action_keyboard(self, *, include_all: bool) -> list[list[str]]:
        labels = self.app_service.all_department_action_labels() if include_all else []
        rows: list[list[str]] = []
        current: list[str] = []
        for label in labels:
            current.append(label)
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        rows.append([self.BUTTON_CANCEL])
        return rows

    def _prompt_profile_keyboard(self) -> list[list[str]]:
        rows: list[list[str]] = []
        current: list[str] = []
        for label in self.app_service.prompt_profile_options():
            current.append(label)
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        rows.append([self.BUTTON_CANCEL])
        return rows

    @staticmethod
    def _department_prompt_text() -> str:
        return "Обязательный опрос перед началом работы. Какой вы департамент? Выберите один вариант кнопкой ниже."

    def _delivery_keyboard(self, formats: list[str]) -> list[list[str]]:
        rows: list[list[str]] = []
        current: list[str] = []
        for label in formats:
            current.append(label)
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        rows.append([self.BUTTON_CANCEL])
        return rows

    def _managed_answer_keyboard(self, options: list[ManagedAnswerOption]) -> list[list[str]]:
        rows: list[list[str]] = []
        current: list[str] = []
        for index, option in enumerate(options, start=1):
            current.append(f"{index}. {option.option_label}"[:40])
            if len(current) == 2:
                rows.append(current)
                current = []
        if current:
            rows.append(current)
        rows.append([self.BUTTON_CANCEL])
        return rows

    def _help_text(self, *, is_admin: bool, user_id: int | None = None) -> str:
        lines = [
            "Команды Telegram-бота:",
            "/start, /help, /menu - показать меню",
            "/ask <вопрос> - задать вопрос по памяти",
            "/search <запрос> - найти подходящие материалы",
            "/list <DD-MM-YYYY> - список материалов за дату",
            "/file <ITEM_ID> - переслать оригинал из хранилища",
            "/promo <код> - активировать промокод",
            "/promt - выбрать профиль prompt",
            "/request_access - отправить заявку администратору при исчерпании лимита",
            "/set_api <token> - подключить свой OpenAI API token",
            "/delete_api - удалить свой API token",
            "/set_prompt <текст> - сохранить свой prompt",
            "/delete_prompt - удалить свой prompt",
            "/my_settings - показать настройки",
        ]
        department_button = self.app_service.department_button_label(user_id) if user_id is not None else None
        if department_button:
            lines.append(f"{department_button} - специальная функция вашего департамента (1 раз в день)")
        if is_admin:
            lines.extend(["/stats [USER_ID] - статистика по пользователям Telegram", "/upload_local - открыть Telegram localhost-админку"])
        lines.extend([
            "",
            f"Без личного API token лимит: {self.settings.daily_message_limit} сообщений в день.",
            "Промокоды добавляют бонусные запросы сверх дневного лимита.",
            "С личным рабочим API token: безлимит.",
            f"Бот учитывает контекст последних {self.settings.conversation_context_messages} сообщений.",
            "Если найдены и текст, и медиа, бот спросит формат ответа кнопками.",
            "Если админ настроил несколько готовых вариантов ответа, бот предложит выбор кнопками.",
        ])
        return "\n".join(lines)

    def _welcome_text(self, *, is_admin: bool, user_id: int | None = None) -> str:
        intro = ["Привет! Это Telegram-бот с RAG-памятью."]
        if is_admin:
            intro.append("У вас есть доступ к статистике Telegram и Telegram-разделу localhost-админки.")
        else:
            intro.append("Здесь можно искать материалы по тексту, фото, аудио и видео и активировать промокоды.")
        intro.extend(["", self._help_text(is_admin=is_admin, user_id=user_id)])
        return "\n".join(intro)

    def _local_upload_text(self, admin_user_id: int) -> str:
        upload_url = f"{self.settings.local_upload_base_url}{self.app_service.admin_panel_path()}"
        return (
            "Telegram-раздел админ-панели доступен через браузер на этом компьютере. "
            "Через него можно загружать материалы, создавать промокоды, команды и готовые варианты ответов.\n\n"
            f"Ссылка: {upload_url}"
        )

    def _limit_reached_text(self) -> str:
        return (
            f"Лимит на сегодня исчерпан: {self.settings.daily_message_limit} сообщений. "
            "Добавьте свой API token через /set_api, активируйте промокод через /promo или отправьте заявку через /request_access."
        )

    @staticmethod
    def _unknown_command_text() -> str:
        return 'Такой команды нет. Если надо добавить, напишите: "пж добавьте админы".'
