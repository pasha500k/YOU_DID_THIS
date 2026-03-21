"""
????: bot.py
?????? legacy-?????????? ??????? Telegram userbot ?? Telethon,
??????? ????????? ? ??????? ??? ???????? ????????????? ? ?????????.
"""

from __future__ import annotations

from collections import defaultdict, deque
import logging
from typing import Any

from telethon import TelegramClient, events

from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.schemas import PendingAdminAddRequest, PendingDeliveryRequest, SearchHit
from telegram_rag_memory_bot.services.rag_service import RagService
from telegram_rag_memory_bot.utils.dates import DateParseError, infer_content_date, parse_iso_date, today_iso
from telegram_rag_memory_bot.utils.text import split_for_telegram, trim_text

LOGGER = logging.getLogger(__name__)


class TelegramRagMemoryBot:
    def __init__(self, settings: Settings, rag_service: RagService) -> None:
        self.settings = settings
        self.rag_service = rag_service
        self.client = TelegramClient(
            settings.telegram_session_name,
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self.self_user_id: int | None = None
        self.chat_histories: dict[int, deque[dict[str, str]]] = defaultdict(
            lambda: deque(maxlen=self.settings.conversation_context_messages)
        )
        self.pending_delivery_requests: dict[int, PendingDeliveryRequest] = {}
        self.pending_admin_add_requests: dict[int, PendingAdminAddRequest] = {}
        self._register_handlers()

    async def run(self) -> None:
        await self.client.start()
        me = await self.client.get_me()
        self.self_user_id = int(me.id)
        LOGGER.info("Telegram account connected as id=%s", self.self_user_id)
        await self.client.run_until_disconnected()

    def _register_handlers(self) -> None:
        @self.client.on(events.NewMessage(chats=self.settings.storage_chat_id, pattern=r"^/"))
        async def storage_command_handler(event: events.NewMessage.Event) -> None:
            await self._handle_storage_command(event)

        watched_chat_ids = self.settings.video_download_chat_ids | self.settings.auto_ingest_chat_ids
        if watched_chat_ids:
            @self.client.on(events.NewMessage(chats=list(watched_chat_ids), incoming=True))
            async def auto_group_message_handler(event: events.NewMessage.Event) -> None:
                await self._handle_auto_group_message(event)

        @self.client.on(events.NewMessage(incoming=True))
        async def private_message_handler(event: events.NewMessage.Event) -> None:
            if not event.is_private:
                return
            await self._handle_private_message(event)

    async def _handle_storage_command(self, event: events.NewMessage.Event) -> None:
        sender_id = getattr(event, "sender_id", None)
        text = (event.raw_text or "").strip()
        command = text.split(maxsplit=1)[0].lower()

        if command == "/help":
            await event.reply(self._storage_help_text())
            return

        if not self._is_uploader(sender_id):
            return

        try:
            if command == "/date":
                await self._handle_date_command(event, text)
                return
            if command == "/delete":
                await self._handle_delete_command(event, text)
                return
            await event.reply(self._unknown_command_text())
        except Exception as exc:
            LOGGER.exception("Storage command failed")
            await event.reply(f"Ошибка: {exc}")

    async def _handle_auto_group_message(self, event: events.NewMessage.Event) -> None:
        message = event.message
        if message is None:
            return

        raw_text = (event.raw_text or "").strip()
        if raw_text.startswith("/"):
            return

        chat_id = int(event.chat_id)
        is_video = self.rag_service.media_service.is_video_message(message)
        has_supported_content = bool(getattr(message, "media", None) or raw_text)
        if not has_supported_content:
            return

        try:
            if chat_id in self.settings.video_download_chat_ids and is_video:
                saved_path = await self.rag_service.media_service.download_video_message(self.client, message)
                if saved_path is not None:
                    LOGGER.info(
                        "Downloaded video from chat %s message %s to %s",
                        event.chat_id,
                        getattr(message, "id", None),
                        saved_path,
                    )

            if chat_id in self.settings.auto_ingest_chat_ids:
                content_text = self.rag_service.media_service.get_caption(message)
                content_date = infer_content_date(content_text)
                ingested = await self.rag_service.ingest_message(
                    client=self.client,
                    message=message,
                    content_date=content_date,
                    ingested_by_user_id=int(getattr(message, "sender_id", 0) or 0),
                )
                LOGGER.info(
                    "Auto-ingested chat %s message %s as item #%s with date %s",
                    event.chat_id,
                    getattr(message, "id", None),
                    ingested.item_id,
                    ingested.content_date,
                )
        except Exception:
            LOGGER.exception(
                "Failed to process group message from chat %s message %s",
                event.chat_id,
                getattr(message, "id", None),
            )

    async def _handle_private_message(self, event: events.NewMessage.Event) -> None:
        sender_id = getattr(event, "sender_id", None)
        if not self._is_authorized(sender_id):
            await event.reply("Доступ запрещен.")
            return

        if sender_id is None:
            await event.reply("Не удалось определить пользователя.")
            return

        sender = await event.get_sender()
        sender_profile = self._sender_profile(sender)
        chat_id = int(event.chat_id)
        is_admin = self._is_uploader(sender_id)
        text = (event.raw_text or "").strip()
        command = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""
        welcome_sent = await self._maybe_send_first_welcome(event, sender_id, is_admin)

        personal_api_key: str | None = None
        try:
            if text in {"/help", "/start"} or text.startswith("/help ") or text.startswith("/start "):
                if welcome_sent:
                    return
                await self._reply_long(event, self._private_help_text(is_admin=is_admin))
                return

            if command in {"/add", "/cancel_add"} and not is_admin:
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="unknown_command",
                    sender_profile=sender_profile,
                    details={"command": command, "admin_only": True},
                )
                await event.reply(f"Команда {command} доступна только администратору.")
                return

            if command == "/cancel_add":
                if self.pending_admin_add_requests.pop(chat_id, None) is not None:
                    await event.reply("Ручное добавление отменено.")
                    return
                await event.reply("Сейчас нет активного ручного добавления.")
                return

            if command == "/add":
                self.pending_delivery_requests.pop(chat_id, None)
                self.pending_admin_add_requests[chat_id] = PendingAdminAddRequest(stage="awaiting_content")
                await event.reply(
                    "Режим ручного добавления запущен.\n"
                    "1. Пришлите файл или текст в этот чат.\n"
                    "2. Потом отправьте дату в формате YYYY-MM-DD.\n"
                    "3. Потом отправьте описание материала.\n\n"
                    "После этого бот сам отправит материал в группу-хранилище, выполнит анализ и индексацию.\n"
                    "Для отмены используйте /cancel_add."
                )
                return

            if is_admin and chat_id in self.pending_admin_add_requests:
                if command.startswith("/"):
                    await event.reply(
                        "Сейчас идет ручное добавление. Пришлите данные по шагам или отмените через /cancel_add."
                    )
                    return
                if await self._maybe_handle_pending_admin_add(event, text, sender_id, sender_profile):
                    return

            if not text:
                return

            if command == "/stats":
                if not is_admin:
                    self._log_user_event(
                        user_id=sender_id,
                        chat_id=chat_id,
                        event_type="unknown_command",
                        sender_profile=sender_profile,
                        details={"command": command},
                    )
                    await event.reply("Команда /stats доступна только администратору.")
                    return
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="stats_view",
                    sender_profile=sender_profile,
                )
                raw_arg = text[len("/stats") :].strip() if text.startswith("/stats") else ""
                await self._handle_stats(event, raw_arg)
                return

            if text == "/set_api":
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/set_api"},
                )
                await event.reply("Использование: /set_api sk-...")
                return
            if text.startswith("/set_api "):
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/set_api"},
                )
                await self._handle_set_api(event, sender_id, text[len("/set_api ") :].strip())
                return
            if text == "/delete_api":
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/delete_api"},
                )
                await self._handle_delete_api(event, sender_id)
                return
            if text == "/set_prompt":
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/set_prompt"},
                )
                await event.reply("Использование: /set_prompt ваш системный промпт")
                return
            if text.startswith("/set_prompt "):
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/set_prompt"},
                )
                await self._handle_set_prompt(event, sender_id, text[len("/set_prompt ") :].strip())
                return
            if text == "/delete_prompt":
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/delete_prompt"},
                )
                await self._handle_delete_prompt(event, sender_id)
                return
            if text == "/my_settings":
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": "/my_settings"},
                )
                await self._handle_my_settings(event, sender_id)
                return
            if command.startswith("/") and command not in self._private_command_names():
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="unknown_command",
                    sender_profile=sender_profile,
                    details={"command": command},
                )
                await event.reply(self._unknown_command_text())
                return

            if not command.startswith("/") and chat_id in self.pending_delivery_requests:
                if await self._maybe_handle_pending_delivery_choice(event, text, sender_id, sender_profile):
                    return

            user_prefs = self.rag_service.get_user_preferences(sender_id)
            personal_api_key = self._get_active_api_key(user_prefs)
            custom_prompt = self._get_active_prompt(user_prefs)

            if text == "/search":
                await event.reply("Использование: /search ваш запрос")
                return
            if text == "/ask":
                await event.reply("Использование: /ask ваш вопрос")
                return
            if text == "/list":
                await event.reply("Использование: /list YYYY-MM-DD")
                return
            if text == "/file":
                await event.reply("Использование: /file ITEM_ID")
                return

            request_kind, payload = self._resolve_request(text)
            allowed, remaining, unlimited_mode = self._consume_daily_limit(
                sender_id,
                has_personal_api=bool(personal_api_key),
            )
            if not allowed:
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="limit_block",
                    sender_profile=sender_profile,
                    details={"request_kind": request_kind},
                )
                await event.reply(
                    f"Лимит на сегодня исчерпан: {self.settings.daily_message_limit} сообщений. Попробуйте завтра или добавьте свой API token через /set_api."
                )
                return

            self.pending_delivery_requests.pop(chat_id, None)
            self._log_user_event(
                user_id=sender_id,
                chat_id=chat_id,
                event_type=request_kind,
                charged=not unlimited_mode,
                sender_profile=sender_profile,
            )

            if request_kind == "search":
                await self._handle_search(event, payload, remaining, personal_api_key, unlimited_mode)
                return
            if request_kind == "list":
                await self._handle_list(event, payload, remaining, unlimited_mode)
                return
            if request_kind == "file":
                await self._handle_file(event, payload, remaining, unlimited_mode)
                return
            await self._handle_ask(
                event,
                payload,
                remaining,
                personal_api_key,
                custom_prompt,
                unlimited_mode,
                sender_id,
                sender_profile,
            )
        except Exception as exc:
            LOGGER.exception("Private command failed")
            if personal_api_key:
                self.rag_service.set_user_api_key_error(sender_id, str(exc))
                await event.reply(
                    "Ошибка при использовании вашего API token. Проверьте ключ через /set_api заново или удалите его командой /delete_api."
                )
                return
            await event.reply(f"Ошибка: {exc}")
    async def _handle_set_api(self, event: events.NewMessage.Event, user_id: int, api_key: str) -> None:
        if not api_key:
            await event.reply("Использование: /set_api sk-...")
            return

        ok, error_text = self.rag_service.validate_user_api_key(api_key)
        if not ok:
            self.rag_service.set_user_api_key_error(user_id, error_text or "unknown error")
            await event.reply(
                "API token не прошел проверку. Убедитесь, что он действителен и имеет доступ к настроенным моделям.\n\n"
                f"Текст ошибки: {(error_text or 'unknown error')[:400]}"
            )
            return

        existing = self.rag_service.get_user_preferences(user_id)
        self.rag_service.set_user_api_key(user_id, api_key)
        has_saved_prompt = bool((existing.get("custom_prompt") or "").strip())
        if has_saved_prompt:
            await event.reply(
                "Ваш API token сохранен и проверен. Для вас включен безлимит. Ранее сохраненный prompt снова активирован."
            )
            return
        await event.reply("Ваш API token сохранен и проверен. Для вас включен безлимит.")

    async def _handle_delete_api(self, event: events.NewMessage.Event, user_id: int) -> None:
        existing = self.rag_service.get_user_preferences(user_id)
        had_prompt = bool((existing.get("custom_prompt") or "").strip())
        self.rag_service.clear_user_api_key(user_id)
        if had_prompt:
            await event.reply(
                "Ваш API token удален. Безлимит отключен, пользовательский prompt сохранен, но не будет применяться, пока вы снова не добавите API token."
            )
            return
        await event.reply("Ваш API token удален. Безлимит отключен.")

    async def _handle_set_prompt(self, event: events.NewMessage.Event, user_id: int, prompt_text: str) -> None:
        if not prompt_text:
            await event.reply("Использование: /set_prompt ваш системный промпт")
            return

        prefs = self.rag_service.get_user_preferences(user_id)
        if not self._get_active_api_key(prefs):
            await event.reply(
                "Сначала добавьте рабочий API token через /set_api. Prompt хранится отдельно и работает только при активном личном токене."
            )
            return

        self.rag_service.set_user_prompt(user_id, prompt_text)
        await event.reply("Ваш пользовательский prompt сохранен и будет применяться к ответам, пока активен ваш API token.")

    async def _handle_delete_prompt(self, event: events.NewMessage.Event, user_id: int) -> None:
        self.rag_service.clear_user_prompt(user_id)
        await event.reply("Ваш пользовательский prompt удален.")

    async def _handle_my_settings(self, event: events.NewMessage.Event, user_id: int) -> None:
        prefs = self.rag_service.get_user_preferences(user_id)
        has_api = bool(self._get_active_api_key(prefs))
        has_prompt = bool((prefs.get("custom_prompt") or "").strip())
        prompt_active = has_api and has_prompt
        lines = [
            f"Личный API token: {'подключен' if has_api else 'не подключен'}",
            f"Безлимит: {'включен' if has_api else 'нет'}",
            f"Пользовательский prompt сохранен: {'да' if has_prompt else 'нет'}",
            f"Пользовательский prompt активен: {'да' if prompt_active else 'нет'}",
        ]
        last_error = (prefs.get("api_key_last_error") or "").strip()
        if last_error:
            lines.append(f"Последняя ошибка API: {last_error[:300]}")
        if not has_api:
            lines.append(f"Лимит без личного токена: {self.settings.daily_message_limit} сообщений в день.")
        await self._reply_long(event, "\n".join(lines))

    async def _handle_date_command(self, event: events.NewMessage.Event, text: str) -> None:
        if not event.is_reply:
            await event.reply("Ответьте на сообщение или файл командой /date YYYY-MM-DD")
            return

        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            await event.reply("Использование: /date YYYY-MM-DD")
            return

        content_date = parse_iso_date(parts[1].split()[0])
        reply_message = await event.get_reply_message()
        if reply_message is None:
            await event.reply("Не удалось загрузить сообщение, на которое вы ответили.")
            return

        ingested = await self.rag_service.ingest_message(
            client=self.client,
            message=reply_message,
            content_date=content_date,
            ingested_by_user_id=int(getattr(event, "sender_id") or 0),
        )
        await event.reply(
            "\n".join(
                [
                    f"Проиндексировано: #{ingested.item_id}",
                    f"Дата: {ingested.content_date}",
                    f"Тип: {ingested.item_type}",
                    f"Файл: {ingested.file_name or '-'}",
                    f"Кратко: {ingested.summary}",
                ]
            )
        )

    async def _handle_delete_command(self, event: events.NewMessage.Event, text: str) -> None:
        if event.is_reply:
            reply_message = await event.get_reply_message()
            if reply_message is None:
                await event.reply("Не удалось загрузить сообщение для удаления.")
                return
            deleted = self.rag_service.delete_item_by_source(int(reply_message.chat_id), int(reply_message.id))
            await event.reply("Удалено из локальной памяти." if deleted else "Для этого сообщения ничего не индексировалось.")
            return

        parts = text.split(maxsplit=1)
        if len(parts) < 2 or not parts[1].strip().isdigit():
            await event.reply("Использование: ответьте /delete или отправьте /delete ITEM_ID")
            return

        deleted = self.rag_service.delete_item_by_id(int(parts[1].strip()))
        await event.reply("Удалено из локальной памяти." if deleted else "Элемент не найден.")

    async def _handle_search(
        self,
        event: events.NewMessage.Event,
        query: str,
        remaining: int,
        api_key: str | None,
        unlimited_mode: bool,
    ) -> None:
        if not query:
            await event.reply("Использование: /search ваш запрос")
            return
        hits = self.rag_service.search(query, api_key=api_key)
        if not hits:
            await event.reply(self._append_remaining("Ничего подходящего не найдено.", remaining, unlimited=unlimited_mode))
            return

        lines = ["Результаты поиска:"]
        for hit in hits:
            lines.append(
                "\n".join(
                    [
                        f"#{hit.item_id} | {hit.content_date} | {hit.item_type} | {hit.file_name or '-'}",
                        hit.summary,
                    ]
                )
            )
        await self._reply_long(event, self._append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode))

    async def _handle_ask(
        self,
        event: events.NewMessage.Event,
        question: str,
        remaining: int,
        api_key: str | None,
        custom_prompt: str | None,
        unlimited_mode: bool,
        user_id: int,
        sender_profile: tuple[str | None, str | None, str | None],
    ) -> None:
        if not question:
            await event.reply("Использование: /ask ваш вопрос")
            return

        chat_id = int(event.chat_id)
        recent_messages = list(self.chat_histories[chat_id])
        hits = self.rag_service.retrieve_relevant_hits(
            question,
            recent_messages=recent_messages,
            api_key=api_key,
            limit=self.settings.max_context_chunks,
            unique_by_item=True,
        )
        if not hits:
            await event.reply(self._append_remaining("Подходящих материалов в памяти не найдено.", remaining, unlimited=unlimited_mode))
            return

        available_formats = self._available_delivery_formats(hits)
        if len(available_formats) > 1:
            self.pending_delivery_requests[chat_id] = PendingDeliveryRequest(
                question=question,
                hits=hits,
                recent_messages=recent_messages,
                api_key=api_key,
                custom_prompt=custom_prompt,
                remaining=remaining,
                unlimited_mode=unlimited_mode,
            )
            self._log_user_event(
                user_id=user_id,
                chat_id=chat_id,
                event_type="delivery_prompt",
                sender_profile=sender_profile,
                details={"formats": available_formats},
            )
            await self._reply_long(
                event,
                self._append_remaining(
                    self._delivery_prompt_text(available_formats),
                    remaining,
                    unlimited=unlimited_mode,
                ),
            )
            return

        answer = self.rag_service.answer_from_hits(
            question,
            hits,
            recent_messages=recent_messages,
            api_key=api_key,
            custom_prompt=custom_prompt,
        )
        self._append_chat_message(chat_id, "user", question)
        self._append_chat_message(chat_id, "assistant", answer)
        self._log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type="text_answer",
            sender_profile=sender_profile,
            details={"auto": True},
        )
        await self._reply_long(event, self._append_remaining(answer, remaining, unlimited=unlimited_mode))

    async def _handle_list(self, event: events.NewMessage.Event, raw_date: str, remaining: int, unlimited_mode: bool) -> None:
        try:
            content_date = parse_iso_date(raw_date.split()[0])
        except (DateParseError, IndexError) as exc:
            await event.reply(str(exc) if str(exc) else "Укажите дату в формате YYYY-MM-DD")
            return

        items = self.rag_service.list_by_date(content_date)
        if not items:
            await event.reply(self._append_remaining("Для этой даты ничего не найдено.", remaining, unlimited=unlimited_mode))
            return

        lines = [f"Материалы за {content_date}:"]
        for item in items:
            lines.append(
                "\n".join(
                    [
                        f"#{item['id']} | {item['item_type']} | {item['file_name'] or '-'}",
                        item["summary"],
                    ]
                )
            )
        await self._reply_long(event, self._append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode))

    async def _handle_file(self, event: events.NewMessage.Event, raw_item_id: str, remaining: int, unlimited_mode: bool) -> None:
        item_id = raw_item_id.strip()
        if not item_id.isdigit():
            await event.reply("Использование: /file ITEM_ID")
            return

        item = self.rag_service.get_item(int(item_id))
        if not item:
            await event.reply(self._append_remaining("Элемент не найден.", remaining, unlimited=unlimited_mode))
            return

        await self.client.forward_messages(
            event.chat_id,
            item["source_message_id"],
            from_peer=item["source_chat_id"],
        )
        await self.client.send_message(event.chat_id, self._remaining_line(remaining, unlimited=unlimited_mode))

    async def _handle_stats(self, event: events.NewMessage.Event, raw_arg: str) -> None:
        raw_arg = raw_arg.strip()
        if raw_arg and not raw_arg.isdigit():
            await event.reply("Использование: /stats или /stats USER_ID")
            return

        if raw_arg:
            rows = self.rag_service.get_user_statistics(today_iso(), limit=1, user_id=int(raw_arg))
            if not rows:
                await event.reply("Статистика по этому пользователю не найдена.")
                return
            await self._reply_long(event, self._format_detailed_user_stats(rows[0]))
            return

        rows = self.rag_service.get_user_statistics(today_iso(), limit=500)
        if not rows:
            await event.reply("Статистика пока пуста.")
            return

        lines = ["Статистика по пользователям:"]
        for row in rows:
            lines.append(self._format_user_stats_row(row))
        await self._reply_long(event, "\n".join(lines))


    async def _maybe_handle_pending_admin_add(
        self,
        event: events.NewMessage.Event,
        text: str,
        user_id: int,
        sender_profile: tuple[str | None, str | None, str | None],
    ) -> bool:
        chat_id = int(event.chat_id)
        pending = self.pending_admin_add_requests.get(chat_id)
        if pending is None:
            return False

        if pending.stage == "awaiting_content":
            has_content = bool(getattr(event.message, "media", None) or text)
            if not has_content:
                await event.reply("Пришлите файл, фото, видео, аудио, документ или текст для добавления.")
                return True
            pending.content_message = event.message
            pending.stage = "awaiting_date"
            await event.reply("Материал получен. Теперь отправьте дату в формате YYYY-MM-DD.")
            return True

        if pending.stage == "awaiting_date":
            if not text:
                await event.reply("Отправьте дату в формате YYYY-MM-DD.")
                return True
            try:
                pending.content_date = parse_iso_date(text.split()[0])
            except (DateParseError, IndexError) as exc:
                await event.reply(str(exc) if str(exc) else "Укажите дату в формате YYYY-MM-DD")
                return True
            pending.stage = "awaiting_description"
            await event.reply("Дата сохранена. Теперь отправьте описание материала.")
            return True

        if pending.stage == "awaiting_description":
            if not text:
                await event.reply("Отправьте текстовое описание материала.")
                return True
            pending.description = text
            await self._complete_admin_add(event, pending, user_id, sender_profile)
            return True

        self.pending_admin_add_requests.pop(chat_id, None)
        await event.reply("Состояние ручного добавления сброшено. Запустите /add заново.")
        return True

    async def _complete_admin_add(
        self,
        event: events.NewMessage.Event,
        pending: PendingAdminAddRequest,
        user_id: int,
        sender_profile: tuple[str | None, str | None, str | None],
    ) -> None:
        content_message = pending.content_message
        content_date = pending.content_date
        description = pending.description.strip()
        chat_id = int(event.chat_id)

        if content_message is None or not content_date or not description:
            self.pending_admin_add_requests.pop(chat_id, None)
            await event.reply("Не удалось завершить добавление. Запустите /add заново.")
            return

        work_dir = self.rag_service.media_service.create_work_dir()
        stored_message: Any | None = None
        try:
            stored_message = await self._send_manual_add_to_storage_group(
                content_message,
                content_date,
                description,
                work_dir,
            )
            ingested = await self.rag_service.ingest_message(
                client=self.client,
                message=stored_message,
                content_date=content_date,
                ingested_by_user_id=user_id,
            )
        except Exception as exc:
            if stored_message is not None:
                self.pending_admin_add_requests.pop(chat_id, None)
                LOGGER.exception("Manual add indexing failed after storage upload")
                await event.reply(
                    "Материал уже отправлен в группу-хранилище, но индексация завершилась ошибкой. "
                    "Его можно переиндексировать вручную через /date в группе.\n\n"
                    f"Текст ошибки: {exc}"
                )
                return
            raise
        finally:
            self.rag_service.media_service.cleanup_work_dir(work_dir)

        self.pending_admin_add_requests.pop(chat_id, None)
        self._log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type="manual_add",
            sender_profile=sender_profile,
            details={
                "item_id": ingested.item_id,
                "content_date": ingested.content_date,
                "storage_chat_id": int(getattr(stored_message, "chat_id", self.settings.storage_chat_id)),
                "storage_message_id": int(getattr(stored_message, "id", 0) or 0),
            },
        )
        await self._reply_long(
            event,
            "\n".join(
                [
                    "Материал добавлен в память и сохранен в группе-хранилище.",
                    f"Проиндексировано: #{ingested.item_id}",
                    f"Дата: {ingested.content_date}",
                    f"Тип: {ingested.item_type}",
                    f"Файл: {ingested.file_name or '-'}",
                    f"Кратко: {ingested.summary}",
                ]
            ),
        )

    async def _send_manual_add_to_storage_group(
        self,
        content_message: Any,
        content_date: str,
        description: str,
        work_dir: Any,
    ) -> Any:
        source_text = self.rag_service.media_service.get_caption(content_message)
        if getattr(content_message, "media", None):
            downloaded_path = await self.rag_service.media_service.download_message_media(
                self.client,
                content_message,
                work_dir,
            )
            if downloaded_path is None:
                raise RuntimeError("Не удалось скачать файл из личного сообщения.")

            sent_message = await self.client.send_file(
                self.settings.storage_chat_id,
                file=str(downloaded_path),
                caption=self._manual_add_caption_text(source_text, content_date, description),
                progress_callback=self.rag_service.media_service.build_transfer_progress_callback(
                    action="Загрузка",
                    message=content_message,
                    target_name=downloaded_path.name,
                ),
            )
            if isinstance(sent_message, list):
                return sent_message[0]
            return sent_message

        return await self.client.send_message(
            self.settings.storage_chat_id,
            self._manual_add_body_text(source_text, content_date, description),
        )
    async def _maybe_handle_pending_delivery_choice(
        self,
        event: events.NewMessage.Event,
        text: str,
        user_id: int,
        sender_profile: tuple[str | None, str | None, str | None],
    ) -> bool:
        chat_id = int(event.chat_id)
        pending = self.pending_delivery_requests.get(chat_id)
        if pending is None:
            return False

        normalized_choice = self._normalize_delivery_choice(text)
        available_formats = self._available_delivery_formats(pending.hits)
        if normalized_choice == "cancel":
            self.pending_delivery_requests.pop(chat_id, None)
            await event.reply("Выбор формата ответа отменен. Для нового вопроса используйте /ask.")
            return True

        if normalized_choice is None or normalized_choice not in available_formats:
            await event.reply(self._pending_delivery_reminder_text(available_formats))
            return True

        self.pending_delivery_requests.pop(chat_id, None)
        self._log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type="delivery_choice",
            sender_profile=sender_profile,
            details={"choice": normalized_choice},
        )

        if normalized_choice == "текст":
            answer = self.rag_service.answer_from_hits(
                pending.question,
                pending.hits,
                recent_messages=pending.recent_messages,
                api_key=pending.api_key,
                custom_prompt=pending.custom_prompt,
            )
            self._append_chat_message(chat_id, "user", pending.question)
            self._append_chat_message(chat_id, "assistant", answer)
            self._log_user_event(
                user_id=user_id,
                chat_id=chat_id,
                event_type="text_answer",
                sender_profile=sender_profile,
                details={"auto": False},
            )
            await self._reply_long(
                event,
                self._append_remaining(answer, pending.remaining, unlimited=pending.unlimited_mode),
            )
            return True

        media_hits = self._hits_for_delivery_choice(pending.hits, normalized_choice)
        if not media_hits:
            await event.reply(self._pending_delivery_reminder_text(available_formats))
            return True

        self._append_chat_message(chat_id, "user", pending.question)
        self._append_chat_message(chat_id, "assistant", f"Отправил материалы формата: {normalized_choice}.")
        self._log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type="media_delivery",
            sender_profile=sender_profile,
            details={"choice": normalized_choice, "count": len(media_hits)},
        )
        await event.reply(
            self._append_remaining(
                f"Отправляю материалы в формате: {normalized_choice}.",
                pending.remaining,
                unlimited=pending.unlimited_mode,
            )
        )
        await self._forward_hits(event.chat_id, media_hits)
        return True

    async def _forward_hits(self, target_chat_id: int, hits: list[SearchHit]) -> None:
        grouped_messages: dict[int, list[int]] = {}
        for hit in hits:
            grouped_messages.setdefault(hit.source_chat_id, []).append(hit.source_message_id)
        for source_chat_id, message_ids in grouped_messages.items():
            await self.client.forward_messages(target_chat_id, message_ids, from_peer=source_chat_id)

    async def _reply_long(self, event: events.NewMessage.Event, text: str) -> None:
        parts = split_for_telegram(text)
        if not parts:
            return
        for index, part in enumerate(parts):
            if index == 0:
                await event.reply(part)
            else:
                await self.client.send_message(event.chat_id, part)

    async def _maybe_send_first_welcome(
        self,
        event: events.NewMessage.Event,
        user_id: int,
        is_admin: bool,
    ) -> bool:
        if self.rag_service.has_sent_welcome(user_id):
            return False
        await self._reply_long(event, self._private_welcome_text(is_admin=is_admin))
        self.rag_service.mark_welcome_sent(user_id)
        return True

    def _append_chat_message(self, chat_id: int, role: str, content: str) -> None:
        clean_content = content.strip()
        if not clean_content:
            return
        self.chat_histories[chat_id].append(
            {
                "role": role,
                "content": clean_content,
            }
        )

    def _consume_daily_limit(self, user_id: int, has_personal_api: bool) -> tuple[bool, int, bool]:
        unlimited_mode = has_personal_api or (self.self_user_id is not None and user_id == self.self_user_id)
        if unlimited_mode:
            return True, self.settings.daily_message_limit, True

        allowed, _used, remaining = self.rag_service.database.consume_daily_user_message(
            user_id,
            today_iso(),
            self.settings.daily_message_limit,
        )
        return allowed, remaining, False

    def _log_user_event(
        self,
        *,
        user_id: int,
        chat_id: int,
        event_type: str,
        sender_profile: tuple[str | None, str | None, str | None],
        charged: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        username, first_name, last_name = sender_profile
        self.rag_service.log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type=event_type,
            event_date=today_iso(),
            charged=charged,
            username=username,
            first_name=first_name,
            last_name=last_name,
            details=details,
        )

    @staticmethod
    def _sender_profile(sender: Any) -> tuple[str | None, str | None, str | None]:
        if sender is None:
            return None, None, None
        return (
            getattr(sender, "username", None),
            getattr(sender, "first_name", None),
            getattr(sender, "last_name", None),
        )

    @staticmethod
    def _resolve_request(text: str) -> tuple[str, str]:
        if text.startswith("/search "):
            return "search", text[len("/search ") :].strip()
        if text.startswith("/ask "):
            return "ask", text[len("/ask ") :].strip()
        if text.startswith("/list "):
            return "list", text[len("/list ") :].strip()
        if text.startswith("/file "):
            return "file", text[len("/file ") :].strip()
        return "ask", text.strip()

    @staticmethod
    def _available_delivery_formats(hits: list[SearchHit]) -> list[str]:
        formats = []
        if any(hit.item_type == "video" for hit in hits):
            formats.append("видео")
        if any(hit.item_type == "image" for hit in hits):
            formats.append("фото")
        if any(hit.item_type == "audio" for hit in hits):
            formats.append("аудио")
        formats.append("текст")
        return formats

    @staticmethod
    def _normalize_delivery_choice(text: str) -> str | None:
        normalized = text.strip().lower().strip("\"' .,!?:;")
        aliases = {
            "видео": "видео",
            "video": "видео",
            "vid": "видео",
            "фото": "фото",
            "photo": "фото",
            "image": "фото",
            "картинка": "фото",
            "аудио": "аудио",
            "audio": "аудио",
            "voice": "аудио",
            "голос": "аудио",
            "текст": "текст",
            "text": "текст",
            "txt": "текст",
            "отмена": "cancel",
            "cancel": "cancel",
        }
        return aliases.get(normalized)

    @staticmethod
    def _hits_for_delivery_choice(hits: list[SearchHit], choice: str, limit: int = 3) -> list[SearchHit]:
        type_map = {
            "видео": "video",
            "фото": "image",
            "аудио": "audio",
        }
        desired_type = type_map.get(choice)
        if desired_type is None:
            return []
        unique_hits: list[SearchHit] = []
        seen_item_ids: set[int] = set()
        for hit in hits:
            if hit.item_type != desired_type or hit.item_id in seen_item_ids:
                continue
            seen_item_ids.add(hit.item_id)
            unique_hits.append(hit)
            if len(unique_hits) >= limit:
                break
        return unique_hits

    @staticmethod
    def _delivery_prompt_text(formats: list[str]) -> str:
        quoted_formats = " или ".join(f'"{item}"' for item in formats)
        listed_formats = ", ".join(formats)
        return (
            f"В памяти есть подходящие материалы в форматах: {listed_formats}. "
            f"Как отправить ответ? Напишите {quoted_formats}."
        )

    @staticmethod
    def _pending_delivery_reminder_text(formats: list[str]) -> str:
        quoted_formats = " или ".join(f'"{item}"' for item in formats)
        return (
            f"Сейчас ожидаю выбор формата ответа. Напишите {quoted_formats}. "
            'Если хотите отменить выбор, напишите "отмена". Для нового вопроса используйте /ask.'
        )

    @staticmethod
    def _display_name_from_stats(row: dict[str, Any]) -> str:
        name_parts = [part for part in [row.get("first_name"), row.get("last_name")] if part]
        return " ".join(name_parts) or "-"

    def _format_user_stats_row(self, row: dict[str, Any]) -> str:
        username = f"@{row['username']}" if row.get("username") else "-"
        api_text = "да" if row.get("has_api") else "нет"
        return (
            f"{row['user_id']} | {username} | {self._display_name_from_stats(row)} | "
            f"сегодня {row['total_today_count']}/{row['charged_today_count']} | "
            f"ask {row['ask_count']} | search {row['search_count']} | list {row['list_count']} | file {row['file_count']} | "
            f"add {row['manual_add_count']} | media {row['media_delivery_count']} | api {api_text} | last {row['last_seen_at']}"
        )

    def _format_detailed_user_stats(self, row: dict[str, Any]) -> str:
        username = f"@{row['username']}" if row.get("username") else "-"
        lines = [
            f"Пользователь: {row['user_id']}",
            f"Username: {username}",
            f"Имя: {self._display_name_from_stats(row)}",
            f"Первый контакт: {row['first_seen_at']}",
            f"Последняя активность: {row['last_seen_at']}",
            f"Всего событий: {row['total_event_count']}",
            f"Сегодня событий: {row['total_today_count']}",
            f"Сегодня списано по лимиту: {row['charged_today_count']}",
            f"Всего списано по лимиту: {row['charged_total_count']}",
            f"Вопросы: {row['ask_count']}",
            f"Поиски: {row['search_count']}",
            f"Списки по дате: {row['list_count']}",
            f"Запросы файлов: {row['file_count']}",
            f"Ручные добавления: {row['manual_add_count']}",
            f"Команды настроек: {row['settings_count']}",
            f"Неизвестные команды: {row['unknown_command_count']}",
            f"Запросов выбора формата: {row['delivery_prompt_count']}",
            f"Выборов формата: {row['delivery_choice_count']}",
            f"Текстовых ответов: {row['text_answer_count']}",
            f"Медиа-отправок: {row['media_delivery_count']}",
            f"Личный API token: {'да' if row.get('has_api') else 'нет'}",
            f"Пользовательский prompt: {'да' if row.get('has_prompt') else 'нет'}",
        ]
        return "\n".join(lines)


    @staticmethod
    def _manual_add_caption_text(source_text: str, content_date: str, description: str) -> str:
        clean_description = description.strip()
        clean_source = source_text.strip()
        parts = [
            f"Дата: {content_date}",
            f"Описание: {clean_description}",
        ]
        if clean_source and clean_source != clean_description:
            parts.append(f"Исходная подпись: {trim_text(clean_source, 450)}")
        return trim_text("\n".join(parts), 900)

    @staticmethod
    def _manual_add_body_text(source_text: str, content_date: str, description: str) -> str:
        clean_description = description.strip()
        clean_source = source_text.strip()
        parts = [
            f"Дата: {content_date}",
            f"Описание: {clean_description}",
        ]
        if clean_source and clean_source != clean_description:
            parts.extend(["", trim_text(clean_source, 3000)])
        return trim_text("\n".join(parts), 3500)

    @staticmethod
    def _append_remaining(text: str, remaining: int, unlimited: bool = False) -> str:
        return f"{text}\n\n{TelegramRagMemoryBot._remaining_line(remaining, unlimited=unlimited)}"

    @staticmethod
    def _remaining_line(remaining: int, unlimited: bool = False) -> str:
        if unlimited:
            return "Режим: безлимит."
        return f"Осталось сообщений на сегодня: {remaining}."

    @staticmethod
    def _get_active_api_key(user_prefs: dict[str, object]) -> str | None:
        api_key = (user_prefs.get("api_key") or "") if user_prefs else ""
        return str(api_key).strip() or None

    def _get_active_prompt(self, user_prefs: dict[str, object]) -> str | None:
        if not self._get_active_api_key(user_prefs):
            return None
        prompt = (user_prefs.get("custom_prompt") or "") if user_prefs else ""
        return str(prompt).strip() or None

    def _is_authorized(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        if self.self_user_id is not None and user_id == self.self_user_id:
            return True
        if self.settings.public_access:
            return True
        allowed = self.settings.authorized_user_ids
        if not allowed:
            return False
        return user_id in allowed

    def _is_uploader(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        if self.self_user_id is not None and user_id == self.self_user_id:
            return True
        return user_id in self.settings.uploader_user_ids

    def _private_welcome_text(self, is_admin: bool = False) -> str:
        intro_lines = [
            "Привет! Это одноразовое приветствие после вашего первого сообщения.",
        ]
        if is_admin:
            intro_lines.append(
                "У вас расширенный доступ: помимо работы с памятью в личке, доступны команды администратора в группе-хранилище и статистика по пользователям."
            )
        else:
            intro_lines.append(
                "Я помогаю искать материалы в памяти по тексту, фото, аудио и видео с учетом даты."
            )
        intro_lines.append("")
        intro_lines.append(self._private_help_text(is_admin=is_admin))
        return "\n".join(intro_lines)

    def _private_help_text(self, is_admin: bool = False) -> str:
        return "\n".join(
            [
                "Команды:",
                *self._private_command_lines(is_admin=is_admin),
                "",
                f"Без личного API token лимит: {self.settings.daily_message_limit} сообщений в день.",
                "С личным рабочим API token: безлимит.",
                "Пользовательский prompt работает только при активном личном API token.",
                "Если удалить API token, prompt сохранится, но перестанет применяться, пока токен не будет добавлен снова.",
                f"Бот учитывает контекст последних {self.settings.conversation_context_messages} сообщений в личке.",
                "Если по запросу найдены и текст, и медиа, бот спросит, в каком формате отправить ответ.",
                "Выбор формата ответа не расходует дополнительный лимит за день.",
                "Сообщения из выбранных групп индексируются автоматически.",
                "Если в тексте есть дата, она используется; иначе ставится сегодняшняя дата.",
                "Видео из выбранных групп скачиваются автоматически локально.",
                "Администратор может вручную добавить материал через /add и отменить сценарий через /cancel_add.",
                "Любое обычное сообщение в личке считается как /ask.",
            ]
        )

    @staticmethod
    def _private_command_lines(is_admin: bool = False) -> list[str]:
        lines = [
            "/ask <вопрос> - задать вопрос по памяти",
            "/search <запрос> - найти подходящие материалы",
            "/list <YYYY-MM-DD> - список материалов за дату",
            "/file <ITEM_ID> - переслать оригинал из группы",
            "/set_api <token> - подключить свой OpenAI API token",
            "/delete_api - удалить свой API token",
            "/set_prompt <текст> - сохранить свой prompt для ответов",
            "/delete_prompt - удалить свой prompt",
            "/my_settings - показать статус личного token и prompt",
            "/help или /start - показать справку",
        ]
        if is_admin:
            lines.extend(
                [
                    "/add - вручную добавить файл или текст через личку",
                    "/cancel_add - отменить текущее ручное добавление",
                    "/stats [USER_ID] - статистика по пользователям",
                    "",
                    "Команды администратора в группе-хранилище:",
                    "/date <YYYY-MM-DD> - ответом на сообщение переиндексировать материал",
                    "/delete - ответом на сообщение удалить материал из локальной памяти",
                    "/delete <ITEM_ID> - удалить материал по номеру из локальной памяти",
                ]
            )
        return lines

    @staticmethod
    def _private_command_names() -> set[str]:
        return {
            "/help",
            "/start",
            "/ask",
            "/search",
            "/list",
            "/file",
            "/set_api",
            "/delete_api",
            "/set_prompt",
            "/delete_prompt",
            "/my_settings",
            "/stats",
            "/add",
            "/cancel_add",
        }

    @staticmethod
    def _unknown_command_text() -> str:
        return 'Такой команды нет. Если надо добавить, напишите: "пж добавьте админы".'

    @staticmethod
    def _storage_help_text() -> str:
        return "\n".join(
            [
                "Команды в группе-хранилище:",
                "Обычные сообщения индексируются автоматически по дате из текста или по сегодняшней дате.",
                "Ответьте на сообщение командой /date YYYY-MM-DD, чтобы вручную переиндексировать его.",
                "Ответьте командой /delete, чтобы удалить его из локальной памяти.",
            ]
        )




















"""
Этот файл хранит старый сценарий userbot-рантайма
и оставлен как совместимый или исторический слой проекта.
"""
