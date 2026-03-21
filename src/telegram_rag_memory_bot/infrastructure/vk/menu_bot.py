"""
????: infrastructure/vk/menu_bot.py
????????? ???????? VK-??? ?? vk_api: ??????? long poll,
???????????? ?????????, ??????????, ?????? ? ????????.
"""

from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
from pathlib import Path
import random
from typing import Any
from urllib.parse import urlencode

import vk_api
from vk_api.bot_longpoll import VkBotEventType, VkBotLongPoll
from vk_api.exceptions import ApiError, VkApiError as VkLibraryError
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
from vk_api.upload import VkUpload

from telegram_rag_memory_bot.application.vk.service import VkAssistantApplicationService
from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.domain.models import ChatSession, DeliveryChoice, ManagedAnswerChoice, ManagedAnswerOption, PendingInput, SenderProfile
from telegram_rag_memory_bot.domain.ports import NotificationGateway
from telegram_rag_memory_bot.schemas import SearchHit
from telegram_rag_memory_bot.utils.text import split_for_telegram

LOGGER = logging.getLogger(__name__)


class VkApiError(RuntimeError):
    pass


class VkMenuBot(NotificationGateway):
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

    def __init__(
        self,
        settings: Settings,
        app_service: VkAssistantApplicationService,
        *,
        telegram_client: object,
    ) -> None:
        self.settings = settings
        self.app_service = app_service
        self.telegram_client = telegram_client
        self.sessions: dict[int, ChatSession] = {}
        self._group_id = settings.vk_group_id or 0
        self._sender_cache: dict[int, SenderProfile] = {}
        self._vk_session: vk_api.VkApi | None = None
        self._vk: Any | None = None
        self._vk_upload: VkUpload | None = None
        self._longpoll: VkBotLongPoll | None = None
        self._longpoll_iterator: Any | None = None

    async def run(self) -> None:
        if not self.settings.vk_enabled:
            LOGGER.info("VK bot is not started because API_VK is empty.")
            return
        while True:
            try:
                await self._ensure_vk_runtime()
                LOGGER.info("VK bot connected for group_id=%s", self._group_id)
                await self._poll_loop()
            except asyncio.CancelledError:
                raise
            except VkApiError as exc:
                LOGGER.error("VK bot temporarily unavailable: %s", self._friendly_vk_error(exc))
                self._reset_vk_runtime()
                await asyncio.sleep(60)
            except Exception:
                LOGGER.exception("VK bot failed unexpectedly. Retrying in 15 seconds.")
                self._reset_vk_runtime()
                await asyncio.sleep(15)

    async def notify_user(self, *, user_id: int, text: str) -> None:
        await self._send_text(user_id, text, buttons=self._main_keyboard(is_admin=self.app_service.is_admin(user_id)))

    def _session(self, peer_id: int) -> ChatSession:
        if peer_id not in self.sessions:
            self.sessions[peer_id] = ChatSession(recent_messages=deque(maxlen=self.settings.conversation_context_messages))
        return self.sessions[peer_id]

    async def _ensure_vk_runtime(self) -> None:
        if self._vk_session is not None and self._vk is not None and self._vk_upload is not None and self._longpoll is not None:
            return
        session, api, upload, longpoll, group_id = await asyncio.to_thread(self._build_vk_runtime_sync)
        self._vk_session = session
        self._vk = api
        self._vk_upload = upload
        self._longpoll = longpoll
        self._longpoll_iterator = None
        self._group_id = group_id

    def _build_vk_runtime_sync(self) -> tuple[vk_api.VkApi, Any, VkUpload, VkBotLongPoll, int]:
        session = vk_api.VkApi(token=self.settings.vk_api_token, api_version=self.settings.vk_api_version)
        api = session.get_api()
        group_id = self._group_id or self._discover_group_id_sync(api)
        longpoll = VkBotLongPoll(session, group_id, wait=25)
        upload = VkUpload(session)
        return session, api, upload, longpoll, int(group_id)

    def _discover_group_id_sync(self, api: Any) -> int:
        response = api.groups.getById()
        if not response:
            raise VkApiError("VK groups.getById returned empty response.")
        group = response[0] if isinstance(response, list) else response.get("groups", [{}])[0]
        group_id = int(group.get("id") or 0)
        if group_id <= 0:
            raise VkApiError("Could not determine VK group id from API_VK.")
        return group_id

    def _reset_longpoll(self) -> None:
        self._longpoll = None
        self._longpoll_iterator = None

    def _reset_vk_runtime(self) -> None:
        self._vk_session = None
        self._vk = None
        self._vk_upload = None
        self._reset_longpoll()

    def _next_longpoll_event(self) -> Any:
        if self._longpoll is None:
            raise VkApiError("VK long poll is not initialized.")
        if self._longpoll_iterator is None:
            self._longpoll_iterator = iter(self._longpoll.listen())
        return next(self._longpoll_iterator)

    async def _poll_loop(self) -> None:
        while True:
            try:
                event = await asyncio.to_thread(self._next_longpoll_event)
            except asyncio.CancelledError:
                raise
            except (VkLibraryError, ApiError) as exc:
                LOGGER.warning("VK long poll library issue: %s. Reconnecting long poll.", exc)
                self._reset_longpoll()
                await asyncio.sleep(2)
                continue
            except Exception:
                LOGGER.warning("VK long poll request failed. Reconnecting long poll.", exc_info=True)
                self._reset_longpoll()
                await asyncio.sleep(3)
                continue

            if event.type != VkBotEventType.MESSAGE_NEW:
                continue

            try:
                update_object = dict(getattr(event, "object", {}) or {})
                message_payload = update_object.get("message", update_object)
                await self._handle_message(message_payload)
            except Exception:
                LOGGER.exception("VK message handler failed")

    async def _handle_message(self, message: dict[str, Any]) -> None:
        if int(message.get("out") or 0) == 1:
            return
        sender_id = int(message.get("from_id") or 0)
        peer_id = int(message.get("peer_id") or sender_id)
        if sender_id <= 0 or peer_id <= 0:
            LOGGER.info("VK message skipped: sender_id=%s peer_id=%s payload_keys=%s", sender_id, peer_id, sorted(message.keys()))
            return
        text = str(message.get("text") or "").strip()
        if not text:
            text = self._payload_command_text(message.get("payload"))
        LOGGER.info(
            "VK incoming message | sender=%s | peer=%s | text=%s | payload=%s",
            sender_id,
            peer_id,
            (text[:120] or "<empty>"),
            self._payload_preview(message.get("payload")),
        )
        is_banned, ban_reason = self.app_service.is_banned(sender_id)
        if is_banned:
            reason_suffix = f"\nПричина: {ban_reason}" if ban_reason else ""
            await self._send_text(peer_id, f"Доступ к боту ограничен администратором.{reason_suffix}")
            return
        if not self.app_service.is_authorized(sender_id):
            await self._send_text(peer_id, "Доступ запрещен.")
            return

        sender_profile = await self._get_sender_profile(sender_id)
        is_admin = self.app_service.is_admin(sender_id)
        session = self._session(peer_id)
        command = text.split(maxsplit=1)[0].lower() if text.startswith("/") else ""
        main_buttons = self._main_keyboard(is_admin=is_admin, user_id=sender_id)
        settings_buttons = self._settings_keyboard(is_admin=is_admin, user_id=sender_id)

        first_welcome = await self._maybe_send_welcome(peer_id, sender_id, is_admin)
        if await self._handle_department_survey(peer_id, sender_id, sender_profile, is_admin, text):
            return

        if text in {"/start", "/help", "/menu", self.BUTTON_HELP} or text.startswith("/start ") or text.startswith("/help "):
            if first_welcome and command in {"/start", "/help"}:
                return
            await self._send_text(peer_id, self._help_text(is_admin=is_admin, user_id=sender_id), buttons=main_buttons)
            return

        if command.startswith("/"):
            custom_command = self.app_service.get_custom_command(command)
            if custom_command is not None:
                await self._handle_custom_command(peer_id, sender_id, sender_profile, is_admin, custom_command)
                return

        if command == "/homosap":
            await self._handle_homosap(peer_id, sender_id, sender_profile, is_admin)
            return

        if text == self.BUTTON_BACK:
            session.pending_input = None
            session.pending_delivery = None
            session.pending_managed_choice = None
            session.state.clear()
            await self._send_text(peer_id, "Главное меню.", buttons=main_buttons)
            return

        if session.pending_managed_choice is not None and text and not command.startswith("/"):
            if await self._handle_managed_answer_choice(peer_id, session, sender_id, sender_profile, is_admin, text):
                return

        if session.pending_delivery is not None and text and not command.startswith("/"):
            if await self._handle_delivery_choice(peer_id, session, sender_id, sender_profile, is_admin, text):
                return

        if text == self.BUTTON_SETTINGS:
            await self._send_text(peer_id, "Раздел настроек.", buttons=settings_buttons)
            return
        if text == self.BUTTON_ASK or text == "/ask":
            session.pending_input = PendingInput("ask", "Напишите вопрос по памяти.")
            session.pending_input = PendingInput("file", "Отправьте ITEM_ID, чтобы получить текстовое описание материала.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_SEARCH or text == "/search":
            session.pending_input = PendingInput("search", "Напишите запрос для поиска по памяти.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_LIST or text == "/list":
            session.pending_input = PendingInput("list", "Отправьте дату в формате DD-MM-YYYY.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_FILE or text == "/file":
            session.pending_input = PendingInput("file", "Отправьте ITEM_ID для отправки оригинала.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_SET_API or text == "/set_api":
            session.pending_input = PendingInput("set_api", "Отправьте ваш OpenAI API token.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_SET_PROMPT or text == "/set_prompt":
            session.pending_input = PendingInput("set_prompt", "Отправьте ваш пользовательский prompt.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text in {self.BUTTON_PROMT, "/promt"}:
            session.pending_input = PendingInput("prompt_profile", "Выберите профиль prompt кнопкой ниже.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._prompt_profile_keyboard())
            return
        if text == "/promo":
            session.pending_input = PendingInput("promo", "Отправьте промокод для активации дополнительных запросов.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_REQUEST_ACCESS or text == "/request_access":
            request_type = session.state.get("request_type", "daily_limit")
            mode_bucket = session.state.get("mode_bucket", "")
            session.pending_input = PendingInput("request_name", "Напишите имя для заявки.")
            session.state = {"request_type": request_type, "mode_bucket": mode_bucket}
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text == self.BUTTON_CANCEL:
            session.pending_input = None
            session.pending_delivery = None
            session.pending_managed_choice = None
            session.state.clear()
            await self._send_text(peer_id, "Действие отменено.", buttons=main_buttons)
            return
        if text in {self.BUTTON_MY_SETTINGS, "/my_settings"}:
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/my_settings"})
            await self._send_text(peer_id, self.app_service.build_user_settings_text(sender_id), buttons=settings_buttons)
            return
        if text in {self.BUTTON_DELETE_API, "/delete_api"}:
            await self._handle_delete_api(peer_id, sender_id, sender_profile, is_admin)
            return
        if text in {self.BUTTON_DELETE_PROMPT, "/delete_prompt"}:
            await self._handle_delete_prompt(peer_id, sender_id, sender_profile, is_admin)
            return
        if text in {self.BUTTON_STATS, "/stats"}:
            if not is_admin:
                self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="unknown_command", sender_profile=sender_profile, details={"command": "/stats"})
                await self._send_text(peer_id, "Команда /stats доступна только администратору.", buttons=main_buttons)
                return
            await self._handle_stats(peer_id, sender_id, sender_profile, is_admin, raw_arg="")
            return
        if text in {self.BUTTON_LOCAL_UPLOAD, "/upload_local"}:
            if not is_admin:
                await self._send_text(peer_id, "Эта функция доступна только администратору.", buttons=main_buttons)
                return
            await self._send_text(peer_id, self._local_upload_text(sender_id), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return

        department_action = self.app_service.resolve_department_action_by_label(text, self.app_service.get_user_department(sender_id))
        if department_action is not None:
            if self.app_service.get_user_department(sender_id) == "проект 11" and text == self.app_service.department_button_label(sender_id):
                session.pending_input = PendingInput("department_pick", self.app_service.department_action_picker_prompt())
                await self._send_text(peer_id, session.pending_input.prompt, buttons=self._department_action_keyboard(include_all=True))
                return
            await self._handle_department_action(peer_id, session, sender_id, sender_profile, is_admin, text, department_action["department"])
            return

        if session.pending_input is not None and not command.startswith("/"):
            await self._handle_pending_input(peer_id, session, sender_id, sender_profile, is_admin, text)
            return

        if command == "/stats":
            if not is_admin:
                self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="unknown_command", sender_profile=sender_profile, details={"command": command})
                await self._send_text(peer_id, "Команда /stats доступна только администратору.", buttons=main_buttons)
                return
            await self._handle_stats(peer_id, sender_id, sender_profile, is_admin, raw_arg=text[len("/stats") :].strip())
            return
        if command == "/delete_api":
            await self._handle_delete_api(peer_id, sender_id, sender_profile, is_admin)
            return
        if command == "/delete_prompt":
            await self._handle_delete_prompt(peer_id, sender_id, sender_profile, is_admin)
            return
        if command == "/my_settings":
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/my_settings"})
            await self._send_text(peer_id, self.app_service.build_user_settings_text(sender_id), buttons=settings_buttons)
            return
        if text.startswith("/set_api "):
            await self._handle_set_api(peer_id, sender_id, sender_profile, is_admin, text[len("/set_api ") :].strip())
            return
        if text.startswith("/set_prompt "):
            await self._handle_set_prompt(peer_id, sender_id, sender_profile, is_admin, text[len("/set_prompt ") :].strip())
            return
        if text.startswith("/promo "):
            await self._handle_promo(peer_id, sender_id, sender_profile, is_admin, text[len("/promo ") :].strip())
            return
        if text.startswith("/promt "):
            await self._handle_prompt_profile(peer_id, sender_id, sender_profile, is_admin, text[len("/promt ") :].strip())
            return
        if text.startswith("/request_access "):
            request_type = session.state.get("request_type", "daily_limit")
            mode_bucket = session.state.get("mode_bucket", "")
            session.pending_input = PendingInput("request_reason", "Напишите причину заявки.")
            session.state = {"request_type": request_type, "mode_bucket": mode_bucket, "request_name": text[len("/request_access ") :].strip()}
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if text.startswith("/ask "):
            await self._handle_ask(peer_id, session, sender_id, sender_profile, is_admin, text[len("/ask ") :].strip())
            return
        if text.startswith("/search "):
            await self._handle_search(peer_id, sender_id, sender_profile, is_admin, text[len("/search ") :].strip())
            return
        if text.startswith("/list "):
            await self._handle_list(peer_id, sender_id, sender_profile, is_admin, text[len("/list ") :].strip())
            return
        if text.startswith("/file "):
            await self._handle_file(peer_id, sender_id, sender_profile, is_admin, text[len("/file ") :].strip())
            return
        if command.startswith("/"):
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="unknown_command", sender_profile=sender_profile, details={"command": command})
            await self._send_text(peer_id, self._unknown_command_text(), buttons=main_buttons)
            return

        await self._handle_ask(peer_id, session, sender_id, sender_profile, is_admin, text)
    async def _handle_pending_input(self, peer_id: int, session: ChatSession, sender_id: int, sender_profile: SenderProfile, is_admin: bool, text: str) -> None:
        action = session.pending_input.action
        session.pending_input = None
        if action == "ask":
            await self._handle_ask(peer_id, session, sender_id, sender_profile, is_admin, text)
            return
        if action == "search":
            await self._handle_search(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "list":
            await self._handle_list(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "file":
            await self._handle_file(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "set_api":
            await self._handle_set_api(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "set_prompt":
            await self._handle_set_prompt(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "promo":
            await self._handle_promo(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "prompt_profile":
            await self._handle_prompt_profile(peer_id, sender_id, sender_profile, is_admin, text)
            return
        if action == "request_name":
            session.state["request_name"] = text.strip()
            session.pending_input = PendingInput("request_reason", "Напишите причину заявки.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        if action == "request_reason":
            await self._handle_access_request(peer_id, session, sender_id, sender_profile, is_admin, text)
            return
        if action == "department_pick":
            selected = self.app_service.resolve_department_action_by_label(text, "проект 11")
            if selected is None:
                await self._send_text(peer_id, "Выберите один из департаментных режимов кнопкой ниже.", buttons=self._department_action_keyboard(include_all=True))
                return
            await self._handle_department_action(peer_id, session, sender_id, sender_profile, is_admin, selected["button"], selected["department"])
            return
        await self._send_text(peer_id, "Неизвестное действие.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_search(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, query: str) -> None:
        if not query:
            await self._send_text(peer_id, "Напишите запрос для поиска.", buttons=self._cancel_keyboard())
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "search", "query": query[:300]})
            await self._send_limit_request_offer(peer_id, session=self._session(peer_id), sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="search", sender_profile=sender_profile, charged=not unlimited_mode, details={"query": query[:500]})
        hits = self.app_service.search(query, api_key=personal_api_key)
        if not hits:
            await self._send_text(peer_id, self.app_service.append_remaining("Ничего подходящего не найдено.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        lines = ["Результаты поиска:"]
        for hit in hits:
            date_text = self.app_service.display_content_with_shift(hit.content_date, getattr(hit, "content_scope", "dated"))
            lines.append(f"#{hit.item_id} | {date_text} | {hit.item_type} | {hit.file_name or '-'}\n{hit.summary}")
        await self._send_text(peer_id, self.app_service.append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_list(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, raw_date: str) -> None:
        if not raw_date:
            await self._send_text(peer_id, "Отправьте дату в формате DD-MM-YYYY.", buttons=self._cancel_keyboard())
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "list", "query": raw_date[:100]})
            await self._send_limit_request_offer(peer_id, session=self._session(peer_id), sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        try:
            content_date, items = self.app_service.list_by_date(raw_date)
        except Exception as exc:
            await self._send_text(peer_id, str(exc) or "Укажите дату в формате DD-MM-YYYY", buttons=self._cancel_keyboard())
            return
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="list", sender_profile=sender_profile, charged=not unlimited_mode, details={"query": raw_date[:100]})
        if not items:
            await self._send_text(peer_id, self.app_service.append_remaining("Для этой даты ничего не найдено.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        lines = [f"Материалы за {content_date}:"]
        for item in items:
            shift_name = str(item.get("shift_name") or "").strip()
            shift_suffix = f" | смена: {shift_name}" if shift_name else ""
            lines.append(f"#{item['id']} | {item['item_type']} | {item['file_name'] or '-'}{shift_suffix}\n{item['summary']}")
        await self._send_text(peer_id, self.app_service.append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_file(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, raw_item_id: str) -> None:
        item_id = raw_item_id.strip()
        if not item_id or not item_id.isdigit():
            await self._send_text(peer_id, "ITEM_ID должен быть числом.", buttons=self._cancel_keyboard())
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        allowed, remaining, unlimited_mode = self.app_service.consume_daily_limit(sender_id, has_personal_api=bool(personal_api_key), is_admin=is_admin)
        if not allowed:
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "file", "query": item_id})
            await self._send_limit_request_offer(peer_id, session=self._session(peer_id), sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="file", sender_profile=sender_profile, charged=not unlimited_mode, details={"query": item_id})
        item = self.app_service.get_item(int(item_id))
        if not item:
            await self._send_text(peer_id, self.app_service.append_remaining("Элемент не найден.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        description = self.app_service.describe_item_for_text_only(item)
        await self._send_text(
            peer_id,
            self.app_service.append_remaining(description, remaining, unlimited=unlimited_mode),
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
        )
        return
        if str(item.get("item_type") or "").strip().lower() == "video":
            await self._send_text(
                peer_id,
                self.app_service.append_remaining(
                    "Видео не отправляется. Бот хранит только извлеченную из видео информацию и отвечает по анализу.",
                    remaining,
                    unlimited=unlimited_mode,
                ),
                buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
            )
            return
        ok = await self._send_storage_item(peer_id, item)
        if not ok:
            await self._send_text(peer_id, "Не удалось отправить оригинал из хранилища.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        await self._send_text(peer_id, self.app_service.remaining_line(remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_ask(self, peer_id: int, session: ChatSession, sender_id: int, sender_profile: SenderProfile, is_admin: bool, question: str) -> None:
        if not question:
            await self._send_text(peer_id, "Напишите вопрос.", buttons=self._cancel_keyboard())
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
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="limit_block", sender_profile=sender_profile, details={"request_kind": "ask", "question": question[:300]})
            await self._send_limit_request_offer(peer_id, session=session, sender_id=sender_id, is_admin=is_admin, department_mode=False)
            return
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="ask", sender_profile=sender_profile, charged=not unlimited_mode, details={"question": question[:500]})
        managed_options = self.app_service.find_managed_answer_options(question)
        if managed_options:
            if len(managed_options) == 1:
                await self._send_managed_answer_option(peer_id, session, sender_id, sender_profile, is_admin, question, managed_options[0], remaining, unlimited_mode)
                return
            session.pending_managed_choice = ManagedAnswerChoice(question=question, options=managed_options, remaining=remaining, unlimited_mode=unlimited_mode)
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="managed_answer_prompt", sender_profile=sender_profile, details={"count": len(managed_options), "trigger": question[:200]})
            await self._send_text(peer_id, self.app_service.append_remaining(f"Нашел несколько готовых вариантов ответа. Выберите вариант кнопкой ниже или напишите номер от 1 до {len(managed_options)}.", remaining, unlimited=unlimited_mode), buttons=self._managed_answer_keyboard(managed_options))
            return
        hits = self.app_service.retrieve_answer_hits(question, recent_messages=list(session.recent_messages), api_key=personal_api_key)
        if not hits:
            await self._send_text(peer_id, self.app_service.append_remaining("Подходящих материалов в памяти не найдено.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
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
            chat_id=peer_id,
            event_type="text_answer",
            sender_profile=sender_profile,
            details={"auto": True, "forced_text_only": True},
        )
        await self._send_text(
            peer_id,
            self.app_service.append_remaining(answer, remaining, unlimited=unlimited_mode),
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
        )
        return
        formats = self.app_service.available_delivery_formats(hits)
        if len(formats) > 1:
            session.pending_delivery = DeliveryChoice(question=question, hits=hits, recent_messages=list(session.recent_messages), api_key=personal_api_key, custom_prompt=effective_prompt, remaining=remaining, unlimited_mode=unlimited_mode)
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="delivery_prompt", sender_profile=sender_profile, details={"formats": formats})
            quoted_formats = " или ".join(f'"{item}"' for item in formats)
            listed_formats = ", ".join(formats)
            await self._send_text(peer_id, self.app_service.append_remaining(f"В памяти есть подходящие материалы в форматах: {listed_formats}. Как отправить ответ? Напишите {quoted_formats}.", remaining, unlimited=unlimited_mode), buttons=self._delivery_keyboard(formats))
            return
        answer = self.app_service.answer_from_hits(question=question, hits=hits, recent_messages=list(session.recent_messages), api_key=personal_api_key, custom_prompt=effective_prompt)
        self._append_history(session, "user", question)
        self._append_history(session, "assistant", answer)
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="text_answer", sender_profile=sender_profile, details={"auto": True})
        await self._send_text(peer_id, self.app_service.append_remaining(answer, remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_department_action(
        self,
        peer_id: int,
        session: ChatSession,
        sender_id: int,
        sender_profile: SenderProfile,
        is_admin: bool,
        question: str,
        action_department: str,
    ) -> None:
        prefs = self.app_service.get_user_preferences(sender_id)
        personal_api_key = self.app_service.get_active_api_key(prefs)
        custom_prompt = self.app_service.get_active_prompt(prefs)
        prompt_profile = self.app_service.get_prompt_profile(prefs)
        allowed, remaining, used_bonus, mode_bucket = self.app_service.consume_department_action_limit(sender_id, action_department)
        if not allowed:
            session.state = {"request_type": "department_mode", "mode_bucket": mode_bucket}
            self.app_service.log_event(
                user_id=sender_id,
                chat_id=peer_id,
                event_type="department_limit_block",
                sender_profile=sender_profile,
                details={"department": action_department, "question": question[:300], "mode_bucket": mode_bucket},
            )
            await self._send_limit_request_offer(peer_id, session=session, sender_id=sender_id, is_admin=is_admin, department_mode=True)
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
        self.app_service.log_event(
            user_id=sender_id,
            chat_id=peer_id,
            event_type="department_action",
            sender_profile=sender_profile,
            charged=used_bonus,
            details={"department": action_department, "question": question[:500], "hits": len(hits), "date_from": date_from},
        )
        await self._send_text(peer_id, self.app_service.append_remaining(answer, remaining, unlimited=False), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_prompt_profile(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, raw_value: str) -> None:
        profile = self.app_service.normalize_prompt_profile(raw_value)
        if profile is None:
            await self._send_text(peer_id, "Выберите один из профилей prompt кнопкой ниже.", buttons=self._prompt_profile_keyboard())
            return
        self.app_service.save_user_prompt_profile(sender_id, profile)
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/promt", "profile": profile})
        await self._send_text(
            peer_id,
            f"Профиль prompt сохранен: {self.app_service.PROMPT_PROFILE_LABELS.get(profile, profile)}.",
            buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id),
        )

    async def _handle_access_request(self, peer_id: int, session: ChatSession, sender_id: int, sender_profile: SenderProfile, is_admin: bool, reason: str) -> None:
        request_name = session.state.get("request_name", "").strip()
        if not request_name:
            session.pending_input = PendingInput("request_name", "Напишите имя для заявки.")
            await self._send_text(peer_id, session.pending_input.prompt, buttons=self._cancel_keyboard())
            return
        request_type = session.state.get("request_type", "daily_limit")
        mode_bucket = session.state.get("mode_bucket", "").strip() or None
        request_id = self.app_service.create_access_request(
            user_id=sender_id,
            request_name=request_name,
            reason=reason,
            request_type=request_type,
            mode_bucket=mode_bucket,
        )
        session.state.clear()
        self.app_service.log_event(
            user_id=sender_id,
            chat_id=peer_id,
            event_type="access_request",
            sender_profile=sender_profile,
            details={"request_id": request_id, "reason": reason[:500], "request_type": request_type, "mode_bucket": mode_bucket or ""},
        )
        await self._send_text(peer_id, f"Заявка #{request_id} отправлена администратору.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _send_limit_request_offer(self, peer_id: int, session: ChatSession, sender_id: int, is_admin: bool, *, department_mode: bool) -> None:
        session.pending_input = None
        await self._send_text(
            peer_id,
            f"{self.app_service.build_limit_request_prompt(department_mode=department_mode)}\nНажмите кнопку \"{self.BUTTON_REQUEST_ACCESS}\" или используйте /request_access.",
            buttons=[[self.BUTTON_REQUEST_ACCESS], *self._main_keyboard(is_admin=is_admin, user_id=sender_id)],
        )

    async def _handle_delivery_choice(self, peer_id: int, session: ChatSession, sender_id: int, sender_profile: SenderProfile, is_admin: bool, text: str) -> bool:
        pending = session.pending_delivery
        if pending is None:
            return False
        choice = self.app_service.normalize_delivery_choice(text)
        if choice == "cancel":
            session.pending_delivery = None
            await self._send_text(peer_id, "Выбор формата ответа отменен.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return True
        if choice == "cancel":
            session.pending_delivery = None
            await self._send_text(peer_id, "Р’С‹Р±РѕСЂ С„РѕСЂРјР°С‚Р° РѕС‚РІРµС‚Р° РѕС‚РјРµРЅРµРЅ.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
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
            chat_id=peer_id,
            event_type="delivery_choice",
            sender_profile=sender_profile,
            details={"choice": "текст", "forced_text_only": True},
        )
        self.app_service.log_event(
            user_id=sender_id,
            chat_id=peer_id,
            event_type="text_answer",
            sender_profile=sender_profile,
            details={"auto": False, "forced_text_only": True},
        )
        await self._send_text(
            peer_id,
            self.app_service.append_remaining(answer, pending.remaining, unlimited=pending.unlimited_mode),
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
        )
        return True
        formats = self.app_service.available_delivery_formats(pending.hits)
        if choice == "cancel":
            session.pending_delivery = None
            await self._send_text(peer_id, "Выбор формата ответа отменен.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return True
        if choice is None or choice not in formats:
            quoted_formats = " или ".join(f'"{item}"' for item in formats)
            await self._send_text(peer_id, f"Сейчас ожидаю выбор формата ответа. Напишите {quoted_formats}.", buttons=self._delivery_keyboard(formats))
            return True
        session.pending_delivery = None
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="delivery_choice", sender_profile=sender_profile, details={"choice": choice})
        if choice == "текст":
            answer = self.app_service.answer_from_hits(question=pending.question, hits=pending.hits, recent_messages=pending.recent_messages, api_key=pending.api_key, custom_prompt=pending.custom_prompt)
            self._append_history(session, "user", pending.question)
            self._append_history(session, "assistant", answer)
            self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="text_answer", sender_profile=sender_profile, details={"auto": False})
            await self._send_text(peer_id, self.app_service.append_remaining(answer, pending.remaining, unlimited=pending.unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return True
        media_hits = self.app_service.hits_for_delivery_choice(pending.hits, choice)
        if not media_hits:
            await self._send_text(peer_id, "Подходящих медиа для этого выбора не найдено.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return True
        self._append_history(session, "user", pending.question)
        self._append_history(session, "assistant", f"Отправил материалы формата: {choice}.")
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="media_delivery", sender_profile=sender_profile, details={"choice": choice, "count": len(media_hits)})
        await self._send_text(peer_id, self.app_service.append_remaining(f"Отправляю материалы в формате: {choice}.", pending.remaining, unlimited=pending.unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
        await self._send_storage_hits(peer_id, media_hits)
        return True

    async def _handle_promo(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, code: str) -> None:
        promo_code = code.strip()
        if not promo_code:
            await self._send_text(peer_id, "Отправьте промокод для активации дополнительных запросов.", buttons=self._cancel_keyboard())
            return
        ok, message = self.app_service.redeem_promo_code(sender_id, promo_code)
        bonus_requests = self.app_service.get_user_bonus_requests(sender_id)
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="promo_redeem", sender_profile=sender_profile, details={"code": promo_code[:80], "ok": ok, "bonus_requests": bonus_requests})
        suffix = f"\n\nДоступно бонусных запросов: {bonus_requests}." if ok else ""
        await self._send_text(peer_id, f"{message}{suffix}", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_custom_command(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, custom_command: dict[str, Any]) -> None:
        command_name = str(custom_command.get("command_name") or "").strip() or "/command"
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="custom_command", sender_profile=sender_profile, details={"command": command_name})
        if bool(custom_command.get("notify_admin", 1)):
            await self._notify_custom_command_admins(command_name=command_name, sender_id=sender_id, sender_profile=sender_profile)
        response_text = str(custom_command.get("response_text") or "").strip()
        media_path = Path(str(custom_command.get("media_path") or "")) if custom_command.get("media_path") else None
        if response_text:
            await self._send_text(peer_id, response_text, buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
        if custom_command.get("media_path"):
            if not response_text:
                await self._send_text(peer_id, f"Для команды {command_name} настроен файл, но отправка файлов ботом отключена.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        if media_path and media_path.exists() and self._is_video_path(media_path):
            await self._send_text(
                peer_id,
                f"Видео для команды {command_name} не отправляется. Бот хранит только извлеченную информацию.",
                buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
            )
            return
        if media_path and media_path.exists():
            await self._send_local_document(peer_id, media_path)
            return
        if custom_command.get("media_path"):
            await self._send_text(peer_id, f"Медиа для команды {command_name} пока недоступно.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        if not response_text:
            await self._send_text(peer_id, f"Команда {command_name} выполнена.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

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
                LOGGER.exception("Failed to notify VK admin %s about %s", admin_id, command_name)

    async def _handle_managed_answer_choice(self, peer_id: int, session: ChatSession, sender_id: int, sender_profile: SenderProfile, is_admin: bool, text: str) -> bool:
        pending = session.pending_managed_choice
        if pending is None:
            return False
        normalized = self._normalize_choice_text(text)
        if normalized in {"отмена", "cancel"}:
            session.pending_managed_choice = None
            await self._send_text(peer_id, "Выбор варианта ответа отменен.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return True
        selected_option = self._resolve_managed_answer_option(text, pending.options)
        if selected_option is None:
            await self._send_text(peer_id, f"Сейчас ожидаю выбор готового варианта ответа. Нажмите кнопку ниже или напишите номер от 1 до {len(pending.options)}.", buttons=self._managed_answer_keyboard(pending.options))
            return True
        session.pending_managed_choice = None
        await self._send_managed_answer_option(peer_id, session, sender_id, sender_profile, is_admin, pending.question, selected_option, pending.remaining, pending.unlimited_mode)
        return True

    async def _send_managed_answer_option(self, peer_id: int, session: ChatSession, sender_id: int, sender_profile: SenderProfile, is_admin: bool, question: str, option: ManagedAnswerOption, remaining: int, unlimited_mode: bool) -> None:
        media_path = Path(option.media_path) if option.media_path else None
        media_exists = bool(media_path and media_path.exists())
        video_media_blocked = media_exists and self._is_video_path(media_path)
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="managed_answer_choice", sender_profile=sender_profile, details={"option_id": option.option_id, "option_label": option.option_label, "has_media": media_exists, "video_blocked": video_media_blocked})
        self._append_history(session, "user", question)
        assistant_text = option.response_text.strip() or f"Отправил вариант: {option.option_label}."
        self._append_history(session, "assistant", assistant_text)
        if option.response_text.strip():
            await self._send_text(peer_id, self.app_service.append_remaining(option.response_text.strip(), remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        if option.media_path:
            await self._send_text(peer_id, self.app_service.append_remaining("Для этого варианта настроен файл, но бот отвечает только текстом.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        elif media_exists and not video_media_blocked:
            await self._send_text(peer_id, self.app_service.append_remaining(f"Отправляю вариант: {option.option_label}.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
        elif video_media_blocked:
            await self._send_text(
                peer_id,
                self.app_service.append_remaining(
                    "Для этого варианта настроено видео, но бот не отправляет видеофайлы. Используйте текстовый ответ.",
                    remaining,
                    unlimited=unlimited_mode,
                ),
                buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
            )
        else:
            await self._send_text(peer_id, self.app_service.append_remaining("Для этого варианта пока не настроен ответ.", remaining, unlimited=unlimited_mode), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        if media_exists and not video_media_blocked:
            await self._send_local_document(peer_id, media_path)
        elif option.media_path and not video_media_blocked:
            await self._send_text(peer_id, "Медиа для выбранного варианта пока недоступно.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

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

    async def _handle_set_api(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, api_key: str) -> None:
        if not api_key:
            await self._send_text(peer_id, "Отправьте API token.", buttons=self._cancel_keyboard())
            return
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/set_api"})
        ok, error_text = self.app_service.validate_user_api_key(api_key)
        if not ok:
            self.app_service.save_user_api_error(sender_id, error_text or "unknown error")
            await self._send_text(peer_id, "API token не прошел проверку. Убедитесь, что он действителен и имеет доступ к настроенным моделям.\n\n" f"Текст ошибки: {(error_text or 'unknown error')[:400]}", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        prefs = self.app_service.get_user_preferences(sender_id)
        has_saved_prompt = bool((prefs.get("custom_prompt") or "").strip())
        self.app_service.save_user_api_key(sender_id, api_key)
        text = "Ваш API token сохранен и проверен. Для вас включен безлимит."
        if has_saved_prompt:
            text += " Ранее сохраненный prompt снова активирован."
        await self._send_text(peer_id, text, buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_delete_api(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool) -> None:
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/delete_api"})
        prefs = self.app_service.get_user_preferences(sender_id)
        had_prompt = bool((prefs.get("custom_prompt") or "").strip())
        self.app_service.clear_user_api_key(sender_id)
        text = "Ваш API token удален. Безлимит отключен."
        if had_prompt:
            text = "Ваш API token удален. Безлимит отключен, пользовательский prompt сохранен, но не будет применяться, пока вы снова не добавите API token."
        await self._send_text(peer_id, text, buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_set_prompt(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, prompt_text: str) -> None:
        if not prompt_text:
            await self._send_text(peer_id, "Отправьте ваш пользовательский prompt.", buttons=self._cancel_keyboard())
            return
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/set_prompt"})
        prefs = self.app_service.get_user_preferences(sender_id)
        if not self.app_service.get_active_api_key(prefs):
            await self._send_text(peer_id, "Сначала добавьте рабочий API token через /set_api.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        self.app_service.save_user_prompt(sender_id, prompt_text)
        await self._send_text(peer_id, "Ваш пользовательский prompt сохранен.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_delete_prompt(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool) -> None:
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="settings", sender_profile=sender_profile, details={"command": "/delete_prompt"})
        self.app_service.clear_user_prompt(sender_id)
        await self._send_text(peer_id, "Ваш пользовательский prompt удален.", buttons=self._settings_keyboard(is_admin=is_admin, user_id=sender_id))
    async def _handle_stats(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, raw_arg: str) -> None:
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="stats_view", sender_profile=sender_profile)
        rows = self.app_service.get_user_statistics(raw_arg)
        if raw_arg:
            if not rows:
                await self._send_text(peer_id, "Статистика по этому пользователю не найдена.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
                return
            await self._send_text(peer_id, self.app_service.format_detailed_user_stats(rows[0]), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        if not rows:
            await self._send_text(peer_id, "Статистика пока пуста.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        lines = ["Статистика по пользователям VK:"]
        for row in rows:
            lines.append(self.app_service.format_user_stats_row(row))
        await self._send_text(peer_id, "\n".join(lines), buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))

    async def _handle_department_survey(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool, text: str) -> bool:
        if self.app_service.has_completed_department_survey(sender_id):
            return False
        department = self.app_service.normalize_department(text)
        if department is None:
            await self._send_text(peer_id, self._department_prompt_text(), buttons=self._department_keyboard())
            return True
        self.app_service.save_user_department(sender_id, department)
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="department_selected", sender_profile=sender_profile, details={"department": department})
        await self._send_text(peer_id, f"Спасибо! Сохранил ваш департамент: {department}. Теперь можно пользоваться ботом.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
        return True

    async def _handle_homosap(self, peer_id: int, sender_id: int, sender_profile: SenderProfile, is_admin: bool) -> None:
        video_path = self.settings.homosap_video_path
        file_ready = video_path.exists()
        self.app_service.log_event(user_id=sender_id, chat_id=peer_id, event_type="quest_homosap", sender_profile=sender_profile, details={"command": "/HOMOSAP", "file_ready": file_ready})
        await self._notify_homosap_admins(sender_id=sender_id, sender_profile=sender_profile, file_ready=file_ready)
        if not file_ready:
            await self._send_text(peer_id, "Видео HOMOSAP пока не загружено. Попробуйте позже.", buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id))
            return
        await self._send_text(
            peer_id,
            "Видео HOMOSAP не отправляется. Бот хранит только извлеченную информацию.",
            buttons=self._main_keyboard(is_admin=is_admin, user_id=sender_id),
        )

    async def _notify_homosap_admins(self, *, sender_id: int, sender_profile: SenderProfile, file_ready: bool) -> None:
        admin_ids = sorted(self.app_service.external_admin_user_ids())
        if not admin_ids:
            LOGGER.warning("VK /HOMOSAP invoked by %s, but no VK uploader admins are configured.", sender_id)
            return
        username = f"@{sender_profile.username}" if sender_profile.username else "-"
        name = " ".join(part for part in [sender_profile.first_name, sender_profile.last_name] if part) or "-"
        status_text = "Файл HOMOSAP.mp4 найден, но отправка видео пользователю отключена." if file_ready else "Файл HOMOSAP.mp4 пока отсутствует на диске."
        text = f"Ввели команду /HOMOSAP.\nПользователь: {name}\nUsername: {username}\nUser ID: {sender_id}\nСтатус: {status_text}"
        for admin_id in admin_ids:
            try:
                await self._send_text(admin_id, text, buttons=self._main_keyboard(is_admin=True))
            except Exception:
                LOGGER.exception("Failed to notify VK admin %s about /HOMOSAP", admin_id)

    async def _maybe_send_welcome(self, peer_id: int, user_id: int, is_admin: bool) -> bool:
        if self.app_service.has_sent_welcome(user_id):
            return False
        await self._send_text(peer_id, self._welcome_text(is_admin=is_admin, user_id=user_id), buttons=self._main_keyboard(is_admin=is_admin, user_id=user_id))
        self.app_service.mark_welcome_sent(user_id)
        return True

    def _append_history(self, session: ChatSession, role: str, content: str) -> None:
        clean = content.strip()
        if clean:
            session.recent_messages.append({"role": role, "content": clean})

    async def _send_text(self, peer_id: int, text: str, *, buttons: list[list[str]] | None = None) -> None:
        parts = split_for_telegram(text)
        if not parts:
            return
        for index, part in enumerate(parts):
            await self._send_message(peer_id, part, keyboard=buttons if index == len(parts) - 1 else None)

    async def _send_message(self, peer_id: int, text: str, *, keyboard: list[list[str]] | None = None, attachment: str | None = None) -> None:
        params: dict[str, Any] = {"random_id": random.randint(1, 2_147_483_647)}
        if peer_id >= 2_000_000_000:
            params["peer_id"] = peer_id
        else:
            params["user_id"] = peer_id
        if text:
            params["message"] = text
        if keyboard:
            params["keyboard"] = self._build_keyboard(keyboard)
        if attachment:
            params["attachment"] = attachment
        LOGGER.info("VK outgoing message | peer=%s | text=%s | has_keyboard=%s | has_attachment=%s", peer_id, (text[:120] if text else "<empty>"), bool(keyboard), bool(attachment))
        try:
            await self._api_call("messages.send", params)
        except Exception:
            if keyboard:
                LOGGER.exception("VK send with keyboard failed for peer=%s, retrying without keyboard", peer_id)
                fallback_params = dict(params)
                fallback_params.pop("keyboard", None)
                await self._api_call("messages.send", fallback_params)
                return
            raise

    async def _send_local_document(self, peer_id: int, file_path: Path, *, caption: str | None = None) -> None:
        await self._send_text(peer_id, caption or "Отправка файлов ботом отключена. Используйте текстовый ответ.")

    async def _send_storage_hits(self, peer_id: int, hits: list[SearchHit]) -> None:
        LOGGER.info("VK file sending is disabled | count=%s", len(hits))
        await self._send_text(peer_id, "Бот не отправляет файлы. Могу ответить только текстом.")

    async def _send_storage_item(self, peer_id: int, item: dict[str, Any]) -> bool:
        LOGGER.info(
            "VK storage item sending is disabled | source_chat_id=%s | source_message_id=%s",
            item.get("source_chat_id"),
            item.get("source_message_id"),
        )
        return False

    async def _upload_document(self, peer_id: int, file_path: Path) -> str:
        if self._vk_upload is None:
            raise VkApiError("VK upload client is not initialized.")
        saved = await asyncio.to_thread(self._vk_upload.document_message, str(file_path), title=file_path.name, peer_id=peer_id)
        doc_data: dict[str, Any] | None = None
        if isinstance(saved, list) and saved:
            candidate = saved[0]
            doc_data = candidate.get("doc", candidate) if isinstance(candidate, dict) else None
        elif isinstance(saved, dict):
            doc_data = saved.get("doc", saved)
        if not doc_data:
            raise VkApiError(f"VK docs.save returned unexpected payload: {saved}")
        return f"doc{int(doc_data['owner_id'])}_{int(doc_data['id'])}"

    async def _api_call(self, method: str, params: dict[str, Any]) -> Any:
        try:
            return await asyncio.to_thread(self._api_call_sync, method, params)
        except (ApiError, VkLibraryError) as exc:
            raise VkApiError(f"{method}: {exc}") from exc

    def _api_call_sync(self, method: str, params: dict[str, Any]) -> Any:
        if self._vk is None:
            raise VkApiError("VK API client is not initialized.")
        api_method: Any = self._vk
        for part in method.split("."):
            api_method = getattr(api_method, part)
        return api_method(**params)

    async def _discover_group_id(self) -> int:
        response = await self._api_call("groups.getById", {})
        if not response:
            raise VkApiError("VK groups.getById returned empty response.")
        group = response[0] if isinstance(response, list) else response.get("groups", [{}])[0]
        group_id = int(group.get("id") or 0)
        if group_id <= 0:
            raise VkApiError("Could not determine VK group id from API_VK.")
        return group_id

    @staticmethod
    def _friendly_vk_error(exc: Exception) -> str:
        text = str(exc)
        if "longpoll for this group is not enabled" in text.lower():
            return (
                "В VK для группы не включен Long Poll. Включите его в настройках сообщества: "
                "Управление -> Работа с API -> Long Poll API, затем перезапустите бот или просто подождите минуту."
            )
        return text

    async def _get_sender_profile(self, user_id: int) -> SenderProfile:
        cached = self._sender_cache.get(user_id)
        if cached is not None:
            return cached
        try:
            response = await self._api_call("users.get", {"user_ids": user_id, "fields": "screen_name"})
            user = response[0] if isinstance(response, list) and response else {}
            profile = SenderProfile(username=user.get("screen_name"), first_name=user.get("first_name"), last_name=user.get("last_name"))
        except Exception:
            LOGGER.exception("Failed to load VK sender profile for %s", user_id)
            profile = SenderProfile()
        self._sender_cache[user_id] = profile
        return profile

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
        titles = {"видео": "Видео", "фото": "Фото", "аудио": "Аудио", "текст": "Текст"}
        labels = [titles.get(item, item.title()) for item in formats]
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
            "Команды VK-бота:",
            "/start, /help, /menu - показать меню",
            "/ask <вопрос> - задать вопрос по памяти",
            "/search <запрос> - найти подходящие материалы",
            "/list <DD-MM-YYYY> - список материалов за дату",
            "/file <ITEM_ID> - отправить оригинал из хранилища",
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
            lines.extend(["/stats [USER_ID] - статистика по пользователям VK", "/upload_local - открыть VK localhost-админку"])
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
        intro = ["Привет! Это VK-бот с RAG-памятью."]
        if is_admin:
            intro.append("У вас есть доступ к статистике VK и VK-разделу localhost-админки.")
        else:
            intro.append("Здесь можно искать материалы по тексту, фото, аудио и видео и активировать промокоды.")
        intro.extend(["", self._help_text(is_admin=is_admin, user_id=user_id)])
        return "\n".join(intro)

    def _local_upload_text(self, admin_user_id: int) -> str:
        params = {"admin_user_id": admin_user_id}
        if self.settings.local_upload_token:
            params["token"] = self.settings.local_upload_token
        upload_url = f"{self.settings.local_upload_base_url}{self.app_service.admin_panel_path()}?{urlencode(params)}"
        return (
            "VK-раздел админ-панели доступен через браузер на этом компьютере. Через него можно загружать материалы,"
            " создавать промокоды, команды и готовые варианты ответов.\n\n"
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

    @staticmethod
    def _build_keyboard(rows: list[list[str]]) -> str:
        keyboard = VkKeyboard(one_time=False, inline=False)
        for row_index, row in enumerate(rows):
            if row_index > 0:
                keyboard.add_line()
            for label in row:
                safe_label = str(label).strip()[:40] or "..."
                keyboard.add_button(
                    safe_label,
                    color=VkMenuBot._keyboard_color_for_label(safe_label),
                    payload={"command": safe_label},
                )
        return keyboard.get_keyboard()

    @staticmethod
    def _keyboard_color_for_label(label: str) -> VkKeyboardColor:
        normalized = label.strip().lower()
        if normalized in {"назад", "отмена"}:
            return VkKeyboardColor.SECONDARY
        if normalized in {"заявка админу", "помощь"}:
            return VkKeyboardColor.POSITIVE
        return VkKeyboardColor.PRIMARY

    @staticmethod
    def _payload_command_text(payload: Any) -> str:
        if payload is None:
            return ""
        raw_payload = payload
        if isinstance(raw_payload, str):
            raw_payload = raw_payload.strip()
            if not raw_payload:
                return ""
            try:
                raw_payload = json.loads(raw_payload)
            except Exception:
                return raw_payload
        if isinstance(raw_payload, dict):
            for key in ("command", "text", "label", "value"):
                value = str(raw_payload.get(key) or "").strip()
                if value:
                    return value
        return ""

    @staticmethod
    def _payload_preview(payload: Any) -> str:
        if payload is None:
            return "<none>"
        text = str(payload)
        return text[:120] + ("..." if len(text) > 120 else "")
