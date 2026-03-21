"""
????: settings_bot.py
?????? legacy-?????????? ?????????? Telegram-???? ????????,
??????? ????????????? ?? ???????? ?? ?????? runtime.
"""

from __future__ import annotations

import logging
from typing import Any

from telethon import TelegramClient, events

from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.services.rag_service import RagService
from telegram_rag_memory_bot.utils.dates import today_iso
from telegram_rag_memory_bot.utils.text import split_for_telegram

LOGGER = logging.getLogger(__name__)


class TelegramSettingsBot:
    def __init__(self, settings: Settings, rag_service: RagService) -> None:
        self.settings = settings
        self.rag_service = rag_service
        self.client = TelegramClient(
            settings.settings_bot_session_name,
            settings.telegram_api_id,
            settings.telegram_api_hash,
        )
        self.bot_user_id: int | None = None
        self._register_handlers()

    async def run(self) -> None:
        if not self.settings.settings_bot_enabled:
            LOGGER.info("Settings bot is disabled because SETTINGS_BOT_TOKEN is empty.")
            return
        await self.client.start(bot_token=self.settings.settings_bot_token)
        me = await self.client.get_me()
        self.bot_user_id = int(me.id)
        LOGGER.info(
            "Settings bot connected as @%s id=%s",
            getattr(me, "username", None) or "unknown",
            self.bot_user_id,
        )
        await self.client.run_until_disconnected()

    def _register_handlers(self) -> None:
        @self.client.on(events.NewMessage(incoming=True))
        async def private_message_handler(event: events.NewMessage.Event) -> None:
            if not event.is_private:
                return
            await self._handle_private_message(event)

    async def _handle_private_message(self, event: events.NewMessage.Event) -> None:
        sender_id = getattr(event, "sender_id", None)
        if sender_id is None:
            await event.reply("Не удалось определить пользователя.")
            return

        text = (event.raw_text or "").strip()
        if not text:
            return

        sender = await event.get_sender()
        sender_profile = self._sender_profile(sender)
        chat_id = int(event.chat_id)
        command = text.split(maxsplit=1)[0].lower()

        try:
            if text in {"/start", "/help"} or text.startswith("/start ") or text.startswith("/help "):
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="settings",
                    sender_profile=sender_profile,
                    details={"command": command or "/help"},
                )
                await self._reply_long(event, self._help_text())
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
            if command.startswith("/"):
                self._log_user_event(
                    user_id=sender_id,
                    chat_id=chat_id,
                    event_type="unknown_command",
                    sender_profile=sender_profile,
                    details={"command": command},
                )
                await event.reply(self._unknown_command_text())
                return
            await self._reply_long(event, self._help_text(include_intro=False, include_non_command_note=True))
        except Exception as exc:
            LOGGER.exception("Settings bot command failed")
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
                "Ваш API token сохранен и проверен. Для вас включен безлимит у основного ассистента. Ранее сохраненный prompt снова активирован."
            )
            return
        await event.reply("Ваш API token сохранен и проверен. Для вас включен безлимит у основного ассистента.")

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
        await event.reply(
            "Ваш пользовательский prompt сохранен и будет применяться к ответам основного ассистента, пока активен ваш API token."
        )

    async def _handle_delete_prompt(self, event: events.NewMessage.Event, user_id: int) -> None:
        self.rag_service.clear_user_prompt(user_id)
        await event.reply("Ваш пользовательский prompt удален.")

    async def _handle_my_settings(self, event: events.NewMessage.Event, user_id: int) -> None:
        prefs = self.rag_service.get_user_preferences(user_id)
        has_api = bool(self._get_active_api_key(prefs))
        has_prompt = bool((prefs.get("custom_prompt") or "").strip())
        prompt_active = has_api and has_prompt
        lines = [
            "Настройки для основного ассистента:",
            f"Личный API token: {'подключен' if has_api else 'не подключен'}",
            f"Безлимит: {'включен' if has_api else 'нет'}",
            f"Пользовательский prompt сохранен: {'да' if has_prompt else 'нет'}",
            f"Пользовательский prompt активен: {'да' if prompt_active else 'нет'}",
        ]
        last_error = (prefs.get("api_key_last_error") or "").strip()
        if last_error:
            lines.append(f"Последняя ошибка API: {last_error[:300]}")
        if not has_api:
            lines.append(f"Лимит без личного токена у основного ассистента: {self.settings.daily_message_limit} сообщений в день.")
        await self._reply_long(event, "\n".join(lines))

    async def _reply_long(self, event: events.NewMessage.Event, text: str) -> None:
        parts = split_for_telegram(text)
        if not parts:
            return
        for index, part in enumerate(parts):
            if index == 0:
                await event.reply(part)
            else:
                await self.client.send_message(event.chat_id, part)

    def _help_text(self, include_intro: bool = True, include_non_command_note: bool = False) -> str:
        lines = []
        if include_intro:
            lines.extend(
                [
                    "Привет! Это отдельный бот для настройки личного API token и prompt.",
                    "Эти настройки применяются к вашему Telegram ID и используются, когда вы общаетесь с основным ассистентом через аккаунт.",
                    "",
                ]
            )
        if include_non_command_note:
            lines.extend(
                [
                    "Этот бот работает только с командами настроек.",
                    "",
                ]
            )
        lines.extend(
            [
                "Команды:",
                "/set_api <token> - подключить свой OpenAI API token",
                "/delete_api - удалить свой API token",
                "/set_prompt <текст> - сохранить свой prompt для ответов",
                "/delete_prompt - удалить свой prompt",
                "/my_settings - показать статус личного token и prompt",
                "/help или /start - показать эту справку",
                "",
                f"Без личного API token у основного ассистента лимит: {self.settings.daily_message_limit} сообщений в день.",
                "С личным рабочим API token: безлимит.",
                "Пользовательский prompt работает только при активном личном API token.",
                "Если удалить API token, prompt сохранится, но перестанет применяться, пока токен не будет добавлен снова.",
            ]
        )
        return "\n".join(lines)

    def _log_user_event(
        self,
        *,
        user_id: int,
        chat_id: int,
        event_type: str,
        sender_profile: tuple[str | None, str | None, str | None],
        details: dict[str, Any] | None = None,
    ) -> None:
        username, first_name, last_name = sender_profile
        self.rag_service.log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type=event_type,
            event_date=today_iso(),
            charged=False,
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
    def _get_active_api_key(user_prefs: dict[str, object]) -> str | None:
        api_key = (user_prefs.get("api_key") or "") if user_prefs else ""
        return str(api_key).strip() or None

    @staticmethod
    def _unknown_command_text() -> str:
        return 'Такой команды нет. Если надо добавить, напишите: "пж добавьте админы".'
"""
Этот файл содержит отдельный legacy-бот настроек,
сохраненный для обратной совместимости со старым сценарием.
"""
