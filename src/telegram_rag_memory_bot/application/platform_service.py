"""
????: platform_service.py
????????? ????? ?????????? ?????? ?????????????? ?????????
? ??????????? ?????????????? Telegram/VK ??? ????? ????.
"""

from __future__ import annotations

from typing import Any

from telegram_rag_memory_bot.application.assistant_service import AssistantApplicationService
from telegram_rag_memory_bot.domain.models import LocalUploadRequest, LocalUploadResult
from telegram_rag_memory_bot.domain.ports import NotificationGateway, StorageGateway
from telegram_rag_memory_bot.utils.dates import parse_iso_date


class PlatformAssistantService(AssistantApplicationService):
    platform_code = "telegram"
    admin_path_segment = "telegram"

    def external_authorized_user_ids(self) -> set[int]:
        return self.settings.authorized_user_ids

    def external_admin_user_ids(self) -> set[int]:
        return self.settings.uploader_user_ids

    def to_internal_user_id(self, user_id: int) -> int:
        return int(user_id)

    def to_external_user_id(self, user_id: int) -> int:
        return int(user_id)

    def to_internal_chat_id(self, chat_id: int) -> int:
        return int(chat_id)

    def admin_panel_path(self) -> str:
        return f"/{self.admin_path_segment}/admin"

    def is_authorized(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        banned, _reason = self.is_banned(user_id)
        if banned:
            return False
        if self.settings.public_access:
            return True
        allowed = self.external_authorized_user_ids()
        if not allowed:
            return False
        return user_id in allowed or user_id in self.external_admin_user_ids()

    def is_admin(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        return user_id in self.external_admin_user_ids()

    def upsert_site_account(
        self,
        *,
        username: str,
        password_hash: str,
        display_name: str,
        platform_user_id: int,
        is_active: bool = True,
    ) -> None:
        self.rag_service.database.upsert_site_account(
            username=username,
            password_hash=password_hash,
            display_name=display_name,
            platform=self.platform_code,
            platform_user_id=self.to_internal_user_id(platform_user_id),
            is_active=is_active,
        )

    def get_site_account(self, username: str) -> dict[str, Any] | None:
        row = self.rag_service.database.get_site_account(username)
        if row is None:
            return None
        if str(row.get("platform") or "") != self.platform_code:
            return None
        payload = dict(row)
        payload["platform_user_id"] = self.to_external_user_id(int(payload.get("platform_user_id") or 0))
        return payload

    def get_site_account_any(self, username: str) -> dict[str, Any] | None:
        row = self.rag_service.database.get_site_account_any(username)
        if row is None:
            return None
        if str(row.get("platform") or "") != self.platform_code:
            return None
        payload = dict(row)
        payload["platform_user_id"] = self.to_external_user_id(int(payload.get("platform_user_id") or 0))
        return payload

    def next_site_platform_user_id(self) -> int:
        return int(self.rag_service.database.next_site_platform_user_id())

    def list_site_accounts(self, limit: int = 300) -> list[dict[str, Any]]:
        rows = self.rag_service.database.list_site_accounts(platform=self.platform_code, limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["platform_user_id"] = self.to_external_user_id(int(payload.get("platform_user_id") or 0))
            normalized_rows.append(payload)
        return normalized_rows

    def deactivate_site_account(self, username: str) -> bool:
        return self.rag_service.database.deactivate_site_account(username, platform=self.platform_code)

    def create_site_support_message(
        self,
        *,
        username: str,
        site_user_id: int,
        display_name: str,
        sender_role: str,
        message_text: str,
    ) -> int:
        return int(
            self.rag_service.database.create_site_support_message(
                username=username,
                site_user_id=self.to_internal_user_id(site_user_id) if site_user_id else 0,
                display_name=display_name,
                sender_role=sender_role,
                message_text=message_text,
            )
        )

    def list_site_support_messages(self, username: str, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.rag_service.database.list_site_support_messages(username, limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            site_user_id = int(payload.get("site_user_id") or 0)
            if site_user_id:
                payload["site_user_id"] = self.to_external_user_id(site_user_id)
            normalized_rows.append(payload)
        return normalized_rows

    def list_site_support_threads(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.rag_service.database.list_site_support_threads(limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            site_user_id = int(payload.get("site_user_id") or 0)
            if site_user_id:
                payload["site_user_id"] = self.to_external_user_id(site_user_id)
            normalized_rows.append(payload)
        return normalized_rows

    def mark_site_support_read_by_admin(self, username: str) -> int:
        return int(self.rag_service.database.mark_site_support_read_by_admin(username))

    def get_user_preferences(self, user_id: int) -> dict[str, Any]:
        return super().get_user_preferences(self.to_internal_user_id(user_id))

    def has_sent_welcome(self, user_id: int) -> bool:
        return super().has_sent_welcome(self.to_internal_user_id(user_id))

    def mark_welcome_sent(self, user_id: int) -> None:
        super().mark_welcome_sent(self.to_internal_user_id(user_id))

    def consume_daily_limit(self, user_id: int, *, has_personal_api: bool, is_admin: bool) -> tuple[bool, int, bool]:
        return super().consume_daily_limit(
            self.to_internal_user_id(user_id),
            has_personal_api=has_personal_api,
            is_admin=is_admin,
        )

    def log_event(
        self,
        *,
        user_id: int,
        chat_id: int,
        event_type: str,
        sender_profile,
        charged: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        enriched_details = dict(details or {})
        enriched_details.setdefault("platform", self.platform_code)
        super().log_event(
            user_id=self.to_internal_user_id(user_id),
            chat_id=self.to_internal_chat_id(chat_id),
            event_type=event_type,
            sender_profile=sender_profile,
            charged=charged,
            details=enriched_details,
        )

    def save_user_api_key(self, user_id: int, api_key: str) -> None:
        super().save_user_api_key(self.to_internal_user_id(user_id), api_key)

    def clear_user_api_key(self, user_id: int) -> None:
        super().clear_user_api_key(self.to_internal_user_id(user_id))

    def save_user_api_error(self, user_id: int, error_text: str) -> None:
        super().save_user_api_error(self.to_internal_user_id(user_id), error_text)

    def save_user_prompt(self, user_id: int, prompt: str) -> None:
        super().save_user_prompt(self.to_internal_user_id(user_id), prompt)

    def clear_user_prompt(self, user_id: int) -> None:
        super().clear_user_prompt(self.to_internal_user_id(user_id))

    def save_user_prompt_profile(self, user_id: int, prompt_profile: str) -> None:
        super().save_user_prompt_profile(self.to_internal_user_id(user_id), prompt_profile)

    def clear_user_prompt_profile(self, user_id: int) -> None:
        super().clear_user_prompt_profile(self.to_internal_user_id(user_id))

    def get_user_bonus_requests(self, user_id: int) -> int:
        return super().get_user_bonus_requests(self.to_internal_user_id(user_id))

    def redeem_promo_code(self, user_id: int, code: str) -> tuple[bool, str]:
        return super().redeem_promo_code(self.to_internal_user_id(user_id), code)

    def get_custom_command(self, command_name: str) -> dict[str, Any] | None:
        return super().get_custom_command(command_name, platform=self.platform_code)

    def list_custom_commands(self) -> list[dict[str, Any]]:
        return super().list_custom_commands(platform=self.platform_code)

    def create_pending_material_upload(self, upload_request: LocalUploadRequest) -> int:
        internal_request = LocalUploadRequest(
            admin_user_id=self.to_internal_user_id(upload_request.admin_user_id),
            content_date=upload_request.content_date,
            description=upload_request.description,
            source_text=upload_request.source_text,
            local_file_path=upload_request.local_file_path,
            original_file_name=upload_request.original_file_name,
            content_scope=upload_request.content_scope,
        )
        return super().create_pending_material_upload(internal_request, platform=self.platform_code)

    def list_pending_material_uploads(self, *, status: str = "pending", limit: int = 100) -> list[dict[str, Any]]:
        rows = super().list_pending_material_uploads(status=status, platform=self.platform_code, limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            payload = dict(row)
            payload["admin_user_id"] = self.to_external_user_id(int(payload.get("admin_user_id") or 0))
            normalized_rows.append(payload)
        return normalized_rows

    def delete_pending_material_upload(self, pending_id: int) -> bool:
        return super().delete_pending_material_upload(pending_id)

    async def analyze_pending_material_upload(
        self,
        pending_id: int,
        upload_request: LocalUploadRequest,
    ) -> LocalUploadResult:
        internal_request = LocalUploadRequest(
            admin_user_id=self.to_internal_user_id(upload_request.admin_user_id),
            content_date=upload_request.content_date,
            description=upload_request.description,
            source_text=upload_request.source_text,
            local_file_path=upload_request.local_file_path,
            original_file_name=upload_request.original_file_name,
            content_scope=upload_request.content_scope,
        )
        return await super().analyze_pending_material_upload(pending_id, internal_request)

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
        internal_sender_id = self.to_internal_user_id(source_sender_id) if source_sender_id is not None else None
        return super().attach_item_source(
            item_id,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            source_sender_id=internal_sender_id,
            telegram_message_date=telegram_message_date,
            metadata=metadata,
        )

    def create_custom_command(
        self,
        command_name: str,
        *,
        response_text: str,
        media_path: str | None,
        notify_admin: bool = True,
        enabled: bool = True,
    ) -> None:
        super().create_custom_command(
            command_name,
            platform=self.platform_code,
            response_text=response_text,
            media_path=media_path,
            notify_admin=notify_admin,
            enabled=enabled,
        )

    def delete_custom_command(self, command_name: str) -> bool:
        return super().delete_custom_command(command_name, platform=self.platform_code)

    def build_user_settings_text(self, user_id: int) -> str:
        prefs = self.get_user_preferences(user_id)
        has_api = bool(self.get_active_api_key(prefs))
        has_prompt = bool((prefs.get("custom_prompt") or "").strip())
        prompt_active = has_api and has_prompt
        prompt_profile = self.get_prompt_profile(prefs)
        department = (prefs.get("department") or "").strip()
        bonus_requests = max(int(prefs.get("bonus_requests") or 0), 0)
        lines = [
            f"Департамент: {department or '-'}",
            f"Профиль prompt: {self.PROMPT_PROFILE_LABELS.get(prompt_profile, 'Департаментный')}",
            f"Бонусные запросы: {bonus_requests}",
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
        return "\n".join(lines)

    def get_user_department(self, user_id: int) -> str | None:
        return super().get_user_department(self.to_internal_user_id(user_id))

    def has_completed_department_survey(self, user_id: int) -> bool:
        return bool(self.get_user_department(user_id))

    def save_user_department(self, user_id: int, department: str) -> None:
        super().save_user_department(self.to_internal_user_id(user_id), department)

    def get_item(self, item_id: int) -> dict[str, Any] | None:
        return super().get_item(item_id)

    def is_banned(self, user_id: int | None) -> tuple[bool, str]:
        if user_id is None:
            return False, ""
        return super().is_banned(self.to_internal_user_id(user_id))

    def consume_department_action_limit(self, user_id: int, action_department: str) -> tuple[bool, int, bool, str]:
        bucket = self.department_mode_bucket_for_user(user_id, action_department)
        internal_user_id = self.to_internal_user_id(user_id)
        allowed, _used, remaining = self.rag_service.consume_daily_department_mode(internal_user_id, self._today(), bucket, daily_limit=1)
        if allowed:
            return True, remaining, False, bucket
        bonus_used, bonus_remaining = self.rag_service.consume_mode_credit(internal_user_id, bucket)
        if bonus_used:
            return True, bonus_remaining, True, bucket
        return False, 0, False, bucket

    def grant_department_special_requests(self, user_id: int, credits: int) -> tuple[str, str]:
        return super().grant_department_special_requests(self.to_internal_user_id(user_id), credits)

    def create_access_request(
        self,
        *,
        user_id: int,
        request_name: str,
        reason: str,
        request_type: str,
        mode_bucket: str | None = None,
    ) -> int:
        return super().create_access_request(
            user_id=self.to_internal_user_id(user_id),
            request_name=request_name,
            reason=reason,
            request_type=request_type,
            mode_bucket=mode_bucket,
        )

    def list_access_requests(self, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        rows = super().list_access_requests(status=status, limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            internal_user_id = int(row.get("user_id") or 0)
            if not self._match_internal_user_id(internal_user_id):
                continue
            payload = dict(row)
            payload["user_id"] = self.to_external_user_id(internal_user_id)
            reviewed_by = int(payload.get("reviewed_by_user_id") or 0)
            if reviewed_by:
                payload["reviewed_by_user_id"] = self.to_external_user_id(reviewed_by)
            normalized_rows.append(payload)
        return normalized_rows

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
        row = super().review_access_request(
            request_id,
            status=status,
            reviewed_by_user_id=self.to_internal_user_id(reviewed_by_user_id),
            decision_note=decision_note,
            granted_bonus_requests=granted_bonus_requests,
            granted_mode_credits=granted_mode_credits,
        )
        if row is None:
            return None
        payload = dict(row)
        payload["user_id"] = self.to_external_user_id(int(payload["user_id"]))
        reviewed_by = int(payload.get("reviewed_by_user_id") or 0)
        if reviewed_by:
            payload["reviewed_by_user_id"] = self.to_external_user_id(reviewed_by)
        return payload

    def set_ban(self, user_id: int, *, reason: str, banned_by_user_id: int) -> None:
        super().set_ban(self.to_internal_user_id(user_id), reason=reason, banned_by_user_id=self.to_internal_user_id(banned_by_user_id))

    def clear_ban(self, user_id: int) -> bool:
        return super().clear_ban(self.to_internal_user_id(user_id))

    def list_banned_users(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = super().list_banned_users(limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            internal_user_id = int(row.get("user_id") or 0)
            if not self._match_internal_user_id(internal_user_id):
                continue
            payload = dict(row)
            payload["user_id"] = self.to_external_user_id(internal_user_id)
            banned_by = int(payload.get("banned_by_user_id") or 0)
            if banned_by:
                payload["banned_by_user_id"] = self.to_external_user_id(banned_by)
            normalized_rows.append(payload)
        return normalized_rows

    def list_user_events(self, user_id: int | None = None, limit: int = 300) -> list[dict[str, Any]]:
        internal_user_id = self.to_internal_user_id(user_id) if user_id is not None else None
        rows = super().list_user_events(user_id=internal_user_id, limit=limit)
        normalized_rows: list[dict[str, Any]] = []
        for row in rows:
            current_user_id = int(row.get("user_id") or 0)
            if not self._match_internal_user_id(current_user_id):
                continue
            payload = dict(row)
            payload["user_id"] = self.to_external_user_id(current_user_id)
            normalized_rows.append(payload)
        return normalized_rows

    def get_user_statistics(self, raw_arg: str) -> list[dict[str, Any]]:
        raw = raw_arg.strip()
        if raw:
            normalized = raw[1:] if raw.startswith("-") else raw
            if not normalized.isdigit():
                raise ValueError("Использование: /stats или /stats USER_ID")
            internal_id = self.to_internal_user_id(int(raw))
            rows = self.rag_service.get_user_statistics(self._today(), limit=1, user_id=internal_id)
        else:
            rows = self.rag_service.get_user_statistics(self._today(), limit=1000)
        normalized_rows = [self._normalize_stats_row(row) for row in rows if self._match_internal_user_id(int(row["user_id"]))]
        return [self._enrich_stats_row(row) for row in normalized_rows]

    def validate_upload_request(self, raw_date: str, description: str, admin_user_id: int, content_scope: str = "dated") -> LocalUploadRequest:
        normalized_scope = self.normalize_content_scope(content_scope)
        content_date = parse_iso_date(raw_date) if normalized_scope == "dated" else ""
        clean_description = description.strip()
        if not clean_description:
            raise ValueError("Описание обязательно.")
        if not self.is_admin(admin_user_id):
            raise PermissionError("Только администратор может загружать материалы.")
        return LocalUploadRequest(
            admin_user_id=admin_user_id,
            content_date=content_date,
            description=clean_description,
            content_scope=normalized_scope,
        )

    async def process_local_upload(
        self,
        upload_request: LocalUploadRequest,
        *,
        storage_gateway: StorageGateway,
        notification_gateway: NotificationGateway | None = None,
    ) -> LocalUploadResult:
        internal_request = LocalUploadRequest(
            admin_user_id=self.to_internal_user_id(upload_request.admin_user_id),
            content_date=upload_request.content_date,
            description=upload_request.description,
            source_text=upload_request.source_text,
            local_file_path=upload_request.local_file_path,
            original_file_name=upload_request.original_file_name,
            content_scope=upload_request.content_scope,
        )
        result = await super().process_local_upload(
            internal_request,
            storage_gateway=storage_gateway,
            notification_gateway=None,
        )
        if notification_gateway is not None:
            await notification_gateway.notify_user(
                user_id=upload_request.admin_user_id,
                text=(
                    "Материал загружен через localhost и проиндексирован.\n"
                    f"Проиндексировано: #{result.item_id}\n"
                    f"Дата: {self.display_content_date(result.content_date, upload_request.content_scope)}\n"
                    f"Тип: {result.item_type}\n"
                    f"Файл: {result.file_name or '-'}\n"
                    f"Кратко: {result.summary}"
                ),
            )
        return result

    def _match_internal_user_id(self, internal_user_id: int) -> bool:
        return True

    def _normalize_stats_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        normalized["user_id"] = self.to_external_user_id(int(row["user_id"]))
        normalized["platform"] = self.platform_code
        return normalized

    def _today(self) -> str:
        from telegram_rag_memory_bot.utils.dates import today_iso

        return today_iso()
