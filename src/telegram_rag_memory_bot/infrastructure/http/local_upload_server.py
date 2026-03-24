"""
????: local_upload_server.py
????????? ????????? HTTP-???????: ???? ?? ??????, ???????? ??????????,
??????, ?????????, ???????, ?????????? ? ?????????? ????????.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from html import escape
import logging
from pathlib import Path
import re
import secrets
from urllib.parse import urlencode
from uuid import uuid4

from aiohttp import web

from telegram_rag_memory_bot.application.platform_service import PlatformAssistantService
from telegram_rag_memory_bot.config import PROJECT_ROOT, Settings
from telegram_rag_memory_bot.domain.models import LocalUploadRequest, SenderProfile
from telegram_rag_memory_bot.domain.ports import NotificationGateway, StorageGateway
from telegram_rag_memory_bot.utils.dates import format_display_date, parse_iso_date
from telegram_rag_memory_bot.utils.security import hash_password

LOGGER = logging.getLogger(__name__)
FAVICON_PATH = PROJECT_ROOT / "mobile_app" / "assets" / "favicon.png"


@dataclass(slots=True)
class AdminPlatformContext:
    slug: str
    title: str
    app_service: PlatformAssistantService
    notification_gateway: NotificationGateway
    upload_dir: Path
    command_dir: Path
    answer_dir: Path

    @property
    def base_path(self) -> str:
        return f"/{self.slug}/admin"

    @property
    def bot_label(self) -> str:
        return "Telegram-бот" if self.slug == "telegram" else "VK-бот"

    @property
    def storage_hint(self) -> str:
        if self.slug == "vk":
            return "Материалы и оригиналы хранятся в Telegram-группе, а VK-бот использует их для ответов и пересылки."
        return "Материалы и оригиналы хранятся в Telegram-группе и сразу доступны Telegram-боту."


class LocalUploadServer:
    SESSION_COOKIE_NAME = "rag_admin_session"

    def __init__(
        self,
        settings: Settings,
        platform_services: dict[str, PlatformAssistantService],
        storage_gateway: StorageGateway,
        notification_gateways: dict[str, NotificationGateway],
    ) -> None:
        self.settings = settings
        self.storage_gateway = storage_gateway
        self.platforms = self._build_platform_contexts(platform_services, notification_gateways)
        self._site_sessions: set[str] = set()
        self.web_app = web.Application(client_max_size=0)
        self.web_app.add_routes(
            [
                web.get("/", self._handle_root),
                web.get("/admin", self._handle_root),
                web.get("/upload", self._handle_root),
                web.post("/login", self._handle_login),
                web.post("/logout", self._handle_logout),
                web.get("/health", self._handle_health),
                web.get("/favicon.ico", self._handle_favicon),
                web.get("/site", self._handle_site_redirect),
                web.get("/site/admin", self._handle_site_dashboard),
                web.post("/site/admin/site-accounts/create", self._handle_site_admin_account_create),
                web.post("/site/admin/site-accounts/delete", self._handle_site_admin_account_delete),
                web.post("/site/admin/support/reply", self._handle_site_admin_support_reply),
                web.get("/{platform}", self._handle_platform_redirect),
                web.get("/{platform}/admin", self._handle_dashboard),
                web.post("/{platform}/admin/shifts/save", self._handle_shift_save),
                web.post("/{platform}/admin/shifts/delete", self._handle_shift_delete),
                web.post("/{platform}/admin/materials/create", self._handle_material_create),
                web.post("/{platform}/admin/materials/pending/delete", self._handle_pending_material_delete),
                web.post("/{platform}/admin/materials/delete", self._handle_material_delete),
                web.post("/{platform}/admin/promocodes/create", self._handle_promo_create),
                web.post("/{platform}/admin/promocodes/delete", self._handle_promo_delete),
                web.post("/{platform}/admin/commands/create", self._handle_command_create),
                web.post("/{platform}/admin/commands/delete", self._handle_command_delete),
                web.post("/{platform}/admin/answers/create", self._handle_answer_create),
                web.post("/{platform}/admin/answers/delete", self._handle_answer_delete),
                web.post("/{platform}/admin/requests/review", self._handle_request_review),
                web.post("/{platform}/admin/department-credits/create", self._handle_department_credit_create),
                web.post("/{platform}/admin/site-accounts/create", self._handle_site_account_create),
                web.post("/{platform}/admin/site-accounts/delete", self._handle_site_account_delete),
                web.post("/{platform}/admin/bans/create", self._handle_ban_create),
                web.post("/{platform}/admin/bans/delete", self._handle_ban_delete),
            ]
        )
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        if not self.settings.local_upload_enabled:
            LOGGER.info("Local upload server is disabled.")
            return
        self.runner = web.AppRunner(self.web_app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self.settings.local_upload_host, port=self.settings.local_upload_port)
        await self.site.start()
        LOGGER.info("Local upload server started on %s", self.settings.local_upload_base_url)
        try:
            await self._stop_event.wait()
        except asyncio.CancelledError:
            raise
        finally:
            await self.close()

    async def close(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()
            self.runner = None

    async def _handle_health(self, request: web.Request) -> web.Response:
        return web.json_response(
            {
                "ok": True,
                "base_url": self.settings.local_upload_base_url,
                "platforms": sorted(self.platforms.keys()),
            }
        )

    async def _handle_favicon(self, request: web.Request) -> web.Response:
        if FAVICON_PATH.exists():
            return web.FileResponse(path=Path(FAVICON_PATH))
        return web.Response(status=204)

    async def _handle_root(self, request: web.Request) -> web.Response:
        if not self._is_site_authenticated(request):
            return web.Response(
                text=self._render_login_page(next_path=request.path_qs or "/"),
                content_type="text/html",
            )
        return web.Response(
            text=self._render_root_selector_safe(notice_text="", error_text=""),
            content_type="text/html",
        )

    @staticmethod
    def _site_admin_base_path() -> str:
        return "/site/admin"

    def _site_admin_service(self) -> PlatformAssistantService:
        ordered_platforms = self._ordered_platforms()
        if not ordered_platforms:
            raise RuntimeError("Для раздела сайта не найден сервис данных.")
        return ordered_platforms[0].app_service

    async def _handle_site_redirect(self, request: web.Request) -> web.StreamResponse:
        raise web.HTTPFound(self._site_admin_base_path())

    async def _handle_site_dashboard(self, request: web.Request) -> web.Response:
        if not self._is_site_authenticated(request):
            return web.Response(
                text=self._render_login_page(next_path=request.path_qs or self._site_admin_base_path()),
                content_type="text/html",
            )
        return web.Response(
            text=self._render_site_dashboard(notice_text="", error_text=""),
            content_type="text/html",
        )

    async def _handle_platform_redirect(self, request: web.Request) -> web.StreamResponse:
        context = self._platform_context(request)
        suffix = f"?{request.query_string}" if request.query_string else ""
        raise web.HTTPFound(f"{context.base_path}{suffix}")

    async def _handle_dashboard(self, request: web.Request) -> web.Response:
        if not self._is_site_authenticated(request):
            return web.Response(
                text=self._render_login_page(next_path=request.path_qs or "/"),
                content_type="text/html",
            )
        context = self._platform_context(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        access_granted = self._can_access(context, admin_user_id, token)
        error_text = ""
        if not access_granted:
            error_text = "В .env не найден подходящий admin id для этой панели."
        return web.Response(
            text=self._render_dashboard(
                context,
                admin_user_id=admin_user_id,
                token=token,
                notice_text="",
                error_text=error_text,
                access_granted=access_granted,
            ),
            content_type="text/html",
        )

    async def _handle_shift_save(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            shift_id = self._parse_int(fields.get("shift_id", ""))
            name = fields.get("name", "").strip()
            date_from = parse_iso_date(fields.get("date_from", "").strip())
            date_to = parse_iso_date(fields.get("date_to", "").strip())
            if shift_id > 0:
                updated = context.app_service.update_shift(shift_id, name=name, date_from=date_from, date_to=date_to)
                notice_text = f"Смена #{shift_id} {'обновлена' if updated else 'не найдена'}."
            else:
                created_id = context.app_service.create_shift(name=name, date_from=date_from, date_to=date_to)
                notice_text = f"Смена #{created_id} сохранена."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice_text,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_shift_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            shift_id = self._parse_int(fields.get("shift_id", ""))
            deleted = context.app_service.delete_shift(shift_id)
            notice_text = f"Смена #{shift_id} {'удалена' if deleted else 'не найдена'}."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice_text,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_login(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        password = fields.get("password", "").strip()
        next_path = fields.get("next", "").strip() or "/"
        if not self._check_site_password(password):
            return web.Response(
                text=self._render_login_page(next_path=next_path, error_text="Неверный пароль для входа на сайт."),
                content_type="text/html",
                status=403,
            )
        session_id = secrets.token_urlsafe(32)
        self._site_sessions.add(session_id)
        response = web.HTTPFound(next_path)
        response.set_cookie(self.SESSION_COOKIE_NAME, session_id, httponly=True, samesite="Lax")
        raise response

    async def _handle_logout(self, request: web.Request) -> web.StreamResponse:
        session_id = request.cookies.get(self.SESSION_COOKIE_NAME, "").strip()
        if session_id:
            self._site_sessions.discard(session_id)
        response = web.HTTPFound("/")
        response.del_cookie(self.SESSION_COOKIE_NAME)
        raise response

    async def _handle_material_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        saved_path: Path | None = None
        fields: dict[str, str] = {}
        pending_id = 0
        try:
            fields, files = await self._read_multipart_fields(request, {"file": context.upload_dir})
            admin_user_id = self._default_admin_user_id(context)
            token = self._default_upload_token()
            self._ensure_access(context, admin_user_id, token)
            saved_path, file_name = files.get("file", (None, None))
            source_text = fields.get("source_text", "")
            validated = context.app_service.validate_upload_request(
                fields.get("content_date", ""),
                fields.get("description", ""),
                admin_user_id,
                fields.get("content_scope", "dated"),
            )
            upload_request = LocalUploadRequest(
                admin_user_id=validated.admin_user_id,
                content_date=validated.content_date,
                description=validated.description,
                source_text=source_text,
                local_file_path=saved_path,
                original_file_name=file_name,
                content_scope=validated.content_scope,
            )
            LOGGER.info(
                "Local upload form: запрос получен | platform=%s | admin=%s | дата=%s | scope=%s | файл=%s",
                context.slug,
                admin_user_id,
                validated.content_date or "-",
                validated.content_scope,
                file_name or "текст без файла",
            )
            pending_id = context.app_service.create_pending_material_upload(upload_request)
            analyzed_result = None
            analyze_now = bool(saved_path is not None or source_text.strip())
            if analyze_now:
                analyzed_result = await context.app_service.analyze_pending_material_upload(pending_id, upload_request)
                saved_path = None
            context.app_service.log_event(
                user_id=admin_user_id,
                chat_id=admin_user_id,
                event_type="manual_add_pending",
                sender_profile=SenderProfile(),
                details={
                    "pending_id": pending_id,
                    "content_scope": validated.content_scope,
                    "via": "localhost-admin",
                    "uploaded_file_name": file_name or "",
                },
            )
            if analyzed_result is not None:
                notice = (
                    f"Заявка #{pending_id} сохранена и уже проиндексирована как материал #{analyzed_result.item_id}."
                    + " Следующее сообщение администратора в Telegram-группе хранения будет запомнено как оригинал для пересылки."
                )
            else:
                notice = (
                    f"Заявка #{pending_id} сохранена."
                    + (f" Файл {file_name} загружен на сайт." if file_name else " Файл на сайте не загружен.")
                    + " ИИ начнет обработку после следующего сообщения администратора в Telegram-группе хранения,"
                    + " а это же сообщение станет оригиналом для будущей пересылки."
                )
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            admin_user_id = self._default_admin_user_id(context)
            token = self._default_upload_token()
            if pending_id > 0:
                try:
                    context.app_service.delete_pending_material_upload(pending_id)
                except Exception:
                    LOGGER.exception("Failed to rollback pending localhost upload %s", pending_id)
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )
        finally:
            if saved_path is not None and saved_path.exists():
                saved_path.unlink(missing_ok=True)

    async def _handle_pending_material_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            pending_id = self._parse_int(fields.get("pending_id", ""))
            if pending_id <= 0:
                raise ValueError("Нужно указать корректный ID ожидающей заявки.")
            pending_row = next((row for row in context.app_service.list_pending_material_uploads(limit=500) if int(row.get("id") or 0) == pending_id), None)
            deleted = context.app_service.delete_pending_material_upload(pending_id)
            if deleted and pending_row is not None:
                pending_file_path = str(pending_row.get("local_file_path") or "").strip()
                if pending_file_path:
                    Path(pending_file_path).unlink(missing_ok=True)
            notice = f"Ожидающая заявка #{pending_id} {'удалена' if deleted else 'не найдена или уже обработана'}."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_material_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            item_id = self._parse_int(fields.get("item_id", ""))
            if item_id <= 0:
                raise ValueError("Нужно указать корректный ITEM_ID.")
            deleted = context.app_service.delete_item(item_id)
            notice = f"Материал #{item_id} {'удален из индекса' if deleted else 'не найден'}."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_promo_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            code = fields.get("code", "").strip()
            bonus_requests = self._parse_int(fields.get("bonus_requests", ""))
            if not code:
                raise ValueError("Промокод обязателен.")
            if bonus_requests <= 0:
                raise ValueError("Бонусных запросов должно быть больше нуля.")
            expires_at = fields.get("expires_at", "").strip() or None
            if expires_at:
                expires_at = parse_iso_date(expires_at)
            context.app_service.create_promo_code(
                code,
                bonus_requests=bonus_requests,
                note=fields.get("note", ""),
                max_redemptions=self._parse_optional_int(fields.get("max_redemptions", "")),
                expires_at=expires_at,
                enabled=self._is_checked(fields.get("enabled"), default=True),
            )
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=f"Промокод {code.upper()} сохранен.",
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_promo_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            code = fields.get("code", "").strip()
            deleted = context.app_service.delete_promo_code(code)
            notice = f"Промокод {code.upper()} {'удален' if deleted else 'не найден'}."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_command_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields, files = await self._read_multipart_fields(request, {"media_file": context.command_dir})
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            command_name = self._normalize_command_name(fields.get("command_name", ""))
            response_text = fields.get("response_text", "").strip()
            saved_path, _file_name = files.get("media_file", (None, None))
            media_path = str(saved_path) if saved_path is not None else self._existing_command_media_path(context, command_name)
            if not command_name:
                raise ValueError("Команда обязательна.")
            if self._is_reserved_command(command_name):
                raise ValueError("Эта команда зарезервирована системным ботом. Используйте другое имя.")
            if not response_text and not media_path:
                raise ValueError("Для команды нужен текст ответа или медиафайл.")
            context.app_service.create_custom_command(
                command_name,
                response_text=response_text,
                media_path=media_path,
                notify_admin=self._is_checked(fields.get("notify_admin"), default=True),
                enabled=self._is_checked(fields.get("enabled"), default=True),
            )
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=f"Команда {command_name} сохранена.",
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_command_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            command_name = self._normalize_command_name(fields.get("command_name", ""))
            deleted = context.app_service.delete_custom_command(command_name)
            notice = f"Команда {command_name} {'удалена' if deleted else 'не найдена'}."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_answer_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields, files = await self._read_multipart_fields(request, {"media_file": context.answer_dir})
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            trigger_text = fields.get("trigger_text", "").strip()
            option_label = fields.get("option_label", "").strip()
            response_text = fields.get("response_text", "").strip()
            saved_path, _file_name = files.get("media_file", (None, None))
            media_path = str(saved_path) if saved_path is not None else None
            if not trigger_text:
                raise ValueError("Триггер обязателен.")
            if not option_label:
                raise ValueError("Название варианта обязательно.")
            if not response_text and not media_path:
                raise ValueError("Для варианта нужен текст или медиафайл.")
            option_id = context.app_service.create_managed_answer_option(
                trigger_text=trigger_text,
                match_mode=fields.get("match_mode", "exact"),
                option_label=option_label,
                response_text=response_text,
                media_path=media_path,
                sort_order=max(self._parse_int(fields.get("sort_order", "100")), 0),
                enabled=self._is_checked(fields.get("enabled"), default=True),
            )
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=f"Вариант ответа #{option_id} сохранен.",
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_answer_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            option_id = self._parse_int(fields.get("option_id", ""))
            if option_id <= 0:
                raise ValueError("Нужен корректный ID варианта.")
            deleted = context.app_service.delete_managed_answer_option(option_id)
            notice = f"Вариант #{option_id} {'удален' if deleted else 'не найден'}."
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text=notice,
                    error_text="",
                    access_granted=True,
                ),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(
                    context,
                    admin_user_id=admin_user_id,
                    token=token,
                    notice_text="",
                    error_text=str(exc),
                    access_granted=self._can_access(context, admin_user_id, token),
                ),
                content_type="text/html",
                status=400,
            )

    async def _handle_request_review(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            request_id = self._parse_int(fields.get("request_id", ""))
            status = fields.get("status", "").strip().lower()
            if request_id <= 0:
                raise ValueError("Нужен корректный ID заявки.")
            if status not in {"approved", "rejected"}:
                raise ValueError("Статус заявки должен быть approved или rejected.")
            reviewed = context.app_service.review_access_request(
                request_id,
                status=status,
                reviewed_by_user_id=admin_user_id,
                decision_note=fields.get("decision_note", ""),
                granted_bonus_requests=max(self._parse_int(fields.get("granted_bonus_requests", "0")), 0),
                granted_mode_credits=max(self._parse_int(fields.get("granted_mode_credits", "0")), 0),
            )
            if reviewed is None:
                raise ValueError("Заявка не найдена.")
            notice = f"Заявка #{request_id} обработана: {status}."
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text=notice, error_text="", access_granted=True),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text="", error_text=str(exc), access_granted=self._can_access(context, admin_user_id, token)),
                content_type="text/html",
                status=400,
            )

    async def _handle_department_credit_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            target_user_id = self._parse_int(fields.get("target_user_id", ""))
            credits = max(self._parse_int(fields.get("credits", "0")), 0)
            if target_user_id <= 0:
                raise ValueError("Нужен корректный user id.")
            department, _bucket = context.app_service.grant_department_special_requests(target_user_id, credits)
            notice = f"Пользователю {target_user_id} добавлено {credits} спец-запросов для департамента: {department}."
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text=notice, error_text="", access_granted=True),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text="", error_text=str(exc), access_granted=self._can_access(context, admin_user_id, token)),
                content_type="text/html",
                status=400,
            )

    async def _handle_site_account_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            username = fields.get("username", "").strip().lower()
            password = fields.get("password", "").strip()
            display_name = fields.get("display_name", "").strip()
            is_active = self._is_checked(fields.get("is_active"), default=True)
            if not username:
                raise ValueError("Нужен логин сайта.")
            if not password:
                raise ValueError("Нужен пароль сайта.")
            existing = context.app_service.get_site_account_any(username)
            site_user_id = int(existing.get("platform_user_id") or 0) if existing else context.app_service.next_site_platform_user_id()
            context.app_service.upsert_site_account(
                username=username,
                password_hash=hash_password(password),
                display_name=display_name,
                platform_user_id=site_user_id,
                is_active=is_active,
            )
            notice = f"Сайт-аккаунт {username} сохранен. Внутренний ID сайта: {site_user_id}."
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text=notice, error_text="", access_granted=True),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text="", error_text=str(exc), access_granted=self._can_access(context, admin_user_id, token)),
                content_type="text/html",
                status=400,
            )

    async def _handle_site_account_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            username = fields.get("username", "").strip().lower()
            if not username:
                raise ValueError("Нужен логин сайта.")
            deleted = context.app_service.deactivate_site_account(username)
            notice = f"Сайт-аккаунт {username} {'отключен' if deleted else 'не найден'}."
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text=notice, error_text="", access_granted=True),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text="", error_text=str(exc), access_granted=self._can_access(context, admin_user_id, token)),
                content_type="text/html",
                status=400,
            )

    async def _handle_site_admin_account_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        fields = await self._read_simple_fields(request)
        try:
            username, site_user_id = self._save_site_account_from_fields(self._site_admin_service(), fields)
            notice = f"Сайт-аккаунт {username} сохранен. Внутренний ID сайта: {site_user_id}."
            return web.Response(
                text=self._render_site_dashboard(notice_text=notice, error_text=""),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_site_dashboard(notice_text="", error_text=str(exc)),
                content_type="text/html",
                status=400,
            )

    async def _handle_site_admin_account_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        fields = await self._read_simple_fields(request)
        try:
            username, deleted = self._delete_site_account_from_fields(self._site_admin_service(), fields)
            notice = f"Сайт-аккаунт {username} {'отключен' if deleted else 'не найден'}."
            return web.Response(
                text=self._render_site_dashboard(notice_text=notice, error_text=""),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_site_dashboard(notice_text="", error_text=str(exc)),
                content_type="text/html",
                status=400,
            )

    async def _handle_site_admin_support_reply(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        fields = await self._read_simple_fields(request)
        service = self._site_admin_service()
        try:
            username = fields.get("username", "").strip().lower()
            display_name = fields.get("display_name", "").strip()
            message_text = fields.get("message_text", "").strip()
            site_user_id = self._parse_int(fields.get("site_user_id", ""))
            if not username:
                raise ValueError("Нужен логин сайта, чтобы ответить пользователю.")
            if not message_text:
                raise ValueError("Введите текст ответа для пользователя сайта.")
            service.create_site_support_message(
                username=username,
                site_user_id=site_user_id,
                display_name=display_name or username,
                sender_role="admin",
                message_text=message_text,
            )
            service.mark_site_support_read_by_admin(username)
            return web.Response(
                text=self._render_site_dashboard(notice_text=f"Ответ для {username} сохранен.", error_text=""),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_site_dashboard(notice_text="", error_text=str(exc)),
                content_type="text/html",
                status=400,
            )

    async def _handle_ban_create(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            target_user_id = self._parse_int(fields.get("target_user_id", ""))
            if target_user_id <= 0:
                raise ValueError("Нужен корректный user id.")
            context.app_service.set_ban(target_user_id, reason=fields.get("reason", ""), banned_by_user_id=admin_user_id)
            notice = f"Пользователь {target_user_id} заблокирован."
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text=notice, error_text="", access_granted=True),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text="", error_text=str(exc), access_granted=self._can_access(context, admin_user_id, token)),
                content_type="text/html",
                status=400,
            )

    async def _handle_ban_delete(self, request: web.Request) -> web.Response:
        self._ensure_site_authenticated(request)
        context = self._platform_context(request)
        fields = await self._read_simple_fields(request)
        admin_user_id = self._default_admin_user_id(context)
        token = self._default_upload_token()
        try:
            self._ensure_access(context, admin_user_id, token)
            target_user_id = self._parse_int(fields.get("target_user_id", ""))
            if target_user_id <= 0:
                raise ValueError("Нужен корректный user id.")
            deleted = context.app_service.clear_ban(target_user_id)
            notice = f"Пользователь {target_user_id} {'разблокирован' if deleted else 'не найден в бан-листе'}."
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text=notice, error_text="", access_granted=True),
                content_type="text/html",
            )
        except Exception as exc:
            return web.Response(
                text=self._render_dashboard(context, admin_user_id=admin_user_id, token=token, notice_text="", error_text=str(exc), access_granted=self._can_access(context, admin_user_id, token)),
                content_type="text/html",
                status=400,
            )

    def _render_root_selector(self, *, notice_text: str, error_text: str) -> str:
        return self._render_root_selector_safe(notice_text=notice_text, error_text=error_text)

    def _render_root_selector_safe(self, *, notice_text: str, error_text: str) -> str:
        notice_html = f'<div class="banner ok">{escape(notice_text)}</div>' if notice_text else ""
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        platform_cards = "".join(self._platform_entry_card(context) for context in self._ordered_platforms()) + self._site_entry_card()
        platform_count = len(self._ordered_platforms()) + 1
        return (
            f"{self._head_html('RAG Admin')}"
            "<body class=\"shell shell-home\"><main class=\"page\">"
            "<section class=\"hero hero-home\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Локальная админка</span>"
            "<h1>RAG Admin</h1>"
            "<p class=\"lead\">Единая панель для управления Telegram и VK: материалы, смены, промокоды, заявки, команды и статистика в одном месте.</p>"
            "<p class=\"hint\">Если открыть корень без path, сайт показывает выбор между Telegram и VK. Общие сущности синхронизированы между платформами, а пользовательская статистика и команды разделены.</p>"
            "<div class=\"meta-grid\">"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Платформы</span><strong class=\"meta-value\">{platform_count}</strong><span class=\"meta-note\">Telegram и VK доступны из одной панели.</span></div>"
            "<div class=\"meta-card\"><span class=\"meta-label\">Единое ядро</span><strong class=\"meta-value\">RAG + админка</strong><span class=\"meta-note\">Материалы, промокоды и готовые ответы управляются централизованно.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Пользовательский сайт</span><strong class=\"meta-value\">{escape(self.settings.public_web_base_url)}</strong><span class=\"meta-note\">Отдельный красивый сайт для пользователей работает на другом порту, но использует те же данные, что и боты.</span></div>"
            "</div>"
            "</div>"
            "<div class=\"hero-side\">"
            "<span class=\"eyebrow\">Быстрый старт</span>"
            "<h2>Открой нужную платформу</h2>"
            "<p class=\"hint\">Сначала выбери раздел, затем уже внутри админки можно создавать смены, загружать материалы и разбирать заявки.</p>"
            "<ul class=\"feature-list\"><li>Современная локальная админка без лишних полей доступа.</li><li>Быстрый переход между Telegram и VK.</li><li>Единый визуальный язык для всех рабочих разделов.</li></ul>"
            "<form method=\"post\" action=\"/logout\" class=\"inline-form\"><button type=\"submit\" class=\"ghost\">Выйти</button></form>"
            "</div>"
            "</section>"
            f"{notice_html}{error_html}"
            f"<section class=\"switch-grid\">{platform_cards}</section>"
            "</main></body></html>"
        )

    def _render_login_page(self, *, next_path: str, error_text: str = "") -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        return (
            f"{self._head_html('Вход в RAG Admin')}"
            "<body class=\"shell shell-auth\"><main class=\"auth-page\">"
            "<section class=\"auth-card\">"
            "<span class=\"eyebrow\">Защищенный вход</span>"
            "<h1>Вход в RAG Admin</h1>"
            "<p class=\"lead\">Админка работает локально и открывается только после ввода пароля.</p>"
            "<p class=\"hint\">После входа можно перейти в Telegram Admin или VK Admin и управлять материалами, сменами, заявками и статистикой.</p>"
            f"{error_html}"
            "<form method=\"post\" action=\"/login\" class=\"grid two\">"
            f"<input type=\"hidden\" name=\"next\" value=\"{escape(next_path)}\">"
            "<label>Пароль<input type=\"password\" name=\"password\" required autofocus></label>"
            "<div class=\"full\"><button type=\"submit\">Открыть админку</button></div>"
            "</form></section></main></body></html>"
        )

    def _render_dashboard(
        self,
        context: AdminPlatformContext,
        *,
        admin_user_id: int,
        token: str,
        notice_text: str,
        error_text: str,
        access_granted: bool,
    ) -> str:
        access_hidden = self._access_hidden_fields()
        notice_html = f'<div class="banner ok">{escape(notice_text)}</div>' if notice_text else ""
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        switch_nav = self._platform_switch_nav(current_slug=context.slug)
        if not access_granted:
            return (
                f"{self._head_html(context.title)}"
                "<body class=\"shell\"><main class=\"page\">"
                "<section class=\"hero hero-dashboard\">"
                "<div class=\"hero-copy\">"
                f"<span class=\"eyebrow\">{escape(context.bot_label)}</span>"
                f"<h1>{escape(context.title)}</h1>"
                "<p class=\"lead\">Панель не может автоматически подтвердить права администратора.</p>"
                f"<p class=\"hint\">Не удалось автоматически определить администратора для {escape(context.bot_label.lower())}. Проверьте `UPLOADER_USER_IDS` или `VK_UPLOADER_USER_IDS` в `.env`.</p>"
                "</div>"
                "<div class=\"hero-side\">"
                "<span class=\"eyebrow\">Что проверить</span>"
                "<h2>Доступ к панели</h2>"
                "<ul class=\"feature-list\"><li>Укажи корректный ID администратора в `.env`.</li><li>Проверь, что бот запущен с актуальными настройками.</li><li>После изменения `.env` перезапусти процесс.</li></ul>"
                "</div>"
                "</section>"
                f"<div class=\"toolbar\">{switch_nav}</div>"
                f"{notice_html}{error_html}</main></body></html>"
            )

        shifts = context.app_service.list_shifts(limit=240)
        promos = context.app_service.list_promo_codes()
        commands = context.app_service.list_custom_commands()
        answers = context.app_service.list_managed_answer_options()
        pending_uploads = context.app_service.list_pending_material_uploads(limit=120)
        items = context.app_service.list_recent_items(120)
        users = context.app_service.get_user_statistics("")[:120]
        site_accounts = context.app_service.list_site_accounts(limit=300)
        requests = context.app_service.list_access_requests(limit=120)
        bans = context.app_service.list_banned_users(limit=120)
        recent_events = context.app_service.list_user_events(limit=180)
        shift_rows = "".join(self._shift_row(row, access_hidden, context.base_path) for row in shifts) or '<tr><td colspan="6">Смен пока нет.</td></tr>'
        promo_rows = "".join(self._promo_row(row, access_hidden, context.base_path) for row in promos) or '<tr><td colspan="8">Промокодов пока нет.</td></tr>'
        command_rows = "".join(self._command_row(row, access_hidden, context.base_path) for row in commands) or '<tr><td colspan="7">Кастомных команд пока нет.</td></tr>'
        answer_rows = "".join(self._answer_row(row, access_hidden, context.base_path) for row in answers) or '<tr><td colspan="9">Готовых вариантов пока нет.</td></tr>'
        pending_rows = "".join(self._pending_material_row(context, row, access_hidden, context.base_path) for row in pending_uploads) or '<tr><td colspan="8">Ожидающих заявок пока нет.</td></tr>'
        item_rows = "".join(self._item_row(context, row, access_hidden, context.base_path) for row in items) or '<tr><td colspan="7">Материалов пока нет.</td></tr>'
        user_rows = "".join(self._user_row(context, row) for row in users) or '<tr><td colspan="14">Статистика пока пуста.</td></tr>'
        site_account_rows = "".join(self._site_account_row(row, access_hidden, context.base_path) for row in site_accounts) or '<tr><td colspan="6">Сайт-аккаунтов пока нет.</td></tr>'
        request_rows = "".join(self._request_row(row, access_hidden, context.base_path) for row in requests) or '<tr><td colspan="10">Заявок пока нет.</td></tr>'
        ban_rows = "".join(self._ban_row(row, access_hidden, context.base_path) for row in bans) or '<tr><td colspan="6">Активных банов пока нет.</td></tr>'
        event_rows = "".join(self._event_row(context, row) for row in recent_events) or '<tr><td colspan="7">Событий пока нет.</td></tr>'
        overview_tiles = (
            "<div class=\"meta-grid\">"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Смены</span><strong class=\"meta-value\">{len(shifts)}</strong><span class=\"meta-note\">Диапазоны дат для сюжетов и материалов.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Материалы</span><strong class=\"meta-value\">{len(items)}</strong><span class=\"meta-note\">Показаны последние записи из индекса.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Ожидание</span><strong class=\"meta-value\">{len(pending_uploads)}</strong><span class=\"meta-note\">Заявки ждут сообщение из группы.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Заявки</span><strong class=\"meta-value\">{len(requests)}</strong><span class=\"meta-note\">Запросы пользователей на доступ и лимиты.</span></div>"
            "</div>"
        )
        department_credit_section = (
            f"<section id=\"department-credits\" class=\"card\"><h2>Добавить спец-запросы департамента</h2>"
            "<p class=\"hint\">Базовый лимит департамента обновляется каждый день и равен 1 спец-запросу. Здесь можно добавить дополнительные спец-запросы конкретному пользователю.</p>"
            f"<form method=\"post\" action=\"{context.base_path}/department-credits/create\" class=\"grid three\">{access_hidden}"
            "<label>User ID<input name=\"target_user_id\" required></label>"
            "<label>Дополнительные спец-запросы<input name=\"credits\" type=\"number\" min=\"1\" value=\"1\" required></label>"
            "<div><button type=\"submit\">Добавить</button></div></form></section>"
        )
        site_account_section = (
            f"<section id=\"site-accounts\" class=\"card\"><h2>Сайт-аккаунты {escape(context.bot_label)}</h2>"
            "<p class=\"hint\">Здесь создаются отдельные логин и пароль для пользовательского сайта. Общей с ботами остается только память материалов, а аккаунты сайта, их лимиты, настройки и история живут отдельно.</p>"
            f"<form method=\"post\" action=\"{context.base_path}/site-accounts/create\" class=\"grid three\">{access_hidden}"
            "<label>Логин сайта<input name=\"username\" placeholder=\"например user_web\" required></label>"
            "<label>Пароль сайта<input type=\"password\" name=\"password\" required></label>"
            "<label class=\"full\">Отображаемое имя<input name=\"display_name\" placeholder=\"Как подписывать пользователя на сайте\"></label>"
            "<label><input type=\"checkbox\" name=\"is_active\" checked> Аккаунт активен</label>"
            "<div><button type=\"submit\">Сохранить сайт-аккаунт</button></div></form>"
            f"<div class=\"table\"><table><thead><tr><th>Логин</th><th>Имя</th><th>Site ID</th><th>Активен</th><th>Обновлен</th><th></th></tr></thead><tbody>{site_account_rows}</tbody></table></div></section>"
        )
        shift_section = (
            f"<section id=\"shifts\" class=\"card\"><h2>Смены</h2>"
            "<p class=\"hint\">Смены задают именованные диапазоны дат, например <code>1-11 июля 2025 года</code>. Все материалы с датой автоматически относятся к нужной смене по своей дате, поэтому после изменения диапазона привязка обновится сама.</p>"
            f"<form method=\"post\" action=\"{context.base_path}/shifts/save\" class=\"grid three\">{access_hidden}"
            "<input type=\"hidden\" name=\"shift_id\" value=\"\">"
            "<label>Название (необязательно)<input name=\"name\" placeholder=\"1-11 июля 2025 года\"></label>"
            "<label>Дата начала<input name=\"date_from\" placeholder=\"01-07-2025\" required></label>"
            "<label>Дата конца<input name=\"date_to\" placeholder=\"11-07-2025\" required></label>"
            "<div><button type=\"submit\">Создать смену</button></div></form>"
            f"<div class=\"table\"><table><thead><tr><th>ID</th><th>Название</th><th>Дата начала</th><th>Дата конца</th><th>Обновлено</th><th></th></tr></thead><tbody>{shift_rows}</tbody></table></div></section>"
        )
        return (
            f"{self._head_html(context.title)}"
            "<body class=\"shell\"><main class=\"page\">"
            "<section class=\"hero hero-dashboard\">"
            "<div class=\"hero-copy\">"
            f"<span class=\"eyebrow\">{escape(context.bot_label)}</span>"
            f"<h1>{escape(context.title)}</h1>"
            f"<p class=\"lead\">{escape(context.storage_hint)}</p>"
            "<p class=\"hint\">Промокоды, индекс материалов и готовые ответы общие для обеих платформ. Статистика пользователей и кастомные команды показаны только для текущей платформы.</p>"
            "</div>"
            f"<div class=\"hero-side\">{overview_tiles}</div>"
            "</section>"
            f"<div class=\"toolbar\">{switch_nav}<form method=\"post\" action=\"/logout\" class=\"inline-form\"><button type=\"submit\" class=\"ghost\">Выйти</button></form></div>"
            f"{notice_html}{error_html}"
            "<nav class=\"nav\" aria-label=\"Разделы панели\"><a href=\"#materials\">Материалы</a><a href=\"#pending-materials\">Ожидание группы</a><a href=\"#shifts\">Смены</a><a href=\"#promos\">Промокоды</a><a href=\"#commands\">Команды</a><a href=\"#answers\">Готовые ответы</a><a href=\"#requests\">Заявки</a><a href=\"#bans\">Баны</a><a href=\"#items\">Индекс</a><a href=\"#users\">Пользователи</a><a href=\"#events\">События</a></nav>"
            f"{department_credit_section}"
            f"{shift_section}"
            f"<section id=\"materials\" class=\"card\"><h2>Добавить материал</h2><p class=\"hint\">Можно загрузить файл прямо на сайте или оставить форму без файла. Загруженный на сайте файл используется для анализа, а следующее сообщение администратора в Telegram-группе хранения становится оригиналом для последующей пересылки пользователям.</p><p class=\"hint\">Если у материала указана дата, он автоматически попадет в подходящую смену по диапазону дат. Категория <code>Компания / без даты</code> подходит для объектов без одной главной даты.</p><form method=\"post\" action=\"{context.base_path}/materials/create\" enctype=\"multipart/form-data\" class=\"grid two\">{access_hidden}<label>Категория<select name=\"content_scope\"><option value=\"dated\">Материал с датой</option><option value=\"timeless\">Компания / без даты</option></select></label><label>Дата (DD-MM-YYYY)<input name=\"content_date\" placeholder=\"01-07-2025\"></label><label class=\"full\">Описание<textarea name=\"description\" required></textarea></label><label class=\"full\">Исходный текст<textarea name=\"source_text\"></textarea></label><label class=\"full\">Файл для анализа (необязательно)<input type=\"file\" name=\"file\"></label><div class=\"full\"><button type=\"submit\">Сохранить заявку и ждать следующее сообщение группы</button></div></form></section>"
            f"<section id=\"pending-materials\" class=\"card\"><h2>Ожидающие заявки</h2><p class=\"hint\">Как только администратор вручную отправит следующее подходящее сообщение в Telegram-группу хранения, бот привяжет его к одной из этих заявок, запомнит для пересылки и проиндексирует материал.</p><div class=\"table\"><table><thead><tr><th>ID</th><th>Admin</th><th>Дата</th><th>Категория</th><th>Описание</th><th>Исходный текст</th><th>Файл сайта</th><th>Создана</th><th></th></tr></thead><tbody>{pending_rows}</tbody></table></div></section>"
            f"<section id=\"promos\" class=\"card\"><h2>Промокоды</h2><form method=\"post\" action=\"{context.base_path}/promocodes/create\" class=\"grid three\">{access_hidden}<label>Код<input name=\"code\" required></label><label>Бонусных запросов<input name=\"bonus_requests\" type=\"number\" min=\"1\" required></label><label>Лимит активаций<input name=\"max_redemptions\" type=\"number\" min=\"1\"></label><label>Истекает (DD-MM-YYYY)<input name=\"expires_at\"></label><label class=\"full\">Заметка<input name=\"note\"></label><label><input type=\"checkbox\" name=\"enabled\" checked> Включен</label><div><button type=\"submit\">Сохранить промокод</button></div></form><div class=\"table\"><table><thead><tr><th>Код</th><th>Бонус</th><th>Активаций</th><th>Истекает</th><th>Лимит</th><th>Вкл</th><th>Заметка</th><th></th></tr></thead><tbody>{promo_rows}</tbody></table></div></section>"
            f"<section id=\"commands\" class=\"card\"><h2>Кастомные команды {escape(context.bot_label)}</h2><form method=\"post\" action=\"{context.base_path}/commands/create\" enctype=\"multipart/form-data\" class=\"grid two\">{access_hidden}<label>Команда<input name=\"command_name\" placeholder=\"/HOMOSAP\" required></label><label>Медиафайл<input type=\"file\" name=\"media_file\"></label><label class=\"full\">Текст ответа<textarea name=\"response_text\"></textarea></label><label><input type=\"checkbox\" name=\"notify_admin\" checked> Уведомлять админов</label><label><input type=\"checkbox\" name=\"enabled\" checked> Включена</label><div class=\"full\"><button type=\"submit\">Сохранить команду</button></div></form><div class=\"table\"><table><thead><tr><th>Команда</th><th>Текст</th><th>Медиа</th><th>Notify</th><th>Вкл</th><th>Обновлено</th><th></th></tr></thead><tbody>{command_rows}</tbody></table></div></section>"
            f"<section id=\"answers\" class=\"card\"><h2>Готовые варианты ответа</h2><form method=\"post\" action=\"{context.base_path}/answers/create\" enctype=\"multipart/form-data\" class=\"grid three\">{access_hidden}<label>Триггер<input name=\"trigger_text\" required></label><label>Match<select name=\"match_mode\"><option value=\"exact\">exact</option><option value=\"contains\">contains</option></select></label><label>Название варианта<input name=\"option_label\" required></label><label>Порядок<input name=\"sort_order\" type=\"number\" min=\"0\" value=\"100\"></label><label>Медиафайл<input type=\"file\" name=\"media_file\"></label><label><input type=\"checkbox\" name=\"enabled\" checked> Включен</label><label class=\"full\">Текст ответа<textarea name=\"response_text\"></textarea></label><div class=\"full\"><button type=\"submit\">Добавить вариант</button></div></form><div class=\"table\"><table><thead><tr><th>ID</th><th>Триггер</th><th>Match</th><th>Вариант</th><th>Порядок</th><th>Текст</th><th>Медиа</th><th>Вкл</th><th></th></tr></thead><tbody>{answer_rows}</tbody></table></div></section>"
            f"<section id=\"requests\" class=\"card\"><h2>Заявки пользователей</h2><div class=\"table\"><table><thead><tr><th>ID</th><th>User ID</th><th>Тип</th><th>Имя</th><th>Причина</th><th>Mode bucket</th><th>Статус</th><th>Бонус</th><th>Mode credits</th><th></th></tr></thead><tbody>{request_rows}</tbody></table></div></section>"
            f"<section id=\"bans\" class=\"card\"><h2>Баны</h2><form method=\"post\" action=\"{context.base_path}/bans/create\" class=\"grid two\">{access_hidden}<label>User ID<input name=\"target_user_id\" required></label><label class=\"full\">Причина<textarea name=\"reason\"></textarea></label><div class=\"full\"><button type=\"submit\" class=\"danger\">Забанить</button></div></form><div class=\"table\"><table><thead><tr><th>User ID</th><th>Причина</th><th>Кто забанил</th><th>Создан</th><th>Обновлен</th><th></th></tr></thead><tbody>{ban_rows}</tbody></table></div></section>"
            f"<section id=\"items\" class=\"card\"><h2>Последние материалы в индексе</h2><p class=\"hint\">Удаление убирает материал из поиска, но не удаляет исходный файл из Telegram-группы хранения.</p><div class=\"table\"><table><thead><tr><th>ID</th><th>Дата</th><th>Категория</th><th>Тип</th><th>Файл</th><th>Кратко</th><th></th></tr></thead><tbody>{item_rows}</tbody></table></div></section>"
            f"<section id=\"users\" class=\"card\"><h2>Пользователи {escape(context.bot_label)}</h2><div class=\"table\"><table><thead><tr><th>User ID</th><th>Username</th><th>Имя</th><th>Департамент</th><th>Prompt</th><th>Risk</th><th>Бан</th><th>Бонус</th><th>Сегодня</th><th>Списано</th><th>Ask</th><th>Search</th><th>File</th><th>Последняя активность</th></tr></thead><tbody>{user_rows}</tbody></table></div></section>"
            f"<section id=\"events\" class=\"card\"><h2>Последние события</h2><div class=\"table\"><table><thead><tr><th>Время</th><th>User ID</th><th>Event</th><th>Risk</th><th>Username</th><th>Детали</th><th>Списано</th></tr></thead><tbody>{event_rows}</tbody></table></div></section></main></body></html>"
        )

    def _render_site_dashboard(self, *, notice_text: str, error_text: str) -> str:
        app_service = self._site_admin_service()
        access_hidden = self._access_hidden_fields()
        notice_html = f'<div class="banner ok">{escape(notice_text)}</div>' if notice_text else ""
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        switch_nav = self._platform_switch_nav(current_slug="site")
        site_accounts = app_service.list_site_accounts(limit=300)
        support_threads = app_service.list_site_support_threads(limit=80)
        site_account_rows = "".join(
            self._site_account_row(row, access_hidden, self._site_admin_base_path()) for row in site_accounts
        ) or '<tr><td colspan="6">Сайт-аккаунтов пока нет.</td></tr>'
        active_accounts = sum(1 for row in site_accounts if int(row.get("is_active") or 0))
        inactive_accounts = max(len(site_accounts) - active_accounts, 0)
        public_site_url = escape(self.settings.public_web_base_url)
        public_site_status = "включен" if getattr(self.settings, "public_web_enabled", False) else "выключен"
        unread_support = sum(max(int(row.get("unread_count") or 0), 0) for row in support_threads)
        site_account_section = (
            f"<section id=\"site-accounts\" class=\"card\"><h2>Сайт-аккаунты</h2>"
            "<p class=\"hint\">Здесь создаются отдельные логины и пароли для пользовательского сайта. Аккаунты сайта не связаны с Telegram/VK-пользователями, а общей остается только база материалов и память ответов.</p>"
            f"<form method=\"post\" action=\"{self._site_admin_base_path()}/site-accounts/create\" class=\"grid three\">{access_hidden}"
            "<label>Логин сайта<input name=\"username\" placeholder=\"например user_web\" required></label>"
            "<label>Пароль сайта<input type=\"password\" name=\"password\" required></label>"
            "<label class=\"full\">Отображаемое имя<input name=\"display_name\" placeholder=\"Как подписывать пользователя на сайте\"></label>"
            "<label><input type=\"checkbox\" name=\"is_active\" checked> Аккаунт активен</label>"
            "<div><button type=\"submit\">Сохранить сайт-аккаунт</button></div></form>"
            f"<div class=\"table\"><table><thead><tr><th>Логин</th><th>Имя</th><th>Site ID</th><th>Активен</th><th>Обновлен</th><th></th></tr></thead><tbody>{site_account_rows}</tbody></table></div></section>"
        )
        support_sections: list[str] = []
        for row in support_threads:
            username = str(row.get("username") or "").strip().lower()
            if not username:
                continue
            app_service.mark_site_support_read_by_admin(username)
            display_name = str(row.get("display_name") or "").strip() or username
            site_user_id = int(row.get("site_user_id") or 0)
            unread_count = max(int(row.get("unread_count") or 0), 0)
            last_message_at = escape(str(row.get("last_message_at") or "-"))
            messages = app_service.list_site_support_messages(username, limit=12)
            messages_html = "".join(
                (
                    f"<article class=\"support-message {'admin' if str(message.get('sender_role') or '') == 'admin' else 'user'}\">"
                    f"<div class=\"support-meta\"><strong>{escape(str(message.get('display_name') or username))}</strong>"
                    f"<span>{escape(str(message.get('created_at') or '-'))}</span></div>"
                    f"<p>{escape(str(message.get('message_text') or ''))}</p>"
                    "</article>"
                )
                for message in messages
            ) or '<p class="hint">Сообщений пока нет.</p>'
            support_sections.append(
                f"<section class=\"support-thread\">"
                f"<div class=\"support-thread-head\"><div><h3>{escape(display_name)}</h3><p class=\"hint\">Логин: <code>{escape(username)}</code> · Site ID: {site_user_id or '-'}</p></div>"
                f"<div class=\"support-badges\"><span class=\"meta-note\">Новых: {unread_count}</span><span class=\"meta-note\">Последнее: {last_message_at}</span></div></div>"
                f"<div class=\"support-log\">{messages_html}</div>"
                f"<form method=\"post\" action=\"{self._site_admin_base_path()}/support/reply\" class=\"grid two support-form\">"
                f"{access_hidden}"
                f"<input type=\"hidden\" name=\"username\" value=\"{escape(username)}\">"
                f"<input type=\"hidden\" name=\"display_name\" value=\"{escape(display_name)}\">"
                f"<input type=\"hidden\" name=\"site_user_id\" value=\"{site_user_id}\">"
                "<label class=\"full\">Ответ пользователю<textarea name=\"message_text\" rows=\"3\" placeholder=\"Напишите ответ от имени администрации сайта\" required></textarea></label>"
                "<div class=\"full\"><button type=\"submit\">Отправить ответ</button></div>"
                "</form>"
                "</section>"
            )
        support_section = (
            f"<section id=\"support\" class=\"card\"><h2>Поддержка сайта</h2>"
            "<p class=\"hint\">Здесь видны обращения с публичного сайта и ответы администрации. Пользователь увидит ответ в своем разделе поддержки после обновления страницы.</p>"
            f"<div class=\"support-overview\"><span class=\"meta-note\">Диалогов: {len(support_threads)}</span><span class=\"meta-note\">Непрочитанных пользовательских сообщений: {unread_support}</span></div>"
            f"{''.join(support_sections) or '<p class=\"hint\">Обращений пока нет.</p>'}"
            "</section>"
        )
        return (
            f"{self._head_html('Site Admin')}"
            "<body class=\"shell\"><main class=\"page\">"
            "<section class=\"hero hero-dashboard\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Веб-сайт</span>"
            "<h1>Site Admin</h1>"
            "<p class=\"lead\">Отдельный раздел для администрирования пользовательского сайта и его веб-аккаунтов.</p>"
            "<p class=\"hint\">Публичный сайт использует ту же базу материалов и общую память ответов, что и боты, но его пользователи и вход на сайт живут отдельно.</p>"
            "</div>"
            "<div class=\"hero-side\">"
            "<div class=\"meta-grid\">"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Аккаунты сайта</span><strong class=\"meta-value\">{len(site_accounts)}</strong><span class=\"meta-note\">Всего сохраненных веб-аккаунтов.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Активные</span><strong class=\"meta-value\">{active_accounts}</strong><span class=\"meta-note\">Аккаунты, которым разрешен вход на сайт.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Отключенные</span><strong class=\"meta-value\">{inactive_accounts}</strong><span class=\"meta-note\">Аккаунты, для которых вход временно закрыт.</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Публичный сайт</span><strong class=\"meta-value\">{public_site_status}</strong><span class=\"meta-note\">Адрес: {public_site_url}</span></div>"
            f"<div class=\"meta-card\"><span class=\"meta-label\">Поддержка</span><strong class=\"meta-value\">{len(support_threads)}</strong><span class=\"meta-note\">Новых сообщений: {unread_support}</span></div>"
            "</div>"
            "</div>"
            "</section>"
            f"<div class=\"toolbar\">{switch_nav}<form method=\"post\" action=\"/logout\" class=\"inline-form\"><button type=\"submit\" class=\"ghost\">Выйти</button></form></div>"
            f"{notice_html}{error_html}"
            "<nav class=\"nav\" aria-label=\"Разделы сайта\"><a href=\"#website\">Сайт</a><a href=\"#site-accounts\">Сайт-аккаунты</a><a href=\"#support\">Поддержка</a></nav>"
            f"<section id=\"website\" class=\"card\"><h2>Публичный сайт</h2><p class=\"hint\">Пользовательский сайт открыт отдельно от админки и работает по адресу <code>{public_site_url}</code>. Через этот раздел можно управлять только самим сайтом и его веб-аккаунтами, не затрагивая Telegram/VK-пользователей.</p><ul class=\"feature-list\"><li>Логины и пароли сайта отдельные.</li><li>Память материалов и поисковый индекс общие с ботами через одну базу данных.</li><li>Сайт-аккаунты можно отдельно отключать, переименовывать и создавать заново.</li></ul></section>"
            f"{site_account_section}"
            f"{support_section}"
            "</main></body></html>"
        )

    @staticmethod
    def _head_html(title: str) -> str:
        return (
            "<!doctype html><html lang=\"ru\"><head><meta charset=\"utf-8\">"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            f"<title>{escape(title)}</title><style>{LocalUploadServer._styles()}</style></head>"
        )

    @staticmethod
    def _styles() -> str:
        return (
            "@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');"
            ":root{--surface:rgba(255,255,255,.8);--line:rgba(24,32,51,.12);--text:#182033;--muted:#647084;--accent:#1667c7;--accent-2:#0f766e;--accent-soft:rgba(22,103,199,.12);--danger:#c15335;--shadow:0 24px 70px rgba(16,24,40,.12);--shadow-soft:0 14px 30px rgba(16,24,40,.08);--radius-xl:30px}"
            "*{box-sizing:border-box}html{scroll-behavior:smooth}"
            "body{margin:0;min-height:100vh;background:radial-gradient(circle at top left,rgba(22,103,199,.16),transparent 30%),radial-gradient(circle at top right,rgba(15,118,110,.12),transparent 24%),linear-gradient(180deg,#faf6ef 0%,#f1e8da 100%);color:var(--text);font:16px/1.5 'IBM Plex Sans','Segoe UI Variable Text','Trebuchet MS',sans-serif}"
            "body::before{content:'';position:fixed;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.4),transparent 48%,rgba(22,103,199,.05))}"
            ".page{position:relative;z-index:1;max-width:1360px;margin:0 auto;padding:32px 24px 72px}"
            ".hero{display:grid;grid-template-columns:minmax(0,1.45fr) minmax(320px,.95fr);gap:24px;margin-bottom:22px}"
            ".hero-copy,.hero-side,.card,.entry,.auth-card{background:var(--surface);backdrop-filter:blur(18px);border:1px solid var(--line);box-shadow:var(--shadow);border-radius:var(--radius-xl)}"
            ".hero-copy{padding:32px 34px}.hero-side{padding:24px 24px 22px}.card,.entry{padding:24px 24px 22px;margin:0 0 18px}"
            ".eyebrow{display:inline-flex;align-items:center;padding:7px 12px;border-radius:999px;background:rgba(24,32,51,.08);color:var(--muted);font-size:.82rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase}"
            "h1,h2{margin:0;font-family:'Space Grotesk','Segoe UI Variable Text',sans-serif;letter-spacing:-.02em}h1{margin-top:16px;font-size:clamp(2rem,3.2vw,3.6rem);line-height:1.02}h2{font-size:clamp(1.2rem,1.8vw,1.65rem);line-height:1.1}.card>h2,.entry>h2{margin-bottom:10px}"
            ".lead{margin:18px 0 0;max-width:62ch;font-size:1.03rem}.hint{margin:10px 0 0;color:var(--muted)}.toolbar{display:flex;flex-wrap:wrap;align-items:center;justify-content:space-between;gap:14px;margin:0 0 18px}"
            ".meta-grid{display:grid;gap:12px;grid-template-columns:repeat(2,minmax(0,1fr));margin-top:18px}.meta-card{padding:16px 18px;border-radius:20px;background:linear-gradient(180deg,rgba(255,255,255,.9),rgba(247,243,236,.95));border:1px solid rgba(24,32,51,.08);box-shadow:var(--shadow-soft)}"
            ".meta-label{display:block;font-size:.74rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}.meta-value{display:block;margin-top:8px;font-family:'Space Grotesk','Segoe UI Variable Text',sans-serif;font-size:1.34rem;line-height:1.08}.meta-note{display:block;margin-top:8px;color:var(--muted);font-size:.92rem}"
            ".feature-list{margin:16px 0 0;padding:0;list-style:none;display:grid;gap:10px}.feature-list li{position:relative;padding-left:18px;color:var(--muted)}.feature-list li::before{content:'';position:absolute;left:0;top:.58em;width:8px;height:8px;border-radius:999px;background:linear-gradient(135deg,var(--accent),var(--accent-2))}"
            ".switch-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(290px,1fr));gap:18px}.entry{display:grid;gap:14px;position:relative;overflow:hidden;background:linear-gradient(180deg,rgba(255,255,255,.88),rgba(248,244,236,.95))}.entry::after{content:'';position:absolute;inset:auto -30% -40% auto;width:180px;height:180px;border-radius:999px;background:radial-gradient(circle,rgba(22,103,199,.16),transparent 68%)}"
            ".entry-top{display:flex;justify-content:space-between;align-items:center;gap:12px}.entry-copy{margin:0;color:var(--muted)}.entry-actions,.inline-form{display:inline-flex;align-items:center;gap:10px;margin:0}.pill{display:inline-flex;align-items:center;padding:7px 11px;border-radius:999px;background:var(--accent-soft);color:var(--accent);font-size:.82rem;font-weight:700}"
            ".switcher,.nav{display:flex;flex-wrap:wrap;gap:10px}.switcher{margin:0}.switcher a,.nav a{display:inline-flex;align-items:center;justify-content:center;padding:10px 14px;border-radius:999px;border:1px solid var(--line);text-decoration:none;font-weight:700;color:var(--text);background:rgba(255,255,255,.72);box-shadow:0 10px 18px rgba(16,24,40,.05);transition:transform .18s ease,border-color .18s ease,color .18s ease}.switcher a:hover,.nav a:hover{transform:translateY(-1px);border-color:rgba(22,103,199,.28);color:var(--accent)}.switcher a.active{background:linear-gradient(135deg,var(--accent),var(--accent-2));color:#fff;border-color:transparent}"
            ".nav{position:sticky;top:16px;z-index:4;margin:0 0 18px;padding:14px;border-radius:20px;background:rgba(255,255,255,.72);backdrop-filter:blur(18px);border:1px solid var(--line);box-shadow:var(--shadow-soft);overflow:auto}"
            ".grid{display:grid;gap:14px}.two{grid-template-columns:repeat(2,minmax(0,1fr))}.three{grid-template-columns:repeat(3,minmax(0,1fr))}.full{grid-column:1/-1}"
            "form{margin:0}label{display:grid;gap:8px;color:var(--muted);font-size:.94rem;font-weight:600}input,textarea,select,button{font:inherit}"
            "input,textarea,select{width:100%;padding:13px 15px;border:1px solid rgba(24,32,51,.12);border-radius:16px;background:rgba(255,255,255,.94);color:var(--text);box-shadow:inset 0 1px 0 rgba(255,255,255,.8);transition:border-color .16s ease,box-shadow .16s ease}textarea{min-height:120px;resize:vertical}input:hover,textarea:hover,select:hover{border-color:rgba(22,103,199,.26)}input:focus,textarea:focus,select:focus{outline:none;border-color:rgba(22,103,199,.55);box-shadow:0 0 0 4px rgba(22,103,199,.14)}"
            "button{padding:12px 16px;border:0;border-radius:16px;background:linear-gradient(135deg,var(--accent),var(--accent-2));color:#fff;font-weight:700;letter-spacing:.01em;cursor:pointer;box-shadow:0 14px 24px rgba(22,103,199,.22);transition:transform .16s ease,box-shadow .16s ease}button:hover{transform:translateY(-1px);box-shadow:0 18px 30px rgba(22,103,199,.26)}button:active{transform:translateY(0)}button.ghost{background:rgba(255,255,255,.8);color:var(--text);border:1px solid var(--line);box-shadow:none}button.danger{background:linear-gradient(135deg,#d36a43,var(--danger));box-shadow:0 14px 24px rgba(193,83,53,.2)}"
            ".actions{display:flex;flex-wrap:wrap;gap:10px}.table{overflow:auto;border:1px solid var(--line);border-radius:20px;background:rgba(255,255,255,.72);box-shadow:inset 0 1px 0 rgba(255,255,255,.7)}table{width:100%;min-width:920px;border-collapse:separate;border-spacing:0;font-size:14px}thead th{position:sticky;top:0;background:rgba(244,240,233,.96);backdrop-filter:blur(12px);z-index:1}th,td{padding:12px 10px;border-top:1px solid rgba(24,32,51,.08);text-align:left;vertical-align:top}th{font-size:.72rem;letter-spacing:.08em;text-transform:uppercase;color:var(--muted)}tbody tr:nth-child(even){background:rgba(248,244,237,.55)}tbody tr:hover{background:rgba(22,103,199,.06)}"
            ".banner{padding:14px 16px;border-radius:18px;margin:0 0 12px;font-weight:700;border:1px solid transparent;box-shadow:var(--shadow-soft)}.ok{background:rgba(227,245,235,.92);border-color:rgba(16,185,129,.24);color:#0d6b45}.err{background:rgba(255,239,234,.94);border-color:rgba(193,83,53,.24);color:#8a2d1d}.auth-page{min-height:100vh;display:grid;place-items:center;padding:32px 20px}.auth-card{width:min(560px,100%);padding:32px}code{background:rgba(24,32,51,.08);padding:2px 6px;border-radius:8px}"
            ".support-overview{display:flex;flex-wrap:wrap;gap:10px;margin:16px 0 18px}.support-thread{display:grid;gap:14px;padding:18px;border-radius:24px;background:linear-gradient(180deg,rgba(255,255,255,.92),rgba(247,243,236,.96));border:1px solid rgba(24,32,51,.08);box-shadow:var(--shadow-soft);margin:0 0 16px}.support-thread-head{display:flex;justify-content:space-between;gap:14px;align-items:flex-start}.support-badges{display:flex;flex-wrap:wrap;gap:10px}.support-log{display:grid;gap:10px;max-height:360px;overflow:auto;padding-right:4px}.support-message{padding:14px 16px;border-radius:18px;background:rgba(255,255,255,.76);border:1px solid var(--line)}.support-message.admin{background:linear-gradient(135deg,rgba(22,103,199,.12),rgba(15,118,110,.12))}.support-meta{display:flex;justify-content:space-between;gap:10px;align-items:center;margin:0 0 8px;font-size:.8rem;color:var(--muted);font-weight:700}.support-message p{margin:0;white-space:pre-wrap}.support-form textarea{min-height:96px}"
            "@media (max-width:1040px){.hero{grid-template-columns:1fr}.nav{position:static}.meta-grid{grid-template-columns:1fr}}@media (max-width:840px){.two,.three{grid-template-columns:1fr}.page{padding:18px 14px 54px}.card,.entry,.hero-copy,.hero-side,.auth-card{padding:20px}.switcher,.toolbar{flex-direction:column;align-items:stretch}.switcher a,.nav a,.toolbar .inline-form,.toolbar form,.toolbar button{width:100%}.nav{padding:12px}}"
        )

    def _platform_entry_card(self, context: AdminPlatformContext) -> str:
        return (
            f'<section class="entry"><div class="entry-top"><span class="pill">{escape(context.bot_label)}</span></div>'
            f'<h2>{escape(context.title)}</h2><p class="entry-copy">{escape(context.storage_hint)}</p>'
            f'<form method="get" action="{context.base_path}" class="entry-actions"><button type="submit">Открыть раздел</button></form></section>'
        )

    def _site_entry_card(self) -> str:
        return (
            f'<section class="entry"><div class="entry-top"><span class="pill">Сайт</span></div>'
            '<h2>Site Admin</h2><p class="entry-copy">Отдельная панель для управления пользовательским сайтом, входом на сайт и веб-аккаунтами.</p>'
            f'<form method="get" action="{self._site_admin_base_path()}" class="entry-actions"><button type="submit">Открыть раздел</button></form></section>'
        )

    def _platform_switch_nav(self, *, current_slug: str) -> str:
        parts: list[str] = ['<nav class="switcher" aria-label="Переключение платформ">']
        for context in self._ordered_platforms():
            class_name = "active" if context.slug == current_slug else ""
            parts.append(f'<a class="{class_name}" href="{context.base_path}">{escape(context.bot_label)}</a>')
        site_class_name = "active" if current_slug == "site" else ""
        parts.append(f'<a class="{site_class_name}" href="{self._site_admin_base_path()}">Сайт</a>')
        parts.append("</nav>")
        return "".join(parts)

    def _promo_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        code = escape(str(row.get("code") or ""))
        expires_at = format_display_date(str(row.get("expires_at") or "")) or "-"
        return f'<tr><td>{code}</td><td>{int(row.get("bonus_requests") or 0)}</td><td>{int(row.get("redeemed_count") or 0)}</td><td>{escape(expires_at)}</td><td>{escape(str(row.get("max_redemptions") or "-"))}</td><td>{"да" if int(row.get("enabled") or 0) else "нет"}</td><td>{escape(self._short(str(row.get("note") or ""), 120))}</td><td><form method="post" action="{base_path}/promocodes/delete">{access_hidden}<input type="hidden" name="code" value="{code}"><button type="submit" class="danger">Удалить</button></form></td></tr>'

    def _command_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        command_name = escape(str(row.get("command_name") or ""))
        media_name = Path(str(row.get("media_path") or "")).name if row.get("media_path") else "-"
        return f'<tr><td>{command_name}</td><td>{escape(self._short(str(row.get("response_text") or ""), 120))}</td><td>{escape(media_name)}</td><td>{"да" if int(row.get("notify_admin") or 0) else "нет"}</td><td>{"да" if int(row.get("enabled") or 0) else "нет"}</td><td>{escape(str(row.get("updated_at") or "-"))}</td><td><form method="post" action="{base_path}/commands/delete">{access_hidden}<input type="hidden" name="command_name" value="{command_name}"><button type="submit" class="danger">Удалить</button></form></td></tr>'

    def _answer_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        option_id = int(row.get("id") or 0)
        media_name = Path(str(row.get("media_path") or "")).name if row.get("media_path") else "-"
        return f'<tr><td>{option_id}</td><td>{escape(str(row.get("trigger_text") or ""))}</td><td>{escape(str(row.get("match_mode") or "exact"))}</td><td>{escape(str(row.get("option_label") or ""))}</td><td>{int(row.get("sort_order") or 0)}</td><td>{escape(self._short(str(row.get("response_text") or ""), 120))}</td><td>{escape(media_name)}</td><td>{"да" if int(row.get("enabled") or 0) else "нет"}</td><td><form method="post" action="{base_path}/answers/delete">{access_hidden}<input type="hidden" name="option_id" value="{option_id}"><button type="submit" class="danger">Удалить</button></form></td></tr>'

    def _shift_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        shift_id = int(row.get("id") or 0)
        name = str(row.get("name") or "")
        date_from = format_display_date(str(row.get("date_from") or ""))
        date_to = format_display_date(str(row.get("date_to") or ""))
        updated_at = str(row.get("updated_at") or row.get("created_at") or "-")
        edit_form_id = f"shift-edit-{shift_id}"
        return (
            f'<tr><td>{shift_id}</td>'
            f'<td><input form="{edit_form_id}" name="name" value="{escape(name, quote=True)}" placeholder="1-11 июля 2025 года"></td>'
            f'<td><input form="{edit_form_id}" name="date_from" value="{escape(date_from, quote=True)}" placeholder="01-07-2025" required></td>'
            f'<td><input form="{edit_form_id}" name="date_to" value="{escape(date_to, quote=True)}" placeholder="11-07-2025" required></td>'
            f'<td>{escape(updated_at)}</td>'
            f'<td><div class="actions"><form id="{edit_form_id}" method="post" action="{base_path}/shifts/save">{access_hidden}<input type="hidden" name="shift_id" value="{shift_id}"><button type="submit">Сохранить</button></form>'
            f'<form method="post" action="{base_path}/shifts/delete">{access_hidden}<input type="hidden" name="shift_id" value="{shift_id}"><button type="submit" class="danger">Удалить</button></form></div></td></tr>'
        )

    def _pending_material_row(self, context: AdminPlatformContext, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        pending_id = int(row.get("id") or 0)
        date_text = context.app_service.display_content_with_shift(str(row.get("content_date") or ""), str(row.get("content_scope") or "dated"))
        scope_text = "компания / без даты" if str(row.get("content_scope") or "dated") == "timeless" else "с датой"
        site_file_name = str(row.get("original_file_name") or "-")
        return (
            f'<tr><td>{pending_id}</td><td>{int(row.get("admin_user_id") or 0)}</td><td>{escape(date_text)}</td>'
            f'<td>{escape(scope_text)}</td><td>{escape(self._short(str(row.get("description") or ""), 160))}</td>'
            f'<td>{escape(self._short(str(row.get("source_text") or ""), 160))}</td><td>{escape(site_file_name)}</td><td>{escape(str(row.get("created_at") or "-"))}</td>'
            f'<td><form method="post" action="{base_path}/materials/pending/delete">{access_hidden}<input type="hidden" name="pending_id" value="{pending_id}"><button type="submit" class="danger">Удалить</button></form></td></tr>'
        )

    def _item_row(self, context: AdminPlatformContext, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        item_id = int(row.get("id") or 0)
        date_text = context.app_service.display_content_with_shift(str(row.get("content_date") or ""), str(row.get("content_scope") or "dated"))
        scope_text = "компания / без даты" if str(row.get("content_scope") or "dated") == "timeless" else "с датой"
        return f'<tr><td>{item_id}</td><td>{escape(date_text)}</td><td>{escape(scope_text)}</td><td>{escape(str(row.get("item_type") or "-"))}</td><td>{escape(str(row.get("file_name") or "-"))}</td><td>{escape(self._short(str(row.get("summary") or ""), 180))}</td><td><form method="post" action="{base_path}/materials/delete">{access_hidden}<input type="hidden" name="item_id" value="{item_id}"><button type="submit" class="danger">Удалить</button></form></td></tr>'

    def _user_row(self, context: AdminPlatformContext, row: dict[str, object]) -> str:
        username = f'@{row.get("username")}' if row.get("username") else "-"
        prompt_profile = context.app_service.PROMPT_PROFILE_LABELS.get(str(row.get("prompt_profile") or "department"), "Департаментный")
        risk_text = f"{row.get('risk_level', 'low')}:{int(row.get('risk_score') or 0)}"
        banned_text = "да" if int(row.get("is_banned") or 0) else "нет"
        return f'<tr><td>{int(row.get("user_id") or 0)}</td><td>{escape(username)}</td><td>{escape(context.app_service.display_name(row))}</td><td>{escape(str(row.get("department") or "-"))}</td><td>{escape(prompt_profile)}</td><td>{escape(risk_text)}</td><td>{banned_text}</td><td>{int(row.get("bonus_requests") or 0)}</td><td>{int(row.get("total_today_count") or 0)}</td><td>{int(row.get("charged_today_count") or 0)}</td><td>{int(row.get("ask_count") or 0)}</td><td>{int(row.get("search_count") or 0)}</td><td>{int(row.get("file_count") or 0)}</td><td>{escape(str(row.get("last_seen_at") or "-"))}</td></tr>'

    def _site_account_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        username = escape(str(row.get("username") or ""))
        display_name = escape(str(row.get("display_name") or "-"))
        platform_user_id = int(row.get("platform_user_id") or 0)
        is_active = "да" if int(row.get("is_active") or 0) else "нет"
        updated_at = escape(str(row.get("updated_at") or row.get("created_at") or "-"))
        return (
            f'<tr><td>{username}</td><td>{display_name}</td><td>{platform_user_id}</td><td>{is_active}</td><td>{updated_at}</td>'
            f'<td><form method="post" action="{base_path}/site-accounts/delete">{access_hidden}<input type="hidden" name="username" value="{username}"><button type="submit" class="danger">Отключить</button></form></td></tr>'
        )

    def _request_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        request_id = int(row.get("id") or 0)
        status = str(row.get("status") or "pending")
        mode_bucket = escape(str(row.get("mode_bucket") or "-"))
        controls = (
            f'<form method="post" action="{base_path}/requests/review" class="grid two">{access_hidden}'
            f'<input type="hidden" name="request_id" value="{request_id}">'
            '<input type="hidden" name="status" value="approved">'
            '<label>Бонусные запросы<input name="granted_bonus_requests" type="number" min="0" value="5"></label>'
            '<label>Mode credits<input name="granted_mode_credits" type="number" min="0" value="1"></label>'
            '<label class="full">Комментарий<input name="decision_note"></label>'
            '<div><button type="submit">Одобрить</button></div></form>'
            f'<form method="post" action="{base_path}/requests/review">{access_hidden}<input type="hidden" name="request_id" value="{request_id}"><input type="hidden" name="status" value="rejected"><input type="hidden" name="decision_note" value="rejected"><button type="submit" class="danger">Отказать</button></form>'
        ) if status == "pending" else escape(str(row.get("decision_note") or "-"))
        return f'<tr><td>{request_id}</td><td>{int(row.get("user_id") or 0)}</td><td>{escape(str(row.get("request_type") or "-"))}</td><td>{escape(self._short(str(row.get("request_name") or ""), 80))}</td><td>{escape(self._short(str(row.get("reason") or ""), 160))}</td><td>{mode_bucket}</td><td>{escape(status)}</td><td>{int(row.get("granted_bonus_requests") or 0)}</td><td>{int(row.get("granted_mode_credits") or 0)}</td><td>{controls}</td></tr>'

    def _ban_row(self, row: dict[str, object], access_hidden: str, base_path: str) -> str:
        user_id = int(row.get("user_id") or 0)
        return f'<tr><td>{user_id}</td><td>{escape(self._short(str(row.get("reason") or ""), 160))}</td><td>{escape(str(row.get("banned_by_user_id") or "-"))}</td><td>{escape(str(row.get("created_at") or "-"))}</td><td>{escape(str(row.get("updated_at") or "-"))}</td><td><form method="post" action="{base_path}/bans/delete">{access_hidden}<input type="hidden" name="target_user_id" value="{user_id}"><button type="submit">Снять бан</button></form></td></tr>'

    def _event_row(self, context: AdminPlatformContext, row: dict[str, object]) -> str:
        risk = context.app_service.compute_risk_for_event(row)
        details = row.get("details") or {}
        detail_text = ", ".join(
            f"{key}={self._short(str(value), 60)}"
            for key, value in details.items()
            if value not in (None, "", [])
        )
        username = f'@{row.get("username")}' if row.get("username") else "-"
        risk_text = f'{risk["risk_level"]}:{risk["risk_score"]}'
        return f'<tr><td>{escape(str(row.get("created_at") or "-"))}</td><td>{int(row.get("user_id") or 0)}</td><td>{escape(str(row.get("event_type") or "-"))}</td><td>{escape(risk_text)}</td><td>{escape(username)}</td><td>{escape(self._short(detail_text, 220))}</td><td>{"да" if int(row.get("charged") or 0) else "нет"}</td></tr>'

    @staticmethod
    def _access_hidden_fields() -> str:
        return ""

    async def _read_multipart_fields(self, request: web.Request, file_targets: dict[str, Path]) -> tuple[dict[str, str], dict[str, tuple[Path, str]]]:
        reader = await request.multipart()
        fields: dict[str, str] = {}
        files: dict[str, tuple[Path, str]] = {}
        while True:
            field = await reader.next()
            if field is None:
                break
            if field.filename and field.name in file_targets:
                file_name = self._sanitize_file_name(field.filename)
                target = file_targets[field.name] / f"{uuid4().hex}_{file_name}"
                with target.open("wb") as handle:
                    while True:
                        chunk = await field.read_chunk()
                        if not chunk:
                            break
                        handle.write(chunk)
                files[field.name] = (target, file_name)
                continue
            fields[field.name] = (await field.text()).strip()
        return fields, files

    async def _read_simple_fields(self, request: web.Request) -> dict[str, str]:
        data = await request.post()
        return {key: str(value).strip() for key, value in data.items()}

    def _save_site_account_from_fields(
        self,
        app_service: PlatformAssistantService,
        fields: dict[str, str],
    ) -> tuple[str, int]:
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        display_name = fields.get("display_name", "").strip()
        is_active = self._is_checked(fields.get("is_active"), default=True)
        if not username:
            raise ValueError("Нужен логин сайта.")
        if not password:
            raise ValueError("Нужен пароль сайта.")
        existing = app_service.get_site_account_any(username)
        site_user_id = int(existing.get("platform_user_id") or 0) if existing else app_service.next_site_platform_user_id()
        app_service.upsert_site_account(
            username=username,
            password_hash=hash_password(password),
            display_name=display_name,
            platform_user_id=site_user_id,
            is_active=is_active,
        )
        return username, site_user_id

    def _delete_site_account_from_fields(
        self,
        app_service: PlatformAssistantService,
        fields: dict[str, str],
    ) -> tuple[str, bool]:
        username = fields.get("username", "").strip().lower()
        if not username:
            raise ValueError("Нужен логин сайта.")
        deleted = app_service.deactivate_site_account(username)
        return username, deleted

    def _ensure_access(self, context: AdminPlatformContext, admin_user_id: int, token: str) -> None:
        if not context.app_service.is_admin(admin_user_id) and admin_user_id not in self.settings.uploader_user_ids:
            raise PermissionError("Только администратор может открыть эту панель.")

    def _default_admin_user_id(self, context: AdminPlatformContext) -> int:
        platform_admins = sorted(context.app_service.external_admin_user_ids())
        if platform_admins:
            return int(platform_admins[0])
        telegram_admins = sorted(self.settings.uploader_user_ids)
        if telegram_admins:
            return int(telegram_admins[0])
        return 0

    def _default_upload_token(self) -> str:
        return self.settings.local_upload_token.strip()

    def _check_site_password(self, password: str) -> bool:
        expected = self.settings.local_upload_password.strip()
        if not expected:
            return True
        return secrets.compare_digest(password.strip(), expected)

    def _is_site_authenticated(self, request: web.Request) -> bool:
        expected = self.settings.local_upload_password.strip()
        if not expected:
            return True
        session_id = request.cookies.get(self.SESSION_COOKIE_NAME, "").strip()
        return bool(session_id and session_id in self._site_sessions)

    def _ensure_site_authenticated(self, request: web.Request) -> None:
        if not self._is_site_authenticated(request):
            raise web.HTTPFound("/")

    def _can_access(self, context: AdminPlatformContext, admin_user_id: int, token: str) -> bool:
        try:
            self._ensure_access(context, admin_user_id, token)
            return True
        except Exception:
            return False

    def _existing_command_media_path(self, context: AdminPlatformContext, command_name: str) -> str | None:
        normalized = self._normalize_command_name(command_name)
        for row in context.app_service.list_custom_commands():
            if self._normalize_command_name(str(row.get("command_name") or "")) == normalized:
                value = str(row.get("media_path") or "").strip()
                return value or None
        return None

    def _platform_context(self, request: web.Request) -> AdminPlatformContext:
        slug = str(request.match_info.get("platform") or "").strip().lower()
        context = self.platforms.get(slug)
        if context is None:
            raise web.HTTPNotFound(text=f"Unknown admin platform: {slug}")
        return context

    def _ordered_platforms(self) -> list[AdminPlatformContext]:
        preferred_order = ["telegram", "vk"]
        ordered = [self.platforms[slug] for slug in preferred_order if slug in self.platforms]
        for slug, context in sorted(self.platforms.items()):
            if slug not in preferred_order:
                ordered.append(context)
        return ordered

    def _build_platform_contexts(
        self,
        platform_services: dict[str, PlatformAssistantService],
        notification_gateways: dict[str, NotificationGateway],
    ) -> dict[str, AdminPlatformContext]:
        contexts: dict[str, AdminPlatformContext] = {}
        for slug, app_service in platform_services.items():
            if slug not in notification_gateways:
                raise RuntimeError(f"Missing notification gateway for admin platform: {slug}")
            root_dir = self.settings.media_cache_dir / "admin_assets" / slug
            upload_dir = self.settings.media_cache_dir / "local_uploads" / slug
            command_dir = root_dir / "commands"
            answer_dir = root_dir / "answers"
            for directory in [upload_dir, command_dir, answer_dir]:
                directory.mkdir(parents=True, exist_ok=True)
            title = "Telegram Admin" if slug == "telegram" else "VK Admin"
            contexts[slug] = AdminPlatformContext(
                slug=slug,
                title=title,
                app_service=app_service,
                notification_gateway=notification_gateways[slug],
                upload_dir=upload_dir,
                command_dir=command_dir,
                answer_dir=answer_dir,
            )
        return contexts

    @staticmethod
    def _normalize_command_name(command_name: str) -> str:
        raw = command_name.strip().lower()
        if not raw:
            return ""
        return raw if raw.startswith("/") else "/" + raw

    @classmethod
    def _is_reserved_command(cls, command_name: str) -> bool:
        reserved = {
            "/start",
            "/help",
            "/menu",
            "/ask",
            "/search",
            "/list",
            "/file",
            "/promo",
            "/promt",
            "/set_api",
            "/delete_api",
            "/set_prompt",
            "/delete_prompt",
            "/my_settings",
            "/stats",
            "/upload_local",
            "/request_access",
        }
        return cls._normalize_command_name(command_name) in reserved

    @staticmethod
    def _parse_int(value: str | None) -> int:
        try:
            return int(str(value or "").strip())
        except Exception:
            return 0

    @classmethod
    def _parse_optional_int(cls, value: str | None) -> int | None:
        parsed = cls._parse_int(value)
        return parsed if parsed > 0 else None

    @staticmethod
    def _is_checked(value: str | None, *, default: bool = False) -> bool:
        if value is None:
            return default
        return str(value).strip().lower() in {"1", "true", "on", "yes", "да", "checked"}

    @staticmethod
    def _sanitize_file_name(file_name: str) -> str:
        cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", Path(file_name).name).strip(" .")
        return cleaned[:180] or "upload.bin"

    @staticmethod
    def _short(text: str, limit: int) -> str:
        clean = str(text or "").strip()
        if len(clean) <= limit:
            return clean
        return clean[: max(limit - 3, 1)].rstrip() + "..."
