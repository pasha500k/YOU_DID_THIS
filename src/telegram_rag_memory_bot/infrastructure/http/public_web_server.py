"""
Файл: public_web_server.py
Поднимает отдельный пользовательский веб-сайт с собственной авторизацией и
отдельными сайт-аккаунтами. С ботами у него общая память материалов и поиска,
но пользовательские настройки, лимиты и история сайта живут отдельно.
"""

from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass
from html import escape
import logging
from pathlib import Path
import secrets
from typing import Any

from aiohttp import web

from telegram_rag_memory_bot.application.platform_service import PlatformAssistantService
from telegram_rag_memory_bot.config import PROJECT_ROOT, Settings
from telegram_rag_memory_bot.domain.models import ChatSession, ManagedAnswerChoice, ManagedAnswerOption, SenderProfile
from telegram_rag_memory_bot.domain.ports import NotificationGateway
from telegram_rag_memory_bot.utils.dates import format_display_date_range
from telegram_rag_memory_bot.utils.security import hash_password, verify_password

LOGGER = logging.getLogger(__name__)
FAVICON_PATH = PROJECT_ROOT / "mobile_app" / "assets" / "favicon.png"


@dataclass(slots=True)
class PublicPlatformContext:
    slug: str
    title: str
    subtitle: str
    accent: str
    app_service: PlatformAssistantService
    notification_gateway: NotificationGateway

    @property
    def base_path(self) -> str:
        return f"/{self.slug}"


@dataclass(slots=True)
class PublicSiteSession:
    session_id: str
    platform_slug: str
    user_id: int
    display_name: str
    chat_session: ChatSession
    username: str = ""
    result_title: str = ""
    result_text: str = ""
    notice_text: str = ""
    error_text: str = ""


class PublicWebServer:
    SESSION_COOKIE_NAME = "public_site_session"

    def __init__(
        self,
        settings: Settings,
        platform_services: dict[str, PlatformAssistantService],
        notification_gateways: dict[str, NotificationGateway],
    ) -> None:
        self.settings = settings
        self.platforms = self._build_platform_contexts(platform_services, notification_gateways)
        self._sessions: dict[str, PublicSiteSession] = {}
        self.web_app = web.Application(client_max_size=0)
        self.web_app.add_routes(
            [
                web.get("/", self._handle_root),
                web.get("/login", self._handle_login_page),
                web.get("/register", self._handle_register_page),
                web.post("/login", self._handle_login),
                web.post("/register", self._handle_register),
                web.post("/logout", self._handle_logout),
                web.get("/health", self._handle_health),
                web.get("/favicon.ico", self._handle_favicon),
                web.get("/app", self._handle_dashboard),
                web.get("/settings", self._handle_settings_page),
                web.get("/settings/api", self._handle_api_settings_page),
                web.get("/support", self._handle_support_page),
                web.post("/ask", self._handle_ask),
                web.post("/support/send", self._handle_support_send),
                web.post("/managed-answer", self._handle_managed_answer),
                web.post("/search", self._handle_search),
                web.post("/list", self._handle_list),
                web.post("/file", self._handle_file),
                web.post("/promo", self._handle_promo),
                web.post("/command/run", self._handle_custom_command),
                web.post("/department/save", self._handle_department_save),
                web.post("/department/action", self._handle_department_action),
                web.post("/settings/account/save", self._handle_account_save),
                web.post("/settings/password/save", self._handle_password_save),
                web.post("/settings/api/save", self._handle_api_save),
                web.post("/settings/api/delete", self._handle_api_delete),
                web.post("/settings/prompt/save", self._handle_prompt_save),
                web.post("/settings/prompt/delete", self._handle_prompt_delete),
                web.post("/settings/profile/save", self._handle_prompt_profile_save),
                web.post("/requests/create", self._handle_access_request),
                web.get("/{platform}", self._handle_dashboard),
                web.get("/{platform}/support", self._handle_support_page),
                web.post("/{platform}/ask", self._handle_ask),
                web.post("/{platform}/support/send", self._handle_support_send),
                web.post("/{platform}/managed-answer", self._handle_managed_answer),
                web.post("/{platform}/search", self._handle_search),
                web.post("/{platform}/list", self._handle_list),
                web.post("/{platform}/file", self._handle_file),
                web.post("/{platform}/promo", self._handle_promo),
                web.post("/{platform}/command/run", self._handle_custom_command),
                web.post("/{platform}/department/save", self._handle_department_save),
                web.post("/{platform}/department/action", self._handle_department_action),
                web.post("/{platform}/settings/api/save", self._handle_api_save),
                web.post("/{platform}/settings/api/delete", self._handle_api_delete),
                web.post("/{platform}/settings/prompt/save", self._handle_prompt_save),
                web.post("/{platform}/settings/prompt/delete", self._handle_prompt_delete),
                web.post("/{platform}/settings/profile/save", self._handle_prompt_profile_save),
                web.post("/{platform}/requests/create", self._handle_access_request),
            ]
        )
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        if not self.settings.public_web_enabled:
            LOGGER.info("Public web server is disabled.")
            return
        self.runner = web.AppRunner(self.web_app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, host=self.settings.public_web_host, port=self.settings.public_web_port)
        await self.site.start()
        LOGGER.info("Public web server started on %s", self.settings.public_web_base_url)
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
                "base_url": self.settings.public_web_base_url,
                "platforms": sorted(self.platforms.keys()),
                "synced_with_bots": True,
            }
        )

    async def _handle_favicon(self, request: web.Request) -> web.Response:
        if FAVICON_PATH.exists():
            return web.FileResponse(path=Path(FAVICON_PATH))
        return web.Response(status=204)

    async def _handle_root(self, request: web.Request) -> web.StreamResponse:
        session = self._current_session(request)
        if session is not None:
            context = self.platforms.get(session.platform_slug)
            if context is not None:
                raise web.HTTPFound(context.base_path)
        return web.Response(text=self._render_landing(error_text=""), content_type="text/html")

    async def _handle_login(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        slug = fields.get("platform", "").strip().lower()
        user_id_raw = fields.get("user_id", "").strip()
        display_name = fields.get("display_name", "").strip()
        password = fields.get("password", "").strip()
        context = self.platforms.get(slug)
        if context is None:
            return web.Response(text=self._render_landing(error_text="Выберите платформу."), content_type="text/html", status=400)
        if not user_id_raw.isdigit():
            return web.Response(text=self._render_landing(error_text="User ID должен быть числом."), content_type="text/html", status=400)
        if not self._check_password(password):
            return web.Response(text=self._render_landing(error_text="Неверный пароль для сайта."), content_type="text/html", status=403)
        user_id = int(user_id_raw)
        if not context.app_service.is_authorized(user_id):
            return web.Response(
                text=self._render_landing(error_text="Для этого user id вход на сайт сейчас недоступен."),
                content_type="text/html",
                status=403,
            )
        banned, ban_reason = context.app_service.is_banned(user_id)
        if banned:
            reason_text = f"Причина: {ban_reason}" if ban_reason else "Доступ заблокирован."
            return web.Response(text=self._render_landing(error_text=reason_text), content_type="text/html", status=403)
        resolved_name = display_name or self._resolve_display_name(context, user_id)
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=slug,
            user_id=user_id,
            display_name=resolved_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            notice_text="Вход выполнен.",
        )
        response = web.HTTPFound(context.base_path)
        response.set_cookie(self.SESSION_COOKIE_NAME, session_id, httponly=True, samesite="Lax")
        raise response

    async def _handle_logout(self, request: web.Request) -> web.StreamResponse:
        session_id = request.cookies.get(self.SESSION_COOKIE_NAME, "").strip()
        if session_id:
            self._sessions.pop(session_id, None)
        response = web.HTTPFound("/")
        response.del_cookie(self.SESSION_COOKIE_NAME)
        raise response

    async def _handle_dashboard(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        return self._dashboard_response(context, session)

    async def _handle_support_page(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        return self._support_response(context, session)


def _public_web_open_public_session(
    self: PublicWebServer,
    *,
    context: PublicPlatformContext,
    user_id: int,
    display_name: str,
    username: str,
    notice_text: str,
) -> web.StreamResponse:
    session_id = secrets.token_urlsafe(32)
    self._sessions[session_id] = PublicSiteSession(
        session_id=session_id,
        platform_slug=context.slug,
        user_id=user_id,
        display_name=display_name,
        chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
        username=username,
        notice_text=notice_text,
    )
    response = web.HTTPFound("/app")
    response.set_cookie(
        self.SESSION_COOKIE_NAME,
        session_id,
        httponly=True,
        samesite="Lax",
        path="/",
        secure=self._cookie_secure(),
    )
    raise response


def _public_web_render_landing(self: PublicWebServer, *, error_text: str) -> str:
    error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
    feature_cards = (
        '<section class="entry telegram"><span class="pill">Чат</span><h2>Рабочее пространство</h2><p>Один интерфейс для диалога с AI, поиска по материалам, истории ответов и быстрых действий по базе знаний.</p><p class="muted">Сайт адаптирован под телефон и компьютер без отдельной мобильной версии интерфейса.</p></section>'
        '<section class="entry vk"><span class="pill">Настройки</span><h2>Глубокая кастомизация</h2><p>Отдельные страницы для профиля, API token, prompt-профиля, пользовательского prompt и рабочих параметров.</p><p class="muted">Сайт использует свои логины и пароли, а общими с ботами остаются только материалы и RAG-память.</p></section>'
        '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с командой</h2><p>Если на сайте возникла ошибка или нужен доступ, можно сразу написать в отдельный чат поддержки внутри интерфейса.</p><p class="muted">Переписка видна администрации в отдельной панели и хранится в общей базе сайта.</p></section>'
    )
    return (
        f"{self._head_html('Letovo Assistant')}"
        '<body class="site home"><main class="page">'
        '<section class="hero">'
        '<div class="hero-copy">'
        '<span class="eyebrow">Веб-платформа</span>'
        '<h1>Letovo Assistant</h1>'
        '<p class="lead">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>'
        f'<p class="muted">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Вход и регистрация живут отдельно от ботов, а общая база материалов и память ответов синхронизируются автоматически.</p>'
        f"{error_html}"
        '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
        "</div>"
        '<div class="hero-side summary">'
        '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены, поиск и поддержка внутри сайта.</span></div>'
        '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск едины для сайта и ботов.</span></div>'
        '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Пользователи сайта не привязываются к аккаунтам в ботах.</span></div>'
        '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и стиль ответов вынесены на отдельную страницу.</span></div>'
        "</div>"
        "</section>"
        f'<section class="entry-grid">{feature_cards}</section>'
        "</main></body></html>"
    )


async def _public_web_handle_login(self: PublicWebServer, request: web.Request) -> web.StreamResponse:
    fields = await self._read_simple_fields(request)
    username = fields.get("username", "").strip().lower()
    password = fields.get("password", "").strip()
    if not username:
        return web.Response(
            text=self._render_login_page(error_text="Введите логин сайта.", username=username),
            content_type="text/html",
            status=400,
        )
    if not password:
        return web.Response(
            text=self._render_login_page(error_text="Введите пароль сайта.", username=username),
            content_type="text/html",
            status=400,
        )

    context, account = self._find_site_account(username)
    if context is None or account is None:
        return web.Response(
            text=self._render_login_page(
                error_text="Такой аккаунт сайта не найден или еще не активирован.",
                username=username,
            ),
            content_type="text/html",
            status=403,
        )
    if not verify_password(password, str(account.get("password_hash") or "")):
        return web.Response(
            text=self._render_login_page(error_text="Неверный логин или пароль сайта.", username=username),
            content_type="text/html",
            status=403,
        )

    try:
        user_id = int(account.get("platform_user_id") or 0)
    except (TypeError, ValueError):
        user_id = 0
    if user_id == 0:
        return web.Response(
            text=self._render_login_page(
                error_text="Аккаунт сайта настроен некорректно. Обратитесь к команде проекта.",
                username=username,
            ),
            content_type="text/html",
            status=400,
        )

    banned, ban_reason = context.app_service.is_banned(user_id)
    if banned:
        reason_text = f"Причина: {ban_reason}" if ban_reason else "Доступ заблокирован."
        return web.Response(
            text=self._render_login_page(error_text=reason_text, username=username),
            content_type="text/html",
            status=403,
        )

    resolved_name = str(account.get("display_name") or "").strip() or self._resolve_display_name(context, user_id)
    return self._open_public_session(
        context=context,
        user_id=user_id,
        display_name=resolved_name,
        username=username,
        notice_text="Вход выполнен. Добро пожаловать в рабочее пространство сайта.",
    )


async def _public_web_handle_register(self: PublicWebServer, request: web.Request) -> web.StreamResponse:
    fields = await self._read_simple_fields(request)
    username = fields.get("username", "").strip().lower()
    password = fields.get("password", "").strip()
    password_repeat = fields.get("password_repeat", "").strip()
    display_name = fields.get("display_name", "").strip()
    context = self._site_context()

    if len(username) < 3:
        return web.Response(
            text=self._render_register_page(
                error_text="Логин должен содержать минимум 3 символа.",
                username=username,
                display_name=display_name,
            ),
            content_type="text/html",
            status=400,
        )
    if any(ch.isspace() for ch in username):
        return web.Response(
            text=self._render_register_page(
                error_text="Логин не должен содержать пробелы.",
                username=username,
                display_name=display_name,
            ),
            content_type="text/html",
            status=400,
        )
    if len(password) < 8:
        return web.Response(
            text=self._render_register_page(
                error_text="Пароль должен содержать минимум 8 символов.",
                username=username,
                display_name=display_name,
            ),
            content_type="text/html",
            status=400,
        )
    if password != password_repeat:
        return web.Response(
            text=self._render_register_page(
                error_text="Пароли не совпадают. Регистрация не выполнена.",
                username=username,
                display_name=display_name,
            ),
            content_type="text/html",
            status=400,
        )

    existing_context, existing_account = self._find_site_account_any(username)
    if existing_context is not None and existing_account is not None:
        return web.Response(
            text=self._render_register_page(
                error_text="Такой логин уже занят.",
                username=username,
                display_name=display_name,
            ),
            content_type="text/html",
            status=409,
        )

    user_id = context.app_service.next_site_platform_user_id()
    resolved_name = display_name or f"Пользователь {username}"
    context.app_service.upsert_site_account(
        username=username,
        password_hash=hash_password(password),
        display_name=resolved_name,
        platform_user_id=user_id,
        is_active=True,
    )
    context.app_service.log_event(
        user_id=user_id,
        chat_id=user_id,
        event_type="site_registration",
        sender_profile=SenderProfile(first_name=resolved_name),
        details={"platform": context.slug, "surface": "web"},
    )
    return self._open_public_session(
        context=context,
        user_id=user_id,
        display_name=resolved_name,
        username=username,
        notice_text="Регистрация завершена. Сайт-аккаунт создан и готов к работе.",
    )


PublicWebServer._open_public_session = _public_web_open_public_session
PublicWebServer._render_landing = _public_web_render_landing
PublicWebServer._handle_login = _public_web_handle_login
PublicWebServer._handle_register = _public_web_handle_register


class PublicWebServer(PublicWebServer):
    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound("/app")
        response.set_cookie(
            self.SESSION_COOKIE_NAME,
            session_id,
            httponly=True,
            samesite="Lax",
            path="/",
            secure=self._cookie_secure(),
        )
        raise response

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Чат</span><h2>Один рабочий экран</h2><p>Диалог, поиск по материалам, смены, история ответов и быстрые действия собраны в одном интерфейсе.</p><p class="muted">Сайт одинаково удобно работает на телефоне и на компьютере.</p></section>'
            '<section class="entry vk"><span class="pill">Настройки</span><h2>Глубокая кастомизация</h2><p>Отдельные страницы для профиля, AI-настроек, API token, prompt и персонального режима работы.</p><p class="muted">Вход и регистрация сайта полностью отдельные от ботов.</p></section>'
            '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с администрацией</h2><p>Если что-то сломалось, можно открыть отдельный диалог поддержки, который сразу виден команде проекта.</p><p class="muted">Вся история обращений сохраняется прямо в базе сайта.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            '<body class="site home"><main class="page">'
            '<section class="hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Веб-платформа</span>'
            '<h1>Letovo Assistant</h1>'
            '<p class="lead">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>'
            f'<p class="muted">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Сайт использует отдельные логин и пароль, а общими с ботами остаются только база материалов и RAG-память.</p>'
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
            '</div>'
            '<div class="hero-side summary">'
            '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены и поддержка внутри сайта.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск общие с ботами через одну базу.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Сайт не синхронизирует пользователей с ботами.</span></div>'
            '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и профиль ответа вынесены в отдельный раздел.</span></div>'
            '</div>'
            '</section>'
            f'<section class="entry-grid">{feature_cards}</section>'
            '</main></body></html>'
        )

    def _render_public_nav(self, active: str) -> str:
        items = [
            ("dashboard", "/app", "Чат"),
            ("settings", "/settings", "Настройки"),
            ("settings-api", "/settings/api", "AI и API"),
            ("support", "/support", "Поддержка"),
        ]
        links = []
        for key, href, label in items:
            if active == key:
                links.append(f'<span class="pill">{escape(label)}</span>')
            else:
                links.append(f'<a class="ghost-link" href="{href}">{escape(label)}</a>')
        return (
            '<div class="toolbar">'
            f'<div class="switcher">{"".join(links)}</div>'
            '<div class="toolbar">'
            '<a class="ghost-link" href="/">Главная</a>'
            '<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>'
            "</div>"
            "</div>"
        )

    def _render_command_buttons(self, context: PublicPlatformContext, commands: list[dict[str, Any]]) -> str:
        if not commands:
            return '<p class="muted">Дополнительные команды пока не настроены.</p>'
        buttons = []
        for row in commands[:18]:
            command_name = str(row.get("command_name") or "").strip()
            if not command_name:
                continue
            buttons.append(
                '<form method="post" action="/command/run" class="command-form">'
                f'<input type="hidden" name="command_name" value="{escape(command_name)}">'
                f'<button type="submit" class="ghost">{escape(command_name)}</button>'
                '</form>'
            )
        return f'<div class="command-grid">{"".join(buttons)}</div>'

    def _render_department_action_card(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        action: dict[str, str] | None,
    ) -> str:
        user_department = context.app_service.get_user_department(session.user_id)
        if not action and user_department != "проект 11":
            return ""
        if user_department == "проект 11":
            options_html = "".join(
                f'<option value="{escape(label)}">{escape(label)}</option>'
                for label in context.app_service.all_department_action_labels()
            )
            return (
                '<section class="card primary">'
                '<span class="eyebrow">Проект 11</span>'
                '<h2>Любой спец-режим на сегодня</h2>'
                f'<p class="muted">{escape(context.app_service.department_action_picker_prompt())}</p>'
                '<form method="post" action="/department/action" class="stack">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Режим<select name="action_label" required>{options_html}</select></label>'
                '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
                '<button type="submit">Запустить анализ</button>'
                '</form>'
                '</section>'
            )
        if action is None:
            return ""
        return (
            '<section class="card primary">'
            f'<span class="eyebrow">{escape(str(action.get("department") or ""))}</span>'
            f'<h2>{escape(str(action.get("title") or "Спец-режим"))}</h2>'
            '<p class="muted">Базовый лимит этого режима обновляется каждый день. Если для вас доступны дополнительные спец-запросы, они будут учтены автоматически.</p>'
            '<form method="post" action="/department/action" class="stack">'
            '<input type="hidden" name="return_to" value="dashboard">'
            '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
            f'<button type="submit">{escape(str(action.get("button") or "Запустить"))}</button>'
            '</form>'
            '</section>'
        )

    def _render_managed_choice_card(
        self,
        context: PublicPlatformContext,
        pending: ManagedAnswerChoice | None,
    ) -> str:
        if pending is None:
            return ""
        buttons_html = "".join(
            '<form method="post" action="/managed-answer" class="choice-form">'
            f'<input type="hidden" name="option_id" value="{option.option_id}">'
            f'<button type="submit">{escape(option.option_label)}</button>'
            '</form>'
            for option in pending.options
        )
        return (
            '<section class="card spotlight managed-choice">'
            '<span class="eyebrow">Готовые ответы</span>'
            '<h2>Выберите вариант</h2>'
            f'<p class="muted">Вопрос: {escape(pending.question)}</p>'
            f'<div class="choice-grid">{buttons_html}</div>'
            '</section>'
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        action = context.app_service.department_action_for_user(session.user_id)
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)

        if banned:
            reason_text = ban_reason or "Причина не указана."
            content_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, откройте страницу поддержки и опишите ситуацию.</p>'
                '</section>'
            )
        elif not department:
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Обязательный шаг</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">После выбора департамента сайт активирует правильные лимиты, профиль ответов и специальные режимы именно для вашего рабочего пространства.</p>'
                '<form method="post" action="/department/save" class="grid one">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить и открыть чат</button></div>'
                '</form>'
                '</section>'
            )
        else:
            chat_html = self._render_chat_panel(context, session)
            result_panel = self._render_result_panel(session)
            commands_html = self._render_command_buttons(context, custom_commands)
            shifts_html = self._render_shift_list(shifts)
            events_html = self._render_event_list(recent_events)
            action_html = self._render_department_action_card(context, session, action)
            content_html = (
                '<section class="workspace">'
                '<section class="chat-column">'
                '<section class="card chat-shell">'
                '<div class="chat-shell-head"><div><span class="eyebrow">Чат</span><h2>Главный диалог</h2><p class="muted">Задавайте вопросы по материалам, ищите факты по датам и сменам, запускайте готовые сценарии и получайте текстовые ответы прямо в ленте.</p></div>'
                f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний</small></span></div></div>'
                '<form method="post" action="/ask" class="chat-composer">'
                '<textarea name="question" rows="4" placeholder="Например: что происходило в смене 01-07-2025..11-07-2025 и есть ли материалы по World News 24?" required></textarea>'
                '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
                '</form>'
                f'{managed_choice_html}'
                f'{chat_html}'
                '</section>'
                f'{result_panel}'
                '</section>'
                '<aside class="sidebar-column">'
                '<section class="card side-panel sticky-panel">'
                '<span class="eyebrow">Сводка</span><h2>Рабочая панель</h2>'
                f'<div class="mini-list"><div class="mini-item"><strong>Департамент</strong><span>{escape(department)}</span></div><div class="mini-item"><strong>Prompt профиль</strong><span>{escape(prompt_profile_label)}</span></div><div class="mini-item"><strong>API token</strong><span>{"подключен" if has_api else "не подключен"}</span></div><div class="mini-item"><strong>Сегодня</strong><span>Списано запросов: {int(stats_row.get("charged_today_count") or 0)}</span></div></div>'
                '</section>'
                '<section class="card side-panel">'
                '<span class="eyebrow">Быстрые действия</span><h2>Поиск и материалы</h2>'
                '<details class="tool-panel" open><summary>Быстрый поиск</summary><form method="post" action="/search" class="stack compact-form"><input name="query" placeholder="Ключевые слова, люди, компании" required><button type="submit">Искать</button></form></details>'
                '<details class="tool-panel"><summary>Дата или смена</summary><form method="post" action="/list" class="stack compact-form"><input name="query" placeholder="21-03-2026 или название смены" required><button type="submit">Показать список</button></form></details>'
                '<details class="tool-panel"><summary>Материал по ID</summary><form method="post" action="/file" class="stack compact-form"><input name="item_id" inputmode="numeric" placeholder="Например 123" required><button type="submit">Показать материал</button></form><p class="muted">Сайт показывает извлеченную информацию, а не сам файл.</p></details>'
                '<details class="tool-panel"><summary>Промокод</summary><form method="post" action="/promo" class="stack compact-form"><input name="code" placeholder="Введите промокод" required><button type="submit">Активировать</button></form></details>'
                '</section>'
                f'{action_html}'
                '<section class="card side-panel">'
                '<span class="eyebrow">Дополнительно</span><h2>Команды, смены и события</h2>'
                f'<details class="tool-panel" open><summary>Кастомные команды</summary>{commands_html}</details>'
                f'<details class="tool-panel"><summary>Смены</summary>{shifts_html}</details>'
                f'<details class="tool-panel"><summary>Последние события</summary>{events_html}</details>'
                '</section>'
                '</aside>'
                '</section>'
            )

        return (
            f"{self._head_html('Рабочее пространство')}"
            '<body class="site"><main class="page">'
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Рабочее пространство</span>'
            '<h1>Letovo Assistant</h1>'
            f'<p class="lead">{escape(context.subtitle)}</p>'
            '<p class="muted">Главная страница сайта: чат, быстрый поиск, работа со сменами, история запросов и доступ к общей RAG-памяти материалов.</p>'
            '</div>'
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Пользователь</span><strong>{escape(session.display_name)}</strong><span class="meta-note">@{escape(session.username or "site-user")}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department or "не выбран")}</strong><span class="meta-note">Профиль: {escape(prompt_profile_label)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">API token</span><strong>{"подключен" if has_api else "не подключен"}</strong><span class="meta-note">AI-настройки на отдельной странице</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Лимиты</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            '</div>'
            '</section>'
            f'{self._render_public_nav("dashboard")}'
            f'{notice_html}{error_html}{content_html}'
            '</main></body></html>'
        )





    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound("/app")
        response.set_cookie(
            self.SESSION_COOKIE_NAME,
            session_id,
            httponly=True,
            samesite="Lax",
            path="/",
            secure=self._cookie_secure(),
        )
        raise response

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Чат</span><h2>Один рабочий экран</h2><p>Диалог, поиск по материалам, смены, история ответов и быстрые действия собраны в одном интерфейсе.</p><p class="muted">Сайт одинаково удобно работает на телефоне и на компьютере.</p></section>'
            '<section class="entry vk"><span class="pill">Настройки</span><h2>Глубокая кастомизация</h2><p>Отдельные страницы для профиля, AI-настроек, API token, prompt и персонального режима работы.</p><p class="muted">Вход и регистрация сайта полностью отдельные от ботов.</p></section>'
            '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с администрацией</h2><p>Если что-то сломалось, можно открыть отдельный диалог поддержки, который сразу виден команде проекта.</p><p class="muted">Вся история обращений сохраняется прямо в базе сайта.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            '<body class="site home"><main class="page">'
            '<section class="hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Веб-платформа</span>'
            '<h1>Letovo Assistant</h1>'
            '<p class="lead">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>'
            f'<p class="muted">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Сайт использует отдельные логин и пароль, а общими с ботами остаются только база материалов и RAG-память.</p>'
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
            '</div>'
            '<div class="hero-side summary">'
            '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены и поддержка внутри сайта.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск общие с ботами через одну базу.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Сайт не синхронизирует пользователей с ботами.</span></div>'
            '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и профиль ответа вынесены в отдельный раздел.</span></div>'
            '</div>'
            '</section>'
            f'<section class="entry-grid">{feature_cards}</section>'
            '</main></body></html>'
        )

    def _render_public_nav(self, active: str) -> str:
        items = [
            ("dashboard", "/app", "Чат"),
            ("settings", "/settings", "Настройки"),
            ("settings-api", "/settings/api", "AI и API"),
            ("support", "/support", "Поддержка"),
        ]
        links = []
        for key, href, label in items:
            if active == key:
                links.append(f'<span class="pill">{escape(label)}</span>')
            else:
                links.append(f'<a class="ghost-link" href="{href}">{escape(label)}</a>')
        return (
            '<div class="toolbar">'
            f'<div class="switcher">{"".join(links)}</div>'
            '<div class="toolbar">'
            '<a class="ghost-link" href="/">Главная</a>'
            '<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>'
            "</div>"
            "</div>"
        )

    def _render_command_buttons(self, context: PublicPlatformContext, commands: list[dict[str, Any]]) -> str:
        if not commands:
            return '<p class="muted">Дополнительные команды пока не настроены.</p>'
        buttons = []
        for row in commands[:18]:
            command_name = str(row.get("command_name") or "").strip()
            if not command_name:
                continue
            buttons.append(
                '<form method="post" action="/command/run" class="command-form">'
                f'<input type="hidden" name="command_name" value="{escape(command_name)}">'
                f'<button type="submit" class="ghost">{escape(command_name)}</button>'
                '</form>'
            )
        return f'<div class="command-grid">{"".join(buttons)}</div>'

    def _render_department_action_card(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        action: dict[str, str] | None,
    ) -> str:
        user_department = context.app_service.get_user_department(session.user_id)
        if not action and user_department != "проект 11":
            return ""
        if user_department == "проект 11":
            options_html = "".join(
                f'<option value="{escape(label)}">{escape(label)}</option>'
                for label in context.app_service.all_department_action_labels()
            )
            return (
                '<section class="card primary">'
                '<span class="eyebrow">Проект 11</span>'
                '<h2>Любой спец-режим на сегодня</h2>'
                f'<p class="muted">{escape(context.app_service.department_action_picker_prompt())}</p>'
                '<form method="post" action="/department/action" class="stack">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Режим<select name="action_label" required>{options_html}</select></label>'
                '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
                '<button type="submit">Запустить анализ</button>'
                '</form>'
                '</section>'
            )
        if action is None:
            return ""
        return (
            '<section class="card primary">'
            f'<span class="eyebrow">{escape(str(action.get("department") or ""))}</span>'
            f'<h2>{escape(str(action.get("title") or "Спец-режим"))}</h2>'
            '<p class="muted">Базовый лимит этого режима обновляется каждый день. Если для вас доступны дополнительные спец-запросы, они будут учтены автоматически.</p>'
            '<form method="post" action="/department/action" class="stack">'
            '<input type="hidden" name="return_to" value="dashboard">'
            '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
            f'<button type="submit">{escape(str(action.get("button") or "Запустить"))}</button>'
            '</form>'
            '</section>'
        )

    def _render_managed_choice_card(
        self,
        context: PublicPlatformContext,
        pending: ManagedAnswerChoice | None,
    ) -> str:
        if pending is None:
            return ""
        buttons_html = "".join(
            '<form method="post" action="/managed-answer" class="choice-form">'
            f'<input type="hidden" name="option_id" value="{option.option_id}">'
            f'<button type="submit">{escape(option.option_label)}</button>'
            '</form>'
            for option in pending.options
        )
        return (
            '<section class="card spotlight managed-choice">'
            '<span class="eyebrow">Готовые ответы</span>'
            '<h2>Выберите вариант</h2>'
            f'<p class="muted">Вопрос: {escape(pending.question)}</p>'
            f'<div class="choice-grid">{buttons_html}</div>'
            '</section>'
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        action = context.app_service.department_action_for_user(session.user_id)
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)

        if banned:
            reason_text = ban_reason or "Причина не указана."
            content_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, откройте страницу поддержки и опишите ситуацию.</p>'
                '</section>'
            )
        elif not department:
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Обязательный шаг</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">После выбора департамента сайт активирует правильные лимиты, профиль ответов и специальные режимы именно для вашего рабочего пространства.</p>'
                '<form method="post" action="/department/save" class="grid one">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить и открыть чат</button></div>'
                '</form>'
                '</section>'
            )
        else:
            chat_html = self._render_chat_panel(context, session)
            result_panel = self._render_result_panel(session)
            commands_html = self._render_command_buttons(context, custom_commands)
            shifts_html = self._render_shift_list(shifts)
            events_html = self._render_event_list(recent_events)
            action_html = self._render_department_action_card(context, session, action)
            content_html = (
                '<section class="workspace">'
                '<section class="chat-column">'
                '<section class="card chat-shell">'
                '<div class="chat-shell-head"><div><span class="eyebrow">Чат</span><h2>Главный диалог</h2><p class="muted">Задавайте вопросы по материалам, ищите факты по датам и сменам, запускайте готовые сценарии и получайте текстовые ответы прямо в ленте.</p></div>'
                f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний</small></span></div></div>'
                '<form method="post" action="/ask" class="chat-composer">'
                '<textarea name="question" rows="4" placeholder="Например: что происходило в смене 01-07-2025..11-07-2025 и есть ли материалы по World News 24?" required></textarea>'
                '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
                '</form>'
                f'{managed_choice_html}'
                f'{chat_html}'
                '</section>'
                f'{result_panel}'
                '</section>'
                '<aside class="sidebar-column">'
                '<section class="card side-panel sticky-panel">'
                '<span class="eyebrow">Сводка</span><h2>Рабочая панель</h2>'
                f'<div class="mini-list"><div class="mini-item"><strong>Департамент</strong><span>{escape(department)}</span></div><div class="mini-item"><strong>Prompt профиль</strong><span>{escape(prompt_profile_label)}</span></div><div class="mini-item"><strong>API token</strong><span>{"подключен" if has_api else "не подключен"}</span></div><div class="mini-item"><strong>Сегодня</strong><span>Списано запросов: {int(stats_row.get("charged_today_count") or 0)}</span></div></div>'
                '</section>'
                '<section class="card side-panel">'
                '<span class="eyebrow">Быстрые действия</span><h2>Поиск и материалы</h2>'
                '<details class="tool-panel" open><summary>Быстрый поиск</summary><form method="post" action="/search" class="stack compact-form"><input name="query" placeholder="Ключевые слова, люди, компании" required><button type="submit">Искать</button></form></details>'
                '<details class="tool-panel"><summary>Дата или смена</summary><form method="post" action="/list" class="stack compact-form"><input name="query" placeholder="21-03-2026 или название смены" required><button type="submit">Показать список</button></form></details>'
                '<details class="tool-panel"><summary>Материал по ID</summary><form method="post" action="/file" class="stack compact-form"><input name="item_id" inputmode="numeric" placeholder="Например 123" required><button type="submit">Показать материал</button></form><p class="muted">Сайт показывает извлеченную информацию, а не сам файл.</p></details>'
                '<details class="tool-panel"><summary>Промокод</summary><form method="post" action="/promo" class="stack compact-form"><input name="code" placeholder="Введите промокод" required><button type="submit">Активировать</button></form></details>'
                '</section>'
                f'{action_html}'
                '<section class="card side-panel">'
                '<span class="eyebrow">Дополнительно</span><h2>Команды, смены и события</h2>'
                f'<details class="tool-panel" open><summary>Кастомные команды</summary>{commands_html}</details>'
                f'<details class="tool-panel"><summary>Смены</summary>{shifts_html}</details>'
                f'<details class="tool-panel"><summary>Последние события</summary>{events_html}</details>'
                '</section>'
                '</aside>'
                '</section>'
            )

        return (
            f"{self._head_html('Рабочее пространство')}"
            '<body class="site"><main class="page">'
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Рабочее пространство</span>'
            '<h1>Letovo Assistant</h1>'
            f'<p class="lead">{escape(context.subtitle)}</p>'
            '<p class="muted">Главная страница сайта: чат, быстрый поиск, работа со сменами, история запросов и доступ к общей RAG-памяти материалов.</p>'
            '</div>'
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Пользователь</span><strong>{escape(session.display_name)}</strong><span class="meta-note">@{escape(session.username or "site-user")}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department or "не выбран")}</strong><span class="meta-note">Профиль: {escape(prompt_profile_label)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">API token</span><strong>{"подключен" if has_api else "не подключен"}</strong><span class="meta-note">AI-настройки на отдельной странице</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Лимиты</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            '</div>'
            '</section>'
            f'{self._render_public_nav("dashboard")}'
            f'{notice_html}{error_html}{content_html}'
            '</main></body></html>'
        )

    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound("/app")
        response.set_cookie(
            self.SESSION_COOKIE_NAME,
            session_id,
            httponly=True,
            samesite="Lax",
            path="/",
            secure=self._cookie_secure(),
        )
        raise response

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Чат</span><h2>Один рабочий экран</h2><p>Диалог, поиск по материалам, смены, история ответов и быстрые действия собраны в одном интерфейсе.</p><p class="muted">Сайт одинаково удобно работает на телефоне и на компьютере.</p></section>'
            '<section class="entry vk"><span class="pill">Настройки</span><h2>Глубокая кастомизация</h2><p>Отдельные страницы для профиля, AI-настроек, API token, prompt и персонального режима работы.</p><p class="muted">Вход и регистрация сайта полностью отдельные от ботов.</p></section>'
            '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с администрацией</h2><p>Если что-то сломалось, можно открыть отдельный диалог поддержки, который сразу виден команде проекта.</p><p class="muted">Вся история обращений сохраняется прямо в базе сайта.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            '<body class="site home"><main class="page">'
            '<section class="hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Веб-платформа</span>'
            '<h1>Letovo Assistant</h1>'
            '<p class="lead">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>'
            f'<p class="muted">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Сайт использует отдельные логин и пароль, а общими с ботами остаются только база материалов и RAG-память.</p>'
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
            '</div>'
            '<div class="hero-side summary">'
            '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены и поддержка внутри сайта.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск общие с ботами через одну базу.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Сайт не синхронизирует пользователей с ботами.</span></div>'
            '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и профиль ответа вынесены в отдельный раздел.</span></div>'
            '</div>'
            '</section>'
            f'<section class="entry-grid">{feature_cards}</section>'
            '</main></body></html>'
        )

    def _render_public_nav(self, active: str) -> str:
        items = [
            ("dashboard", "/app", "Чат"),
            ("settings", "/settings", "Настройки"),
            ("settings-api", "/settings/api", "AI и API"),
            ("support", "/support", "Поддержка"),
        ]
        links = []
        for key, href, label in items:
            if active == key:
                links.append(f'<span class="pill">{escape(label)}</span>')
            else:
                links.append(f'<a class="ghost-link" href="{href}">{escape(label)}</a>')
        return (
            '<div class="toolbar">'
            f'<div class="switcher">{"".join(links)}</div>'
            '<div class="toolbar">'
            '<a class="ghost-link" href="/">Главная</a>'
            '<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>'
            "</div>"
            "</div>"
        )

    def _render_command_buttons(self, context: PublicPlatformContext, commands: list[dict[str, Any]]) -> str:
        if not commands:
            return '<p class="muted">Дополнительные команды пока не настроены.</p>'
        buttons = []
        for row in commands[:18]:
            command_name = str(row.get("command_name") or "").strip()
            if not command_name:
                continue
            buttons.append(
                '<form method="post" action="/command/run" class="command-form">'
                f'<input type="hidden" name="command_name" value="{escape(command_name)}">'
                f'<button type="submit" class="ghost">{escape(command_name)}</button>'
                '</form>'
            )
        return f'<div class="command-grid">{"".join(buttons)}</div>'

    def _render_department_action_card(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        action: dict[str, str] | None,
    ) -> str:
        user_department = context.app_service.get_user_department(session.user_id)
        if not action and user_department != "проект 11":
            return ""
        if user_department == "проект 11":
            options_html = "".join(
                f'<option value="{escape(label)}">{escape(label)}</option>'
                for label in context.app_service.all_department_action_labels()
            )
            return (
                '<section class="card primary">'
                '<span class="eyebrow">Проект 11</span>'
                '<h2>Любой спец-режим на сегодня</h2>'
                f'<p class="muted">{escape(context.app_service.department_action_picker_prompt())}</p>'
                '<form method="post" action="/department/action" class="stack">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Режим<select name="action_label" required>{options_html}</select></label>'
                '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
                '<button type="submit">Запустить анализ</button>'
                '</form>'
                '</section>'
            )
        if action is None:
            return ""
        return (
            '<section class="card primary">'
            f'<span class="eyebrow">{escape(str(action.get("department") or ""))}</span>'
            f'<h2>{escape(str(action.get("title") or "Спец-режим"))}</h2>'
            '<p class="muted">Базовый лимит этого режима обновляется каждый день. Если для вас доступны дополнительные спец-запросы, они будут учтены автоматически.</p>'
            '<form method="post" action="/department/action" class="stack">'
            '<input type="hidden" name="return_to" value="dashboard">'
            '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
            f'<button type="submit">{escape(str(action.get("button") or "Запустить"))}</button>'
            '</form>'
            '</section>'
        )

    def _render_managed_choice_card(
        self,
        context: PublicPlatformContext,
        pending: ManagedAnswerChoice | None,
    ) -> str:
        if pending is None:
            return ""
        buttons_html = "".join(
            '<form method="post" action="/managed-answer" class="choice-form">'
            f'<input type="hidden" name="option_id" value="{option.option_id}">'
            f'<button type="submit">{escape(option.option_label)}</button>'
            '</form>'
            for option in pending.options
        )
        return (
            '<section class="card spotlight managed-choice">'
            '<span class="eyebrow">Готовые ответы</span>'
            '<h2>Выберите вариант</h2>'
            f'<p class="muted">Вопрос: {escape(pending.question)}</p>'
            f'<div class="choice-grid">{buttons_html}</div>'
            '</section>'
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        action = context.app_service.department_action_for_user(session.user_id)
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)

        if banned:
            reason_text = ban_reason or "Причина не указана."
            content_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, откройте страницу поддержки и опишите ситуацию.</p>'
                '</section>'
            )
        elif not department:
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Обязательный шаг</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">После выбора департамента сайт активирует правильные лимиты, профиль ответов и специальные режимы именно для вашего рабочего пространства.</p>'
                '<form method="post" action="/department/save" class="grid one">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить и открыть чат</button></div>'
                '</form>'
                '</section>'
            )
        else:
            chat_html = self._render_chat_panel(context, session)
            result_panel = self._render_result_panel(session)
            commands_html = self._render_command_buttons(context, custom_commands)
            shifts_html = self._render_shift_list(shifts)
            events_html = self._render_event_list(recent_events)
            action_html = self._render_department_action_card(context, session, action)
            content_html = (
                '<section class="workspace">'
                '<section class="chat-column">'
                '<section class="card chat-shell">'
                '<div class="chat-shell-head"><div><span class="eyebrow">Чат</span><h2>Главный диалог</h2><p class="muted">Задавайте вопросы по материалам, ищите факты по датам и сменам, запускайте готовые сценарии и получайте текстовые ответы прямо в ленте.</p></div>'
                f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний</small></span></div></div>'
                '<form method="post" action="/ask" class="chat-composer">'
                '<textarea name="question" rows="4" placeholder="Например: что происходило в смене 01-07-2025..11-07-2025 и есть ли материалы по World News 24?" required></textarea>'
                '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
                '</form>'
                f'{managed_choice_html}'
                f'{chat_html}'
                '</section>'
                f'{result_panel}'
                '</section>'
                '<aside class="sidebar-column">'
                '<section class="card side-panel sticky-panel">'
                '<span class="eyebrow">Сводка</span><h2>Рабочая панель</h2>'
                f'<div class="mini-list"><div class="mini-item"><strong>Департамент</strong><span>{escape(department)}</span></div><div class="mini-item"><strong>Prompt профиль</strong><span>{escape(prompt_profile_label)}</span></div><div class="mini-item"><strong>API token</strong><span>{"подключен" if has_api else "не подключен"}</span></div><div class="mini-item"><strong>Сегодня</strong><span>Списано запросов: {int(stats_row.get("charged_today_count") or 0)}</span></div></div>'
                '</section>'
                '<section class="card side-panel">'
                '<span class="eyebrow">Быстрые действия</span><h2>Поиск и материалы</h2>'
                '<details class="tool-panel" open><summary>Быстрый поиск</summary><form method="post" action="/search" class="stack compact-form"><input name="query" placeholder="Ключевые слова, люди, компании" required><button type="submit">Искать</button></form></details>'
                '<details class="tool-panel"><summary>Дата или смена</summary><form method="post" action="/list" class="stack compact-form"><input name="query" placeholder="21-03-2026 или название смены" required><button type="submit">Показать список</button></form></details>'
                '<details class="tool-panel"><summary>Материал по ID</summary><form method="post" action="/file" class="stack compact-form"><input name="item_id" inputmode="numeric" placeholder="Например 123" required><button type="submit">Показать материал</button></form><p class="muted">Сайт показывает извлеченную информацию, а не сам файл.</p></details>'
                '<details class="tool-panel"><summary>Промокод</summary><form method="post" action="/promo" class="stack compact-form"><input name="code" placeholder="Введите промокод" required><button type="submit">Активировать</button></form></details>'
                '</section>'
                f'{action_html}'
                '<section class="card side-panel">'
                '<span class="eyebrow">Дополнительно</span><h2>Команды, смены и события</h2>'
                f'<details class="tool-panel" open><summary>Кастомные команды</summary>{commands_html}</details>'
                f'<details class="tool-panel"><summary>Смены</summary>{shifts_html}</details>'
                f'<details class="tool-panel"><summary>Последние события</summary>{events_html}</details>'
                '</section>'
                '</aside>'
                '</section>'
            )

        return (
            f"{self._head_html('Рабочее пространство')}"
            '<body class="site"><main class="page">'
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Рабочее пространство</span>'
            '<h1>Letovo Assistant</h1>'
            f'<p class="lead">{escape(context.subtitle)}</p>'
            '<p class="muted">Главная страница сайта: чат, быстрый поиск, работа со сменами, история запросов и доступ к общей RAG-памяти материалов.</p>'
            '</div>'
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Пользователь</span><strong>{escape(session.display_name)}</strong><span class="meta-note">@{escape(session.username or "site-user")}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department or "не выбран")}</strong><span class="meta-note">Профиль: {escape(prompt_profile_label)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">API token</span><strong>{"подключен" if has_api else "не подключен"}</strong><span class="meta-note">AI-настройки на отдельной странице</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Лимиты</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            '</div>'
            '</section>'
            f'{self._render_public_nav("dashboard")}'
            f'{notice_html}{error_html}{content_html}'
            '</main></body></html>'
        )

    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound("/app")
        response.set_cookie(
            self.SESSION_COOKIE_NAME,
            session_id,
            httponly=True,
            samesite="Lax",
            path="/",
            secure=self._cookie_secure(),
        )
        raise response

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Чат</span><h2>Один рабочий экран</h2><p>Диалог, поиск по материалам, смены, история ответов и быстрые действия собраны в одном интерфейсе.</p><p class="muted">Сайт одинаково удобно работает на телефоне и на компьютере.</p></section>'
            '<section class="entry vk"><span class="pill">Настройки</span><h2>Глубокая кастомизация</h2><p>Отдельные страницы для профиля, AI-настроек, API token, prompt и персонального режима работы.</p><p class="muted">Вход и регистрация сайта полностью отдельные от ботов.</p></section>'
            '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с администрацией</h2><p>Если что-то сломалось, можно открыть отдельный диалог поддержки, который сразу виден команде проекта.</p><p class="muted">Вся история обращений сохраняется прямо в базе сайта.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            '<body class="site home"><main class="page">'
            '<section class="hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Веб-платформа</span>'
            '<h1>Letovo Assistant</h1>'
            '<p class="lead">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>'
            f'<p class="muted">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Сайт использует отдельные логин и пароль, а общими с ботами остаются только база материалов и RAG-память.</p>'
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
            '</div>'
            '<div class="hero-side summary">'
            '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены и поддержка внутри сайта.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск общие с ботами через одну базу.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Сайт не синхронизирует пользователей с ботами.</span></div>'
            '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и профиль ответа вынесены в отдельный раздел.</span></div>'
            '</div>'
            '</section>'
            f'<section class="entry-grid">{feature_cards}</section>'
            '</main></body></html>'
        )

    def _render_public_nav(self, active: str) -> str:
        items = [
            ("dashboard", "/app", "Чат"),
            ("settings", "/settings", "Настройки"),
            ("settings-api", "/settings/api", "AI и API"),
            ("support", "/support", "Поддержка"),
        ]
        links = []
        for key, href, label in items:
            if active == key:
                links.append(f'<span class="pill">{escape(label)}</span>')
            else:
                links.append(f'<a class="ghost-link" href="{href}">{escape(label)}</a>')
        return (
            '<div class="toolbar">'
            f'<div class="switcher">{"".join(links)}</div>'
            '<div class="toolbar">'
            '<a class="ghost-link" href="/">Главная</a>'
            '<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>'
            "</div>"
            "</div>"
        )

    def _render_command_buttons(self, context: PublicPlatformContext, commands: list[dict[str, Any]]) -> str:
        if not commands:
            return '<p class="muted">Дополнительные команды пока не настроены.</p>'
        buttons = []
        for row in commands[:18]:
            command_name = str(row.get("command_name") or "").strip()
            if not command_name:
                continue
            buttons.append(
                '<form method="post" action="/command/run" class="command-form">'
                f'<input type="hidden" name="command_name" value="{escape(command_name)}">'
                f'<button type="submit" class="ghost">{escape(command_name)}</button>'
                '</form>'
            )
        return f'<div class="command-grid">{"".join(buttons)}</div>'

    def _render_department_action_card(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        action: dict[str, str] | None,
    ) -> str:
        user_department = context.app_service.get_user_department(session.user_id)
        if not action and user_department != "проект 11":
            return ""
        if user_department == "проект 11":
            options_html = "".join(
                f'<option value="{escape(label)}">{escape(label)}</option>'
                for label in context.app_service.all_department_action_labels()
            )
            return (
                '<section class="card primary">'
                '<span class="eyebrow">Проект 11</span>'
                '<h2>Любой спец-режим на сегодня</h2>'
                f'<p class="muted">{escape(context.app_service.department_action_picker_prompt())}</p>'
                '<form method="post" action="/department/action" class="stack">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Режим<select name="action_label" required>{options_html}</select></label>'
                '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
                '<button type="submit">Запустить анализ</button>'
                '</form>'
                '</section>'
            )
        if action is None:
            return ""
        return (
            '<section class="card primary">'
            f'<span class="eyebrow">{escape(str(action.get("department") or ""))}</span>'
            f'<h2>{escape(str(action.get("title") or "Спец-режим"))}</h2>'
            '<p class="muted">Базовый лимит этого режима обновляется каждый день. Если для вас доступны дополнительные спец-запросы, они будут учтены автоматически.</p>'
            '<form method="post" action="/department/action" class="stack">'
            '<input type="hidden" name="return_to" value="dashboard">'
            '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
            f'<button type="submit">{escape(str(action.get("button") or "Запустить"))}</button>'
            '</form>'
            '</section>'
        )

    def _render_managed_choice_card(
        self,
        context: PublicPlatformContext,
        pending: ManagedAnswerChoice | None,
    ) -> str:
        if pending is None:
            return ""
        buttons_html = "".join(
            '<form method="post" action="/managed-answer" class="choice-form">'
            f'<input type="hidden" name="option_id" value="{option.option_id}">'
            f'<button type="submit">{escape(option.option_label)}</button>'
            '</form>'
            for option in pending.options
        )
        return (
            '<section class="card spotlight managed-choice">'
            '<span class="eyebrow">Готовые ответы</span>'
            '<h2>Выберите вариант</h2>'
            f'<p class="muted">Вопрос: {escape(pending.question)}</p>'
            f'<div class="choice-grid">{buttons_html}</div>'
            '</section>'
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        action = context.app_service.department_action_for_user(session.user_id)
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)

        if banned:
            reason_text = ban_reason or "Причина не указана."
            content_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, откройте страницу поддержки и опишите ситуацию.</p>'
                '</section>'
            )
        elif not department:
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Обязательный шаг</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">После выбора департамента сайт активирует правильные лимиты, профиль ответов и специальные режимы именно для вашего рабочего пространства.</p>'
                '<form method="post" action="/department/save" class="grid one">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить и открыть чат</button></div>'
                '</form>'
                '</section>'
            )
        else:
            chat_html = self._render_chat_panel(context, session)
            result_panel = self._render_result_panel(session)
            commands_html = self._render_command_buttons(context, custom_commands)
            shifts_html = self._render_shift_list(shifts)
            events_html = self._render_event_list(recent_events)
            action_html = self._render_department_action_card(context, session, action)
            content_html = (
                '<section class="workspace">'
                '<section class="chat-column">'
                '<section class="card chat-shell">'
                '<div class="chat-shell-head"><div><span class="eyebrow">Чат</span><h2>Главный диалог</h2><p class="muted">Задавайте вопросы по материалам, ищите факты по датам и сменам, запускайте готовые сценарии и получайте текстовые ответы прямо в ленте.</p></div>'
                f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний</small></span></div></div>'
                '<form method="post" action="/ask" class="chat-composer">'
                '<textarea name="question" rows="4" placeholder="Например: что происходило в смене 01-07-2025..11-07-2025 и есть ли материалы по World News 24?" required></textarea>'
                '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
                '</form>'
                f'{managed_choice_html}'
                f'{chat_html}'
                '</section>'
                f'{result_panel}'
                '</section>'
                '<aside class="sidebar-column">'
                '<section class="card side-panel sticky-panel">'
                '<span class="eyebrow">Сводка</span><h2>Рабочая панель</h2>'
                f'<div class="mini-list"><div class="mini-item"><strong>Департамент</strong><span>{escape(department)}</span></div><div class="mini-item"><strong>Prompt профиль</strong><span>{escape(prompt_profile_label)}</span></div><div class="mini-item"><strong>API token</strong><span>{"подключен" if has_api else "не подключен"}</span></div><div class="mini-item"><strong>Сегодня</strong><span>Списано запросов: {int(stats_row.get("charged_today_count") or 0)}</span></div></div>'
                '</section>'
                '<section class="card side-panel">'
                '<span class="eyebrow">Быстрые действия</span><h2>Поиск и материалы</h2>'
                '<details class="tool-panel" open><summary>Быстрый поиск</summary><form method="post" action="/search" class="stack compact-form"><input name="query" placeholder="Ключевые слова, люди, компании" required><button type="submit">Искать</button></form></details>'
                '<details class="tool-panel"><summary>Дата или смена</summary><form method="post" action="/list" class="stack compact-form"><input name="query" placeholder="21-03-2026 или название смены" required><button type="submit">Показать список</button></form></details>'
                '<details class="tool-panel"><summary>Материал по ID</summary><form method="post" action="/file" class="stack compact-form"><input name="item_id" inputmode="numeric" placeholder="Например 123" required><button type="submit">Показать материал</button></form><p class="muted">Сайт показывает извлеченную информацию, а не сам файл.</p></details>'
                '<details class="tool-panel"><summary>Промокод</summary><form method="post" action="/promo" class="stack compact-form"><input name="code" placeholder="Введите промокод" required><button type="submit">Активировать</button></form></details>'
                '</section>'
                f'{action_html}'
                '<section class="card side-panel">'
                '<span class="eyebrow">Дополнительно</span><h2>Команды, смены и события</h2>'
                f'<details class="tool-panel" open><summary>Кастомные команды</summary>{commands_html}</details>'
                f'<details class="tool-panel"><summary>Смены</summary>{shifts_html}</details>'
                f'<details class="tool-panel"><summary>Последние события</summary>{events_html}</details>'
                '</section>'
                '</aside>'
                '</section>'
            )

        return (
            f"{self._head_html('Рабочее пространство')}"
            '<body class="site"><main class="page">'
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Рабочее пространство</span>'
            '<h1>Letovo Assistant</h1>'
            f'<p class="lead">{escape(context.subtitle)}</p>'
            '<p class="muted">Главная страница сайта: чат, быстрый поиск, работа со сменами, история запросов и доступ к общей RAG-памяти материалов.</p>'
            '</div>'
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Пользователь</span><strong>{escape(session.display_name)}</strong><span class="meta-note">@{escape(session.username or "site-user")}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department or "не выбран")}</strong><span class="meta-note">Профиль: {escape(prompt_profile_label)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">API token</span><strong>{"подключен" if has_api else "не подключен"}</strong><span class="meta-note">AI-настройки на отдельной странице</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Лимиты</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            '</div>'
            '</section>'
            f'{self._render_public_nav("dashboard")}'
            f'{notice_html}{error_html}{content_html}'
            '</main></body></html>'
        )

    async def _handle_login(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        if not username:
            return web.Response(
                text=self._render_login_page(error_text="Введите логин сайта.", username=username),
                content_type="text/html",
                status=400,
            )
        if not password:
            return web.Response(
                text=self._render_login_page(error_text="Введите пароль сайта.", username=username),
                content_type="text/html",
                status=400,
            )

        context, account = self._find_site_account(username)
        if context is None or account is None:
            return web.Response(
                text=self._render_login_page(
                    error_text="Такой сайт-аккаунт не найден или еще не активирован.",
                    username=username,
                ),
                content_type="text/html",
                status=403,
            )
        if not verify_password(password, str(account.get("password_hash") or "")):
            return web.Response(
                text=self._render_login_page(error_text="Неверный логин или пароль сайта.", username=username),
                content_type="text/html",
                status=403,
            )

        try:
            user_id = int(account.get("platform_user_id") or 0)
        except (TypeError, ValueError):
            user_id = 0
        if user_id == 0:
            return web.Response(
                text=self._render_login_page(
                    error_text="Аккаунт сайта настроен некорректно. Обратитесь к команде проекта.",
                    username=username,
                ),
                content_type="text/html",
                status=400,
            )

        banned, ban_reason = context.app_service.is_banned(user_id)
        if banned:
            reason_text = f"Причина: {ban_reason}" if ban_reason else "Доступ заблокирован."
            return web.Response(
                text=self._render_login_page(error_text=reason_text, username=username),
                content_type="text/html",
                status=403,
            )

        resolved_name = str(account.get("display_name") or "").strip() or self._resolve_display_name(context, user_id)
        return self._open_public_session(
            context=context,
            user_id=user_id,
            display_name=resolved_name,
            username=username,
            notice_text="Вход выполнен. Рабочее пространство сайта готово.",
        )

    async def _handle_register(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        password_repeat = fields.get("password_repeat", "").strip()
        display_name = fields.get("display_name", "").strip()
        context = self._site_context()

        if len(username) < 3:
            return web.Response(
                text=self._render_register_page(
                    error_text="Логин должен содержать минимум 3 символа.",
                    username=username,
                    display_name=display_name,
                ),
                content_type="text/html",
                status=400,
            )
        if any(ch.isspace() for ch in username):
            return web.Response(
                text=self._render_register_page(
                    error_text="Логин не должен содержать пробелы.",
                    username=username,
                    display_name=display_name,
                ),
                content_type="text/html",
                status=400,
            )
        if len(password) < 8:
            return web.Response(
                text=self._render_register_page(
                    error_text="Пароль должен содержать минимум 8 символов.",
                    username=username,
                    display_name=display_name,
                ),
                content_type="text/html",
                status=400,
            )
        if password != password_repeat:
            return web.Response(
                text=self._render_register_page(
                    error_text="Пароли не совпадают. Регистрация не выполнена.",
                    username=username,
                    display_name=display_name,
                ),
                content_type="text/html",
                status=400,
            )

        existing_context, existing_account = self._find_site_account_any(username)
        if existing_context is not None and existing_account is not None:
            return web.Response(
                text=self._render_register_page(
                    error_text="Такой логин уже занят.",
                    username=username,
                    display_name=display_name,
                ),
                content_type="text/html",
                status=409,
            )

        user_id = context.app_service.next_site_platform_user_id()
        resolved_name = display_name or f"Пользователь {username}"
        context.app_service.upsert_site_account(
            username=username,
            password_hash=hash_password(password),
            display_name=resolved_name,
            platform_user_id=user_id,
            is_active=True,
        )
        context.app_service.log_event(
            user_id=user_id,
            chat_id=user_id,
            event_type="site_registration",
            sender_profile=SenderProfile(first_name=resolved_name),
            details={"platform": context.slug, "surface": "web"},
        )
        return self._open_public_session(
            context=context,
            user_id=user_id,
            display_name=resolved_name,
            username=username,
            notice_text="Регистрация завершена. Аккаунт сайта готов к работе.",
        )

    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound("/app")
        response.set_cookie(
            self.SESSION_COOKIE_NAME,
            session_id,
            httponly=True,
            samesite="Lax",
            path="/",
            secure=self._cookie_secure(),
        )
        raise response

    def _render_public_nav(self, active: str) -> str:
        items = [
            ("dashboard", "/app", "Чат"),
            ("settings", "/settings", "Настройки"),
            ("settings-api", "/settings/api", "AI и API"),
            ("support", "/support", "Поддержка"),
        ]
        links = []
        for key, href, label in items:
            if active == key:
                links.append(f'<span class="pill">{escape(label)}</span>')
            else:
                links.append(f'<a class="ghost-link" href="{href}">{escape(label)}</a>')
        return (
            '<div class="toolbar">'
            f'<div class="switcher">{"".join(links)}</div>'
            '<div class="toolbar">'
            '<a class="ghost-link" href="/">Главная</a>'
            '<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>'
            "</div>"
            "</div>"
        )

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Чат</span><h2>Один рабочий экран</h2><p>Диалог, поиск по материалам, смены, история ответов и быстрые действия собраны в одном интерфейсе.</p><p class="muted">Сайт одинаково удобно работает на телефоне и на компьютере.</p></section>'
            '<section class="entry vk"><span class="pill">Настройки</span><h2>Глубокая кастомизация</h2><p>Отдельные страницы для профиля, AI-настроек, API token, prompt и персонального режима работы.</p><p class="muted">Вход и регистрация сайта полностью отдельные от ботов.</p></section>'
            '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с администрацией</h2><p>Если что-то сломалось, можно открыть отдельный диалог поддержки, который сразу виден команде проекта.</p><p class="muted">Вся история обращений сохраняется прямо в базе сайта.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            '<body class="site home"><main class="page">'
            '<section class="hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Веб-платформа</span>'
            '<h1>Letovo Assistant</h1>'
            '<p class="lead">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>'
            f'<p class="muted">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Сайт использует отдельные логин и пароль, а общими с ботами остаются только база материалов и RAG-память.</p>'
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
            '</div>'
            '<div class="hero-side summary">'
            '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены и поддержка внутри сайта.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск общие с ботами через одну базу.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Сайт не синхронизирует пользователей с ботами.</span></div>'
            '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и профиль ответа вынесены в отдельный раздел.</span></div>'
            '</div>'
            '</section>'
            f'<section class="entry-grid">{feature_cards}</section>'
            '</main></body></html>'
        )

    def _render_command_buttons(self, context: PublicPlatformContext, commands: list[dict[str, Any]]) -> str:
        if not commands:
            return '<p class="muted">Дополнительные команды пока не настроены.</p>'
        buttons = []
        for row in commands[:18]:
            command_name = str(row.get("command_name") or "").strip()
            if not command_name:
                continue
            buttons.append(
                '<form method="post" action="/command/run" class="command-form">'
                f'<input type="hidden" name="command_name" value="{escape(command_name)}">'
                f'<button type="submit" class="ghost">{escape(command_name)}</button>'
                '</form>'
            )
        return f'<div class="command-grid">{"".join(buttons)}</div>'

    def _render_department_action_card(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        action: dict[str, str] | None,
    ) -> str:
        user_department = context.app_service.get_user_department(session.user_id)
        if not action and user_department != "проект 11":
            return ""
        if user_department == "проект 11":
            options_html = "".join(
                f'<option value="{escape(label)}">{escape(label)}</option>'
                for label in context.app_service.all_department_action_labels()
            )
            return (
                '<section class="card primary">'
                '<span class="eyebrow">Проект 11</span>'
                '<h2>Любой спец-режим на сегодня</h2>'
                f'<p class="muted">{escape(context.app_service.department_action_picker_prompt())}</p>'
                '<form method="post" action="/department/action" class="stack">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Режим<select name="action_label" required>{options_html}</select></label>'
                '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
                '<button type="submit">Запустить анализ</button>'
                '</form>'
                '</section>'
            )
        if action is None:
            return ""
        return (
            '<section class="card primary">'
            f'<span class="eyebrow">{escape(str(action.get("department") or ""))}</span>'
            f'<h2>{escape(str(action.get("title") or "Спец-режим"))}</h2>'
            '<p class="muted">Базовый лимит этого режима обновляется каждый день. Если для вас доступны дополнительные спец-запросы, они будут учтены автоматически.</p>'
            '<form method="post" action="/department/action" class="stack">'
            '<input type="hidden" name="return_to" value="dashboard">'
            '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
            f'<button type="submit">{escape(str(action.get("button") or "Запустить"))}</button>'
            '</form>'
            '</section>'
        )

    def _render_managed_choice_card(
        self,
        context: PublicPlatformContext,
        pending: ManagedAnswerChoice | None,
    ) -> str:
        if pending is None:
            return ""
        buttons_html = "".join(
            '<form method="post" action="/managed-answer" class="choice-form">'
            f'<input type="hidden" name="option_id" value="{option.option_id}">'
            f'<button type="submit">{escape(option.option_label)}</button>'
            '</form>'
            for option in pending.options
        )
        return (
            '<section class="card spotlight managed-choice">'
            '<span class="eyebrow">Готовые ответы</span>'
            '<h2>Выберите вариант</h2>'
            f'<p class="muted">Вопрос: {escape(pending.question)}</p>'
            f'<div class="choice-grid">{buttons_html}</div>'
            '</section>'
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        action = context.app_service.department_action_for_user(session.user_id)
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)
        blocked_html = ""

        if banned:
            reason_text = ban_reason or "Причина не указана."
            content_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, откройте страницу поддержки и опишите ситуацию.</p>'
                '</section>'
            )
        elif not department:
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Обязательный шаг</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">После выбора департамента сайт активирует правильные лимиты, профиль ответов и специальные режимы именно для вашего рабочего пространства.</p>'
                '<form method="post" action="/department/save" class="grid one">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить и открыть чат</button></div>'
                '</form>'
                '</section>'
            )
        else:
            chat_html = self._render_chat_panel(context, session)
            result_panel = self._render_result_panel(session)
            commands_html = self._render_command_buttons(context, custom_commands)
            shifts_html = self._render_shift_list(shifts)
            events_html = self._render_event_list(recent_events)
            action_html = self._render_department_action_card(context, session, action)
            content_html = (
                '<section class="workspace">'
                '<section class="chat-column">'
                '<section class="card chat-shell">'
                '<div class="chat-shell-head"><div><span class="eyebrow">Чат</span><h2>Главный диалог</h2><p class="muted">Задавайте вопросы по материалам, ищите факты по датам и сменам, запускайте готовые сценарии и получайте текстовые ответы прямо в ленте.</p></div>'
                f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний</small></span></div></div>'
                '<form method="post" action="/ask" class="chat-composer">'
                '<textarea name="question" rows="4" placeholder="Например: что происходило в смене 01-07-2025..11-07-2025 и есть ли материалы по World News 24?" required></textarea>'
                '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
                '</form>'
                f'{managed_choice_html}'
                f'{chat_html}'
                '</section>'
                f'{result_panel}'
                '</section>'
                '<aside class="sidebar-column">'
                '<section class="card side-panel sticky-panel">'
                '<span class="eyebrow">Сводка</span><h2>Рабочая панель</h2>'
                f'<div class="mini-list"><div class="mini-item"><strong>Департамент</strong><span>{escape(department)}</span></div><div class="mini-item"><strong>Prompt профиль</strong><span>{escape(prompt_profile_label)}</span></div><div class="mini-item"><strong>API token</strong><span>{"подключен" if has_api else "не подключен"}</span></div><div class="mini-item"><strong>Сегодня</strong><span>Списано запросов: {int(stats_row.get("charged_today_count") or 0)}</span></div></div>'
                '</section>'
                '<section class="card side-panel">'
                '<span class="eyebrow">Быстрые действия</span><h2>Поиск и материалы</h2>'
                '<details class="tool-panel" open><summary>Быстрый поиск</summary><form method="post" action="/search" class="stack compact-form"><input name="query" placeholder="Ключевые слова, люди, компании" required><button type="submit">Искать</button></form></details>'
                '<details class="tool-panel"><summary>Дата или смена</summary><form method="post" action="/list" class="stack compact-form"><input name="query" placeholder="21-03-2026 или название смены" required><button type="submit">Показать список</button></form></details>'
                '<details class="tool-panel"><summary>Материал по ID</summary><form method="post" action="/file" class="stack compact-form"><input name="item_id" inputmode="numeric" placeholder="Например 123" required><button type="submit">Показать материал</button></form><p class="muted">Сайт показывает извлеченную информацию, а не сам файл.</p></details>'
                '<details class="tool-panel"><summary>Промокод</summary><form method="post" action="/promo" class="stack compact-form"><input name="code" placeholder="Введите промокод" required><button type="submit">Активировать</button></form></details>'
                '</section>'
                f'{action_html}'
                '<section class="card side-panel">'
                '<span class="eyebrow">Дополнительно</span><h2>Команды, смены и события</h2>'
                f'<details class="tool-panel" open><summary>Кастомные команды</summary>{commands_html}</details>'
                f'<details class="tool-panel"><summary>Смены</summary>{shifts_html}</details>'
                f'<details class="tool-panel"><summary>Последние события</summary>{events_html}</details>'
                '</section>'
                '</aside>'
                '</section>'
            )

        return (
            f"{self._head_html('Рабочее пространство')}"
            '<body class="site"><main class="page">'
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Рабочее пространство</span>'
            '<h1>Letovo Assistant</h1>'
            f'<p class="lead">{escape(context.subtitle)}</p>'
            '<p class="muted">Главная страница сайта: чат, быстрый поиск, работа со сменами, история запросов и доступ к общей RAG-памяти материалов.</p>'
            '</div>'
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Пользователь</span><strong>{escape(session.display_name)}</strong><span class="meta-note">@{escape(session.username or "site-user")}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department or "не выбран")}</strong><span class="meta-note">Профиль: {escape(prompt_profile_label)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">API token</span><strong>{"подключен" if has_api else "не подключен"}</strong><span class="meta-note">AI-настройки на отдельной странице</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Лимиты</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            '</div>'
            '</section>'
            f'{self._render_public_nav("dashboard")}'
            f'{notice_html}{error_html}{blocked_html}{content_html}'
            '</main></body></html>'
        )

    async def _handle_support_send(self, request: web.Request) -> web.StreamResponse:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        message_text = fields.get("message", "").strip()
        if not session.username:
            self._set_error(session, "Сессия сайта устарела. Войдите заново, чтобы написать в поддержку.")
            raise web.HTTPFound("/login")
        if not message_text:
            self._set_error(session, "Опишите проблему, вопрос или ошибку.")
            return self._support_response(context, session, status=400)
        context.app_service.create_site_support_message(
            username=session.username,
            site_user_id=session.user_id,
            display_name=session.display_name,
            sender_role="user",
            message_text=message_text,
        )
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="site_support_message",
            sender_profile=self._sender_profile(session),
            details={"surface": "web", "username": session.username, "message": message_text[:500]},
        )
        self._set_notice(session, "Сообщение отправлено администрации.")
        raise web.HTTPFound("/support")

    async def _handle_login(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        if not username:
            return web.Response(text=self._render_login_page(error_text="Введите логин сайта.", username=username), content_type="text/html", status=400)
        if not password:
            return web.Response(text=self._render_login_page(error_text="Введите пароль сайта.", username=username), content_type="text/html", status=400)
        context, account = self._find_site_account(username)
        if context is None or account is None:
            return web.Response(
                text=self._render_login_page(error_text="Такой сайт-аккаунт не найден или еще не активирован.", username=username),
                content_type="text/html",
                status=403,
            )
        if not verify_password(password, str(account.get("password_hash") or "")):
            return web.Response(
                text=self._render_login_page(error_text="Неверный логин или пароль сайта.", username=username),
                content_type="text/html",
                status=403,
            )
        try:
            user_id = int(account.get("platform_user_id") or 0)
        except (TypeError, ValueError):
            user_id = 0
        if user_id == 0:
            return web.Response(
                text=self._render_login_page(error_text="Аккаунт сайта настроен некорректно. Обратитесь к команде проекта.", username=username),
                content_type="text/html",
                status=400,
            )
        banned, ban_reason = context.app_service.is_banned(user_id)
        if banned:
            reason_text = f"Причина: {ban_reason}" if ban_reason else "Доступ заблокирован."
            return web.Response(text=self._render_login_page(error_text=reason_text, username=username), content_type="text/html", status=403)
        resolved_name = str(account.get("display_name") or "").strip() or self._resolve_display_name(context, user_id)
        return self._open_public_session(
            context=context,
            user_id=user_id,
            display_name=resolved_name,
            username=username,
            notice_text="Вход выполнен. Добро пожаловать в рабочее пространство сайта.",
        )

    async def _handle_register(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        password_repeat = fields.get("password_repeat", "").strip()
        display_name = fields.get("display_name", "").strip()
        context = self._site_context()
        if len(username) < 3:
            return web.Response(
                text=self._render_register_page(error_text="Логин должен содержать минимум 3 символа.", username=username, display_name=display_name),
                content_type="text/html",
                status=400,
            )
        if any(ch.isspace() for ch in username):
            return web.Response(
                text=self._render_register_page(error_text="Логин не должен содержать пробелы.", username=username, display_name=display_name),
                content_type="text/html",
                status=400,
            )
        if len(password) < 8:
            return web.Response(
                text=self._render_register_page(error_text="Пароль должен содержать минимум 8 символов.", username=username, display_name=display_name),
                content_type="text/html",
                status=400,
            )
        if password != password_repeat:
            return web.Response(
                text=self._render_register_page(error_text="Пароли не совпадают. Регистрация не выполнена.", username=username, display_name=display_name),
                content_type="text/html",
                status=400,
            )
        existing_context, existing_account = self._find_site_account_any(username)
        if existing_context is not None and existing_account is not None:
            return web.Response(
                text=self._render_register_page(error_text="Такой логин уже занят.", username=username, display_name=display_name),
                content_type="text/html",
                status=409,
            )
        user_id = context.app_service.next_site_platform_user_id()
        resolved_name = display_name or f"Пользователь {username}"
        context.app_service.upsert_site_account(
            username=username,
            password_hash=hash_password(password),
            display_name=resolved_name,
            platform_user_id=user_id,
            is_active=True,
        )
        context.app_service.log_event(
            user_id=user_id,
            chat_id=user_id,
            event_type="site_registration",
            sender_profile=SenderProfile(first_name=resolved_name),
            details={"platform": context.slug, "surface": "web"},
        )
        return self._open_public_session(
            context=context,
            user_id=user_id,
            display_name=resolved_name,
            username=username,
            notice_text="Регистрация завершена. Сайт-аккаунт создан и готов к работе.",
        )

    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound("/app")
        response.set_cookie(self.SESSION_COOKIE_NAME, session_id, httponly=True, samesite="Lax", secure=self._cookie_secure(), path="/")
        raise response

    async def _handle_account_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        display_name = fields.get("display_name", "").strip()
        if not display_name:
            self._set_error(session, "Введите отображаемое имя для сайта.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        updated = context.app_service.update_site_account(session.username, display_name=display_name)
        if not updated:
            self._set_error(session, "Не удалось обновить профиль сайта.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        session.display_name = display_name
        self._set_notice(session, "Профиль сайта обновлен.")
        return self._page_response(context, session, page=fields.get("return_to", "settings"))

    async def _handle_password_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        current_password = fields.get("current_password", "").strip()
        new_password = fields.get("new_password", "").strip()
        new_password_repeat = fields.get("new_password_repeat", "").strip()
        account = self._site_account(context, session)
        if account is None:
            self._set_error(session, "Сайт-аккаунт не найден. Войдите заново.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        if not verify_password(current_password, str(account.get("password_hash") or "")):
            self._set_error(session, "Текущий пароль введен неверно.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        if len(new_password) < 8:
            self._set_error(session, "Новый пароль должен содержать минимум 8 символов.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        if new_password != new_password_repeat:
            self._set_error(session, "Новые пароли не совпадают.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        context.app_service.update_site_account(session.username, password_hash=hash_password(new_password))
        self._set_notice(session, "Пароль сайта обновлен.")
        return self._page_response(context, session, page=fields.get("return_to", "settings"))

    async def _handle_department_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        department_raw = fields.get("department", "").strip()
        department = context.app_service.normalize_department(department_raw)
        if department is None:
            self._set_error(session, "Выберите департамент из списка.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        context.app_service.save_user_department(session.user_id, department)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="department_selected",
            sender_profile=self._sender_profile(session),
            details={"department": department, "surface": "web"},
        )
        self._set_notice(session, f"Департамент сохранен: {department}.")
        return self._page_response(context, session, page=fields.get("return_to", "settings"))

    async def _handle_api_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        if not self._ensure_department_selected(context, session):
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        api_key = fields.get("api_key", "").strip()
        if not api_key:
            self._set_error(session, "Введите API token.")
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_set_api", "surface": "web"},
        )
        ok, error_text = context.app_service.validate_user_api_key(api_key)
        if not ok:
            context.app_service.save_user_api_error(session.user_id, error_text or "unknown error")
            self._set_error(session, f"API token не прошел проверку.\n\nТекст ошибки: {(error_text or 'unknown error')[:400]}")
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        prefs = context.app_service.get_user_preferences(session.user_id)
        has_saved_prompt = bool((prefs.get("custom_prompt") or "").strip())
        context.app_service.save_user_api_key(session.user_id, api_key)
        text = "Ваш API token сохранен и проверен. Для вас включен безлимит."
        if has_saved_prompt:
            text += " Ранее сохраненный prompt снова активирован."
        self._set_notice(session, text)
        return self._page_response(context, session, page=fields.get("return_to", "settings-api"))

    async def _handle_api_delete(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        if not self._ensure_department_selected(context, session):
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_delete_api", "surface": "web"},
        )
        prefs = context.app_service.get_user_preferences(session.user_id)
        had_prompt = bool((prefs.get("custom_prompt") or "").strip())
        context.app_service.clear_user_api_key(session.user_id)
        text = "Ваш API token удален. Безлимит отключен."
        if had_prompt:
            text = "Ваш API token удален. Безлимит отключен, пользовательский prompt сохранен, но не будет применяться, пока вы снова не добавите API token."
        self._set_notice(session, text)
        return self._page_response(context, session, page=fields.get("return_to", "settings-api"))

    async def _handle_prompt_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        if not self._ensure_department_selected(context, session):
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        prompt_text = fields.get("prompt_text", "").strip()
        if not prompt_text:
            self._set_error(session, "Введите пользовательский prompt.")
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_set_prompt", "surface": "web"},
        )
        prefs = context.app_service.get_user_preferences(session.user_id)
        if not context.app_service.get_active_api_key(prefs):
            self._set_error(session, "Сначала добавьте рабочий API token.")
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        context.app_service.save_user_prompt(session.user_id, prompt_text)
        self._set_notice(session, "Ваш пользовательский prompt сохранен.")
        return self._page_response(context, session, page=fields.get("return_to", "settings-api"))

    async def _handle_prompt_delete(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        if not self._ensure_department_selected(context, session):
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_delete_prompt", "surface": "web"},
        )
        context.app_service.clear_user_prompt(session.user_id)
        self._set_notice(session, "Ваш пользовательский prompt удален.")
        return self._page_response(context, session, page=fields.get("return_to", "settings-api"))

    async def _handle_prompt_profile_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        if not self._ensure_department_selected(context, session):
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        profile_label = fields.get("prompt_profile", "").strip()
        normalized = context.app_service.normalize_prompt_profile(profile_label)
        if normalized is None:
            self._set_error(session, "Выберите один из доступных профилей prompt.")
            return self._page_response(context, session, page=fields.get("return_to", "settings-api"), status=400)
        context.app_service.save_user_prompt_profile(session.user_id, normalized)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_prompt_profile", "profile": normalized, "surface": "web"},
        )
        self._set_notice(session, f"Профиль prompt переключен на: {context.app_service.PROMPT_PROFILE_LABELS.get(normalized, normalized)}.")
        return self._page_response(context, session, page=fields.get("return_to", "settings-api"))

    async def _handle_access_request(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        if not self._ensure_department_selected(context, session):
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        request_name = fields.get("request_name", "").strip()
        reason = fields.get("reason", "").strip()
        request_type = fields.get("request_type", "").strip() or "daily_limit"
        if not request_name:
            self._set_error(session, "Укажите имя заявки.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        if not reason:
            self._set_error(session, "Опишите причину заявки.")
            return self._page_response(context, session, page=fields.get("return_to", "settings"), status=400)
        mode_bucket = None
        if request_type == "department_mode":
            mode_bucket = context.app_service.department_mode_bucket_for_user(
                session.user_id,
                context.app_service.get_user_department(session.user_id) or "",
            )
        request_id = context.app_service.create_access_request(
            user_id=session.user_id,
            request_name=request_name,
            reason=reason,
            request_type=request_type,
            mode_bucket=mode_bucket,
        )
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="access_request",
            sender_profile=self._sender_profile(session),
            details={
                "request_id": request_id,
                "reason": reason[:500],
                "request_type": request_type,
                "mode_bucket": mode_bucket or "",
                "surface": "web",
            },
        )
        self._set_notice(session, f"Запрос #{request_id} отправлен.")
        return self._page_response(context, session, page=fields.get("return_to", "settings"))

    def _render_authenticated_shell(
        self,
        *,
        title: str,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        active: str,
        lead: str,
        body_html: str,
    ) -> str:
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id) or "не выбран"
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        return (
            f"{self._head_html(title)}"
            "<body class=\"site\"><main class=\"page\">"
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            '<span class="eyebrow">Letovo Assistant</span>'
            f'<h1>{escape(title)}</h1>'
            f'<p class="lead">{escape(lead)}</p>'
            '<p class="muted">Сайт работает независимо от аккаунтов в ботах: общими остаются только материалы, смены и память поиска.</p>'
            "</div>"
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Аккаунт</span><strong>{escape(session.display_name)}</strong><span class="meta-note">@{escape(session.username or "site-user")}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department)}</strong><span class="meta-note">Профиль: {escape(prompt_profile_label)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">API token</span><strong>{"подключен" if has_api else "не подключен"}</strong><span class="meta-note">AI-настройки на отдельной странице</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Лимиты</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            "</div>"
            "</section>"
            f"{self._render_public_nav(active)}"
            f"{notice_html}{error_html}"
            f"{body_html}"
            "</main></body></html>"
        )

    def _support_response(self, context: PublicPlatformContext, session: PublicSiteSession, *, status: int = 200) -> web.Response:
        messages = context.app_service.list_site_support_messages(session.username, limit=80) if session.username else []
        log_rows: list[str] = []
        if not messages:
            log_rows.append(
                '<article class="chat-row assistant latest"><div class="chat-bubble"><div class="chat-meta"><span>Поддержка</span><span>старт</span></div><div class="chat-title">Диалог готов</div><p>Опишите ошибку, вопрос или пожелание. Ответ администрации появится прямо в этой ленте.</p></div></article>'
            )
        else:
            for message in messages:
                is_admin = str(message.get("sender_role") or "").strip().lower() == "admin"
                label = "Администрация" if is_admin else (session.display_name or "Вы")
                role_class = "assistant" if is_admin else "user"
                extra_class = " latest" if is_admin else ""
                log_rows.append(
                    f'<article class="chat-row {role_class}{extra_class}"><div class="chat-bubble">'
                    f'<div class="chat-meta"><span>{escape(label)}</span><span>{escape(str(message.get("created_at") or "-"))}</span></div>'
                    f'<p>{escape(str(message.get("message_text") or ""))}</p>'
                    '</div></article>'
                )
        body_html = (
            '<section class="workspace">'
            '<section class="chat-column">'
            '<section class="card chat-shell">'
            '<div class="chat-shell-head"><div><span class="eyebrow">Поддержка</span><h2>Связь с администрацией</h2><p class="muted">Если на сайте возникла ошибка или нужна помощь, напишите сюда. Весь диалог виден команде проекта в отдельной админ-панели.</p></div>'
            f'<div class="chat-metrics"><span class="metric"><strong>{len(messages)}</strong><small>сообщений</small></span><span class="metric"><strong>{escape(session.username or "-")}</strong><small>логин</small></span></div></div>'
            f'<div class="chat-log">{"".join(log_rows)}</div>'
            '<form method="post" action="/support/send" class="chat-composer">'
            '<textarea name="message" rows="4" placeholder="Опишите проблему, приложите шаги воспроизведения, расскажите что ожидали увидеть" required></textarea>'
            '<div class="composer-actions"><button type="submit">Отправить сообщение</button></div>'
            '</form>'
            '</section>'
            '</section>'
            '<aside class="sidebar-column">'
            '<section class="card side-panel sticky-panel"><span class="eyebrow">Как писать в поддержку</span><h2>Чтобы ответ пришел быстрее</h2>'
            '<div class="mini-list">'
            '<div class="mini-item"><strong>Что произошло</strong><span>Опишите ошибку простыми словами: что нажали и что получили в ответ.</span></div>'
            '<div class="mini-item"><strong>Когда это было</strong><span>Если есть, укажите дату, смену, ID материала или формулировку запроса.</span></div>'
            '<div class="mini-item"><strong>Что ожидали</strong><span>Это помогает быстрее понять, где именно поведение сайта отличается от ожидаемого.</span></div>'
            '</div></section>'
            '</aside>'
            '</section>'
        )
        return web.Response(
            text=self._render_authenticated_shell(
                title="Поддержка",
                context=context,
                session=session,
                active="support",
                lead="Отдельный чат для ошибок, вопросов и связи с администрацией сайта.",
                body_html=body_html,
            ),
            content_type="text/html",
            status=status,
        )

    def _settings_response(self, context: PublicPlatformContext, session: PublicSiteSession, *, status: int = 200) -> web.Response:
        account = self._site_account(context, session) or {}
        department = context.app_service.get_user_department(session.user_id)
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        stats_html = escape(context.app_service.build_user_settings_text(session.user_id))
        body_html = (
            '<section class="grid-layout">'
            '<section class="card side-panel">'
            '<span class="eyebrow">Аккаунт</span><h2>Профиль сайта</h2>'
            '<p class="muted">Эти настройки относятся только к веб-версии и не меняют аккаунты в ботах.</p>'
            '<form method="post" action="/settings/account/save" class="stack">'
            '<input type="hidden" name="return_to" value="settings">'
            f'<label>Логин сайта<input value="{escape(session.username)}" readonly></label>'
            f'<label>Отображаемое имя<input name="display_name" value="{escape(str(account.get("display_name") or session.display_name))}" placeholder="Как подписывать вас на сайте" required></label>'
            '<button type="submit">Сохранить профиль</button>'
            '</form>'
            '<form method="post" action="/settings/password/save" class="stack">'
            '<input type="hidden" name="return_to" value="settings">'
            '<label>Текущий пароль<input type="password" name="current_password" autocomplete="current-password" required></label>'
            '<label>Новый пароль<input type="password" name="new_password" autocomplete="new-password" required></label>'
            '<label>Повтор нового пароля<input type="password" name="new_password_repeat" autocomplete="new-password" required></label>'
            '<button type="submit">Обновить пароль</button>'
            '</form>'
            '</section>'
            '<section class="card side-panel">'
            '<span class="eyebrow">Рабочее пространство</span><h2>Персонализация</h2>'
            '<p class="muted">Выберите департамент, чтобы сайт подстраивал ответы и специальные режимы под ваш профиль.</p>'
            '<form method="post" action="/department/save" class="stack">'
            '<input type="hidden" name="return_to" value="settings">'
            f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
            '<button type="submit">Сохранить департамент</button>'
            '</form>'
            '<form method="post" action="/requests/create" class="stack">'
            '<input type="hidden" name="return_to" value="settings">'
            '<label>Имя заявки<input name="request_name" placeholder="Например: нужен дополнительный доступ" required></label>'
            '<label>Тип<select name="request_type"><option value="daily_limit">Обычный лимит</option><option value="department_mode">Спец-режим департамента</option></select></label>'
            '<label>Причина<textarea name="reason" rows="4" placeholder="Опишите, зачем вам нужен дополнительный доступ" required></textarea></label>'
            '<button type="submit">Отправить заявку</button>'
            '</form>'
            f'<pre class="pre compact-pre">{stats_html}</pre>'
            '</section>'
            '</section>'
        )
        return web.Response(
            text=self._render_authenticated_shell(
                title="Настройки сайта",
                context=context,
                session=session,
                active="settings",
                lead="Гибкая настройка профиля сайта, департамента и личного рабочего пространства.",
                body_html=body_html,
            ),
            content_type="text/html",
            status=status,
        )

    def _api_settings_response(self, context: PublicPlatformContext, session: PublicSiteSession, *, status: int = 200) -> web.Response:
        prefs = context.app_service.get_user_preferences(session.user_id)
        has_api = bool(context.app_service.get_active_api_key(prefs))
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        prompt_options_html = "".join(
            f'<option value="{escape(label)}"{" selected" if label == prompt_profile_label else ""}>{escape(label)}</option>'
            for label in context.app_service.prompt_profile_options()
        )
        custom_prompt = str(prefs.get("custom_prompt") or "")
        api_state_html = (
            f'<div class="mini-item"><strong>Статус API token</strong><span>{"Подключен и участвует в безлимите" if has_api else "Не подключен"}</span></div>'
            f'<div class="mini-item"><strong>Профиль prompt</strong><span>{escape(prompt_profile_label)}</span></div>'
            f'<div class="mini-item"><strong>Пользовательский prompt</strong><span>{"Сохранен" if custom_prompt.strip() else "Не задан"}</span></div>'
        )
        body_html = (
            '<section class="grid-layout">'
            '<section class="card side-panel">'
            '<span class="eyebrow">AI и API</span><h2>Личный API token</h2>'
            '<p class="muted">Здесь можно подключить собственный ключ и управлять AI-поведением отдельно от остальных пользователей.</p>'
            '<form method="post" action="/settings/api/save" class="stack">'
            '<input type="hidden" name="return_to" value="settings-api">'
            '<label>OpenAI API token<input type="password" name="api_key" placeholder="sk-..." autocomplete="off" required></label>'
            '<button type="submit">Сохранить API token</button>'
            '</form>'
            '<form method="post" action="/settings/api/delete" class="inline-form">'
            '<input type="hidden" name="return_to" value="settings-api">'
            '<button type="submit" class="ghost">Удалить API token</button>'
            '</form>'
            f'<div class="mini-list">{api_state_html}</div>'
            '</section>'
            '<section class="card side-panel">'
            '<span class="eyebrow">Кастомизация ответа</span><h2>Prompt и стиль</h2>'
            '<form method="post" action="/settings/profile/save" class="stack">'
            '<input type="hidden" name="return_to" value="settings-api">'
            f'<label>Профиль prompt<select name="prompt_profile">{prompt_options_html}</select></label>'
            '<button type="submit">Сохранить профиль</button>'
            '</form>'
            '<form method="post" action="/settings/prompt/save" class="stack">'
            '<input type="hidden" name="return_to" value="settings-api">'
            f'<label>Пользовательский prompt<textarea name="prompt_text" rows="8" placeholder="Опишите стиль ответа, ограничения, предпочтения по тону и структуре">{escape(custom_prompt)}</textarea></label>'
            '<button type="submit">Сохранить prompt</button>'
            '</form>'
            '<form method="post" action="/settings/prompt/delete" class="inline-form">'
            '<input type="hidden" name="return_to" value="settings-api">'
            '<button type="submit" class="ghost">Удалить prompt</button>'
            '</form>'
            '</section>'
            '</section>'
        )
        return web.Response(
            text=self._render_authenticated_shell(
                title="AI и API",
                context=context,
                session=session,
                active="settings-api",
                lead="Отдельная страница для API token, пользовательского prompt и глубокой кастомизации ответов.",
                body_html=body_html,
            ),
            content_type="text/html",
            status=status,
        )

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Чат</span><h2>Диалог в одном окне</h2><p>Задавайте вопросы, просматривайте историю ответов и работайте с памятью материалов в едином интерфейсе.</p><p class="muted">Интерфейс одинаково удобно выглядит на телефоне и на компьютере.</p></section>'
            '<section class="entry vk"><span class="pill">Настройки</span><h2>Гибкая кастомизация</h2><p>Отдельные страницы для профиля сайта, AI-настроек, API token, prompt и режимов работы.</p><p class="muted">Аккаунты сайта живут отдельно, а память материалов общая.</p></section>'
            '<section class="entry telegram"><span class="pill">Поддержка</span><h2>Связь с администрацией</h2><p>Ошибки и вопросы можно отправить прямо из сайта, а ответы команды проекта придут в отдельный диалог поддержки.</p><p class="muted">Поддержка и ответы сохраняются в общей базе сайта.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            "<body class=\"site home\"><main class=\"page\">"
            "<section class=\"hero\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Веб-платформа</span>"
            "<h1>Letovo Assistant</h1>"
            "<p class=\"lead\">Современный сайт для работы с материалами, сменами, поиском, персональными настройками и диалогами с AI.</p>"
            f"<p class=\"muted\">Сервис открыт по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Вход и регистрация на сайте полностью отдельные, а память материалов и поиска синхронизирована с ботами через общую базу.</p>"
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/login">Войти</a><a class="ghost-link" href="/register">Создать аккаунт</a></div>'
            "</div>"
            '<div class="hero-side summary">'
            '<div class="summary-tile"><span class="meta-label">Формат</span><strong>Один интерфейс</strong><span class="meta-note">Чат, настройки, смены и поддержка внутри сайта.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Данные</span><strong>Общая память</strong><span class="meta-note">Материалы и RAG-поиск общие с ботами.</span></div>'
            '<div class="summary-tile"><span class="meta-label">Аккаунты</span><strong>Отдельный вход</strong><span class="meta-note">Регистрация и логин сайта живут отдельно от аккаунтов в ботах.</span></div>'
            '<div class="summary-tile"><span class="meta-label">AI</span><strong>Гибкая настройка</strong><span class="meta-note">API token, prompt и профиль ответа вынесены в отдельный раздел.</span></div>'
            "</div>"
            "</section>"
            f'<section class="entry-grid">{feature_cards}</section>'
            "</main></body></html>"
        )

    def _render_login_page(self, *, error_text: str, username: str = "") -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        return (
            f"{self._head_html('Вход в Letovo Assistant')}"
            "<body class=\"site home\"><main class=\"page\">"
            "<section class=\"hero\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Вход</span>"
            "<h1>Сайт-аккаунт</h1>"
            "<p class=\"lead\">Войдите под отдельным логином сайта, чтобы открыть чат, настройки, историю запросов и поддержку.</p>"
            "<p class=\"muted\">Если аккаунт еще не создан, зарегистрируйтесь на отдельной странице. Для сайта используются свои логин и пароль.</p>"
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/">На главную</a><a class="ghost-link" href="/register">Регистрация</a></div>'
            "</div>"
            '<div class="hero-side"><form method="post" action="/login" class="card auth-form">'
            "<h2>Вход в сайт</h2>"
            f'<label>Логин сайта<input name="username" value="{escape(username)}" placeholder="Например user_web" autocomplete="username" required></label>'
            '<label>Пароль сайта<input type="password" name="password" autocomplete="current-password" required></label>'
            '<p class="hint">После входа откроется рабочее пространство с общими материалами и отдельными настройками сайта.</p>'
            '<button type="submit">Войти</button>'
            "</form></div>"
            "</section>"
            "</main></body></html>"
        )

    def _render_register_page(self, *, error_text: str, username: str = "", display_name: str = "") -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        return (
            f"{self._head_html('Регистрация в Letovo Assistant')}"
            "<body class=\"site home\"><main class=\"page\">"
            "<section class=\"hero\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Регистрация</span>"
            "<h1>Создать сайт-аккаунт</h1>"
            "<p class=\"lead\">После регистрации сайт сразу откроет личное рабочее пространство: чат, настройки, AI-профиль и поддержку.</p>"
            "<p class=\"muted\">Пароль хранится только в виде хеша, а логин и история сайта не связаны с аккаунтами в ботах.</p>"
            f"{error_html}"
            '<div class="toolbar"><a class="ghost-link" href="/">На главную</a><a class="ghost-link" href="/login">У меня уже есть аккаунт</a></div>'
            "</div>"
            '<div class="hero-side"><form method="post" action="/register" class="card auth-form" id="register-form-page">'
            "<h2>Регистрация</h2>"
            f'<label>Отображаемое имя<input name="display_name" value="{escape(display_name)}" placeholder="Как подписывать вас на сайте"></label>'
            f'<label>Логин сайта<input name="username" value="{escape(username)}" placeholder="Например user_web" autocomplete="username" required></label>'
            '<label>Пароль<input type="password" name="password" id="register-page-password" autocomplete="new-password" required></label>'
            '<label>Повтор пароля<input type="password" name="password_repeat" id="register-page-password-repeat" autocomplete="new-password" required></label>'
            '<div class="banner err" id="register-page-password-error" style="display:none"></div>'
            '<p class="hint">Минимум 8 символов. После регистрации можно будет поменять и имя, и пароль в настройках сайта.</p>'
            '<button type="submit">Создать аккаунт</button>'
            "</form></div>"
            "</section>"
            "<script>"
            "(() => {"
            "const form = document.getElementById('register-form-page');"
            "const password = document.getElementById('register-page-password');"
            "const repeat = document.getElementById('register-page-password-repeat');"
            "const errorBox = document.getElementById('register-page-password-error');"
            "if (!form || !password || !repeat || !errorBox) return;"
            "const syncState = () => { if (password.value === repeat.value) { errorBox.style.display = 'none'; errorBox.textContent = ''; } };"
            "password.addEventListener('input', syncState);"
            "repeat.addEventListener('input', syncState);"
            "form.addEventListener('submit', (event) => { if (password.value !== repeat.value) { event.preventDefault(); errorBox.textContent = 'Пароли не совпадают. Регистрация не выполнена.'; errorBox.style.display = 'block'; repeat.focus(); } });"
            "})();"
            "</script>"
            "</main></body></html>"
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        commands_html = self._render_command_buttons(context, custom_commands)
        shifts_html = self._render_shift_list(shifts)
        events_html = self._render_event_list(recent_events)
        action = context.app_service.department_action_for_user(session.user_id)
        action_html = self._render_department_action_card(context, session, action)
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)
        blocked_html = ""
        content_html = ""
        if banned:
            reason_text = ban_reason or "Причина не указана."
            blocked_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт сайта заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, воспользуйтесь страницей поддержки после повторного входа или свяжитесь с командой проекта.</p>'
                '</section>'
            )
        elif not department:
            department_options_html = "".join(
                f'<option value="{escape(option)}">{escape(option)}</option>'
                for option in context.app_service.department_options()
            )
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Старт</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">Это обязательный шаг: после выбора департамента сайт активирует правильные лимиты, профиль ответов и специальные режимы.</p>'
                '<form method="post" action="/department/save" class="grid one">'
                '<input type="hidden" name="return_to" value="dashboard">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить и открыть чат</button></div>'
                '</form>'
                '</section>'
            )
        else:
            chat_html = self._render_chat_panel(context, session)
            result_panel = self._render_result_panel(session)
            content_html = (
                '<section class="workspace">'
                '<section class="chat-column">'
                '<section class="card chat-shell">'
                '<div class="chat-shell-head"><div><span class="eyebrow">Чат</span><h2>Главный диалог</h2><p class="muted">Задавайте вопросы по материалам, ищите факты по датам и сменам, запускайте готовые сценарии и получайте текстовые ответы прямо в ленте.</p></div>'
                f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний</small></span></div></div>'
                '<form method="post" action="/ask" class="chat-composer">'
                '<textarea name="question" rows="4" placeholder="Например: что происходило в смене 01-07-2025..11-07-2025 и есть ли материалы по World News 24?" required></textarea>'
                '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
                '</form>'
                f'{managed_choice_html}'
                f'{chat_html}'
                '</section>'
                f'{result_panel}'
                '</section>'
                '<aside class="sidebar-column">'
                '<section class="card side-panel sticky-panel">'
                '<span class="eyebrow">Сводка</span><h2>Рабочая панель</h2>'
                f'<div class="mini-list"><div class="mini-item"><strong>Департамент</strong><span>{escape(department)}</span></div><div class="mini-item"><strong>Prompt профиль</strong><span>{escape(prompt_profile_label)}</span></div><div class="mini-item"><strong>API token</strong><span>{"подключен" if has_api else "не подключен"}</span></div><div class="mini-item"><strong>Сегодня</strong><span>Списано запросов: {int(stats_row.get("charged_today_count") or 0)}</span></div></div>'
                '</section>'
                '<section class="card side-panel">'
                '<span class="eyebrow">Быстрые действия</span><h2>Поиск и материалы</h2>'
                '<details class="tool-panel" open><summary>Быстрый поиск</summary><form method="post" action="/search" class="stack compact-form"><input name="query" placeholder="Ключевые слова, люди, компании" required><button type="submit">Искать</button></form></details>'
                '<details class="tool-panel"><summary>Дата или смена</summary><form method="post" action="/list" class="stack compact-form"><input name="query" placeholder="21-03-2026 или название смены" required><button type="submit">Показать список</button></form></details>'
                '<details class="tool-panel"><summary>Материал по ID</summary><form method="post" action="/file" class="stack compact-form"><input name="item_id" inputmode="numeric" placeholder="Например 123" required><button type="submit">Показать материал</button></form><p class="muted">Сайт показывает извлеченную информацию, а не сам файл.</p></details>'
                '<details class="tool-panel"><summary>Промокод</summary><form method="post" action="/promo" class="stack compact-form"><input name="code" placeholder="Введите промокод" required><button type="submit">Активировать</button></form></details>'
                '</section>'
                f'{action_html}'
                '<section class="card side-panel">'
                '<span class="eyebrow">Дополнительно</span><h2>Команды, смены и события</h2>'
                f'<details class="tool-panel" open><summary>Кастомные команды</summary>{commands_html}</details>'
                f'<details class="tool-panel"><summary>Смены</summary>{shifts_html}</details>'
                f'<details class="tool-panel"><summary>Последние события</summary>{events_html}</details>'
                '</section>'
                '</aside>'
                '</section>'
            )
        body_html = blocked_html or content_html
        return self._render_authenticated_shell(
            title="Рабочее пространство",
            context=context,
            session=session,
            active="dashboard",
            lead="Главная страница сайта: чат, поиск, быстрые действия и доступ к общей памяти материалов.",
            body_html=body_html,
        )

    async def _handle_support_send(self, request: web.Request) -> web.StreamResponse:
        context, session = self._require_session(request)
        fields = await self._read_simple_fields(request)
        message_text = fields.get("message", "").strip()
        if not session.username:
            self._set_error(session, "Сессия сайта устарела. Войдите заново, чтобы написать в поддержку.")
            raise web.HTTPFound("/")
        if not message_text:
            self._set_error(session, "Опишите проблему, вопрос или ошибку.")
            raise web.HTTPFound(f"{context.base_path}/support")
        context.app_service.create_site_support_message(
            username=session.username,
            site_user_id=session.user_id,
            display_name=session.display_name,
            sender_role="user",
            message_text=message_text,
        )
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="site_support_message",
            sender_profile=self._sender_profile(session),
            details={"surface": "web", "username": session.username, "message": message_text[:500]},
        )
        self._set_notice(session, "Сообщение отправлено администрации.")
        raise web.HTTPFound(f"{context.base_path}/support")

    async def _handle_ask(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        question = (await self._read_simple_fields(request)).get("question", "").strip()
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        if not question:
            self._set_error(session, "Напишите вопрос.")
            return self._dashboard_response(context, session, status=400)

        prefs = context.app_service.get_user_preferences(session.user_id)
        personal_api_key = context.app_service.get_active_api_key(prefs)
        custom_prompt = context.app_service.get_active_prompt(prefs)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        effective_prompt = context.app_service.build_effective_prompt(
            department=context.app_service.get_user_department(session.user_id),
            prompt_profile=prompt_profile,
            custom_prompt=custom_prompt,
        )
        is_admin = context.app_service.is_admin(session.user_id)
        allowed, remaining, unlimited_mode = context.app_service.consume_daily_limit(
            session.user_id,
            has_personal_api=bool(personal_api_key),
            is_admin=is_admin,
        )
        sender_profile = self._sender_profile(session)
        if not allowed:
            context.app_service.log_event(
                user_id=session.user_id,
                chat_id=session.user_id,
                event_type="limit_block",
                sender_profile=sender_profile,
                details={"request_kind": "ask", "question": question[:300], "surface": "web"},
            )
            self._set_error(session, context.app_service.build_limit_request_prompt(department_mode=False))
            return self._dashboard_response(context, session, status=403)

        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="ask",
            sender_profile=sender_profile,
            charged=not unlimited_mode,
            details={"question": question[:500], "surface": "web"},
        )
        managed_options = context.app_service.find_managed_answer_options(question)
        if managed_options:
            if len(managed_options) == 1:
                self._apply_managed_answer_option(
                    context,
                    session,
                    question=question,
                    option=managed_options[0],
                    remaining=remaining,
                    unlimited_mode=unlimited_mode,
                )
                return self._dashboard_response(context, session)
            session.chat_session.pending_managed_choice = ManagedAnswerChoice(
                question=question,
                options=managed_options,
                remaining=remaining,
                unlimited_mode=unlimited_mode,
            )
            context.app_service.log_event(
                user_id=session.user_id,
                chat_id=session.user_id,
                event_type="managed_answer_prompt",
                sender_profile=sender_profile,
                details={"count": len(managed_options), "trigger": question[:200], "surface": "web"},
            )
            self._set_result(
                session,
                "Выберите готовый вариант",
                context.app_service.append_remaining(
                    f"Нашел несколько готовых вариантов ответа. Выберите один из {len(managed_options)} вариантов ниже.",
                    remaining,
                    unlimited=unlimited_mode,
                ),
                user_text=question,
            )
            return self._dashboard_response(context, session)

        hits = context.app_service.retrieve_answer_hits(
            question,
            recent_messages=list(session.chat_session.recent_messages),
            api_key=personal_api_key,
        )
        if not hits:
            self._set_result(
                session,
                "Ответ",
                context.app_service.append_remaining("Подходящих материалов в памяти не найдено.", remaining, unlimited=unlimited_mode),
                user_text=question,
            )
            return self._dashboard_response(context, session)

        answer = context.app_service.answer_from_hits(
            question=question,
            hits=hits,
            recent_messages=list(session.chat_session.recent_messages),
            api_key=personal_api_key,
            custom_prompt=effective_prompt,
        )
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="text_answer",
            sender_profile=sender_profile,
            details={"auto": True, "forced_text_only": True, "surface": "web"},
        )
        self._set_result(
            session,
            "Ответ",
            context.app_service.append_remaining(answer, remaining, unlimited=unlimited_mode),
            user_text=question,
        )
        return self._dashboard_response(context, session)

    async def _handle_managed_answer(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        pending = session.chat_session.pending_managed_choice
        if pending is None:
            self._set_error(session, "Сейчас нет ожидающего выбора готового ответа.")
            return self._dashboard_response(context, session, status=400)
        option_raw = (await self._read_simple_fields(request)).get("option_id", "").strip()
        selected = self._resolve_managed_option(option_raw, pending.options)
        if selected is None:
            self._set_error(session, "Не удалось определить выбранный вариант ответа.")
            return self._dashboard_response(context, session, status=400)
        session.chat_session.pending_managed_choice = None
        self._apply_managed_answer_option(
            context,
            session,
            question=pending.question,
            option=selected,
            remaining=pending.remaining,
            unlimited_mode=pending.unlimited_mode,
        )
        return self._dashboard_response(context, session)

    async def _handle_search(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        query = (await self._read_simple_fields(request)).get("query", "").strip()
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        if not query:
            self._set_error(session, "Напишите запрос для поиска.")
            return self._dashboard_response(context, session, status=400)
        prefs = context.app_service.get_user_preferences(session.user_id)
        personal_api_key = context.app_service.get_active_api_key(prefs)
        is_admin = context.app_service.is_admin(session.user_id)
        allowed, remaining, unlimited_mode = context.app_service.consume_daily_limit(
            session.user_id,
            has_personal_api=bool(personal_api_key),
            is_admin=is_admin,
        )
        sender_profile = self._sender_profile(session)
        if not allowed:
            context.app_service.log_event(
                user_id=session.user_id,
                chat_id=session.user_id,
                event_type="limit_block",
                sender_profile=sender_profile,
                details={"request_kind": "search", "query": query[:300], "surface": "web"},
            )
            self._set_error(session, context.app_service.build_limit_request_prompt(department_mode=False))
            return self._dashboard_response(context, session, status=403)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="search",
            sender_profile=sender_profile,
            charged=not unlimited_mode,
            details={"query": query[:500], "surface": "web"},
        )
        hits = context.app_service.search(query, api_key=personal_api_key)
        if not hits:
            self._set_result(
                session,
                "Поиск",
                context.app_service.append_remaining("Ничего подходящего не найдено.", remaining, unlimited=unlimited_mode),
                user_text=f"Поиск: {query}",
            )
            return self._dashboard_response(context, session)
        lines = ["Результаты поиска:"]
        for hit in hits:
            date_text = context.app_service.display_content_with_shift(hit.content_date, getattr(hit, "content_scope", "dated"))
            lines.append(f"#{hit.item_id} | {date_text} | {hit.item_type} | {hit.file_name or '-'}\n{hit.summary}")
        self._set_result(
            session,
            "Поиск",
            context.app_service.append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode),
            user_text=f"Поиск: {query}",
        )
        return self._dashboard_response(context, session)

    async def _handle_list(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        raw_date = (await self._read_simple_fields(request)).get("query", "").strip()
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        if not raw_date:
            self._set_error(session, "Укажите дату в формате DD-MM-YYYY или название смены.")
            return self._dashboard_response(context, session, status=400)
        prefs = context.app_service.get_user_preferences(session.user_id)
        personal_api_key = context.app_service.get_active_api_key(prefs)
        is_admin = context.app_service.is_admin(session.user_id)
        allowed, remaining, unlimited_mode = context.app_service.consume_daily_limit(
            session.user_id,
            has_personal_api=bool(personal_api_key),
            is_admin=is_admin,
        )
        sender_profile = self._sender_profile(session)
        if not allowed:
            context.app_service.log_event(
                user_id=session.user_id,
                chat_id=session.user_id,
                event_type="limit_block",
                sender_profile=sender_profile,
                details={"request_kind": "list", "query": raw_date[:100], "surface": "web"},
            )
            self._set_error(session, context.app_service.build_limit_request_prompt(department_mode=False))
            return self._dashboard_response(context, session, status=403)
        try:
            content_date, items = context.app_service.list_by_date(raw_date)
        except Exception as exc:
            self._set_error(session, str(exc) or "Укажите дату в формате DD-MM-YYYY.")
            return self._dashboard_response(context, session, status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="list",
            sender_profile=sender_profile,
            charged=not unlimited_mode,
            details={"query": raw_date[:100], "surface": "web"},
        )
        if not items:
            self._set_result(
                session,
                "Список материалов",
                context.app_service.append_remaining("Для этой даты ничего не найдено.", remaining, unlimited=unlimited_mode),
                user_text=f"Список: {raw_date}",
            )
            return self._dashboard_response(context, session)
        lines = [f"Материалы за {content_date}:"]
        for item in items:
            shift_name = str(item.get("shift_name") or "").strip()
            shift_suffix = f" | смена: {shift_name}" if shift_name else ""
            lines.append(f"#{item['id']} | {item['item_type']} | {item['file_name'] or '-'}{shift_suffix}\n{item['summary']}")
        self._set_result(
            session,
            "Список материалов",
            context.app_service.append_remaining("\n\n".join(lines), remaining, unlimited=unlimited_mode),
            user_text=f"Список: {raw_date}",
        )
        return self._dashboard_response(context, session)

    async def _handle_file(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        raw_item_id = (await self._read_simple_fields(request)).get("item_id", "").strip()
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        if not raw_item_id.isdigit():
            self._set_error(session, "ITEM_ID должен быть числом.")
            return self._dashboard_response(context, session, status=400)
        prefs = context.app_service.get_user_preferences(session.user_id)
        personal_api_key = context.app_service.get_active_api_key(prefs)
        is_admin = context.app_service.is_admin(session.user_id)
        allowed, remaining, unlimited_mode = context.app_service.consume_daily_limit(
            session.user_id,
            has_personal_api=bool(personal_api_key),
            is_admin=is_admin,
        )
        sender_profile = self._sender_profile(session)
        if not allowed:
            context.app_service.log_event(
                user_id=session.user_id,
                chat_id=session.user_id,
                event_type="limit_block",
                sender_profile=sender_profile,
                details={"request_kind": "file", "query": raw_item_id, "surface": "web"},
            )
            self._set_error(session, context.app_service.build_limit_request_prompt(department_mode=False))
            return self._dashboard_response(context, session, status=403)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="file",
            sender_profile=sender_profile,
            charged=not unlimited_mode,
            details={"query": raw_item_id, "surface": "web"},
        )
        item = context.app_service.get_item(int(raw_item_id))
        if not item:
            self._set_result(
                session,
                "Материал",
                context.app_service.append_remaining("Элемент не найден.", remaining, unlimited=unlimited_mode),
                user_text=f"Материал #{raw_item_id}",
            )
            return self._dashboard_response(context, session)
        description = context.app_service.describe_item_for_text_only(item)
        self._set_result(
            session,
            "Материал",
            context.app_service.append_remaining(description, remaining, unlimited=unlimited_mode),
            user_text=f"Материал #{raw_item_id}",
        )
        return self._dashboard_response(context, session)

    async def _handle_promo(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        code = (await self._read_simple_fields(request)).get("code", "").strip()
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        if not code:
            self._set_error(session, "Введите промокод.")
            return self._dashboard_response(context, session, status=400)
        ok, message = context.app_service.redeem_promo_code(session.user_id, code)
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="promo_redeem",
            sender_profile=self._sender_profile(session),
            details={"code": code[:80], "ok": ok, "bonus_requests": bonus_requests, "surface": "web"},
        )
        suffix = f"\n\nДоступно бонусных запросов: {bonus_requests}." if ok else ""
        self._set_result(session, "Промокод", f"{message}{suffix}", user_text=f"Промокод: {code}")
        return self._dashboard_response(context, session, status=200 if ok else 400)

    async def _handle_custom_command(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        command_name = (await self._read_simple_fields(request)).get("command_name", "").strip()
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        if not command_name:
            self._set_error(session, "Команда не указана.")
            return self._dashboard_response(context, session, status=400)
        custom_command = context.app_service.get_custom_command(command_name)
        if not custom_command:
            self._set_error(session, "Команда не найдена или отключена.")
            return self._dashboard_response(context, session, status=404)
        sender_profile = self._sender_profile(session)
        normalized_command = str(custom_command.get("command_name") or command_name).strip() or "/command"
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="custom_command",
            sender_profile=sender_profile,
            details={"command": normalized_command, "surface": "web"},
        )
        if bool(custom_command.get("notify_admin", 1)):
            await self._notify_custom_command_admins(context, session, normalized_command)
        response_text = str(custom_command.get("response_text") or "").strip()
        if response_text:
            self._set_result(session, f"Команда {normalized_command}", response_text, user_text=normalized_command)
            return self._dashboard_response(context, session)
        if custom_command.get("media_path"):
            self._set_result(
                session,
                f"Команда {normalized_command}",
                f"Для команды {normalized_command} настроен файл, но сайт отвечает только текстом.",
                user_text=normalized_command,
            )
            return self._dashboard_response(context, session)
        self._set_result(session, f"Команда {normalized_command}", f"Команда {normalized_command} выполнена.", user_text=normalized_command)
        return self._dashboard_response(context, session)

    async def _handle_department_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        department_raw = (await self._read_simple_fields(request)).get("department", "").strip()
        department = context.app_service.normalize_department(department_raw)
        if department is None:
            self._set_error(session, "Выберите департамент из списка.")
            return self._dashboard_response(context, session, status=400)
        context.app_service.save_user_department(session.user_id, department)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="department_selected",
            sender_profile=self._sender_profile(session),
            details={"department": department, "surface": "web"},
        )
        self._set_notice(session, f"Департамент сохранен: {department}.")
        return self._dashboard_response(context, session)

    async def _handle_department_action(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        fields = await self._read_simple_fields(request)
        question = fields.get("question", "").strip()
        if not question:
            self._set_error(session, "Напишите вопрос для спец-режима.")
            return self._dashboard_response(context, session, status=400)
        user_department = context.app_service.get_user_department(session.user_id)
        if user_department == "проект 11":
            chosen_label = fields.get("action_label", "").strip()
            action = context.app_service.resolve_department_action_by_label(chosen_label, user_department)
            if action is None:
                self._set_error(session, "Для проекта 11 выберите режим анализа.")
                return self._dashboard_response(context, session, status=400)
            action_department = str(action["department"])
        else:
            action_department = user_department or ""
        prefs = context.app_service.get_user_preferences(session.user_id)
        personal_api_key = context.app_service.get_active_api_key(prefs)
        custom_prompt = context.app_service.get_active_prompt(prefs)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        allowed, remaining, used_bonus, mode_bucket = context.app_service.consume_department_action_limit(session.user_id, action_department)
        sender_profile = self._sender_profile(session)
        if not allowed:
            context.app_service.log_event(
                user_id=session.user_id,
                chat_id=session.user_id,
                event_type="department_limit_block",
                sender_profile=sender_profile,
                details={"department": action_department, "question": question[:300], "mode_bucket": mode_bucket, "surface": "web"},
            )
            self._set_error(session, context.app_service.build_limit_request_prompt(department_mode=True))
            return self._dashboard_response(context, session, status=403)
        answer, hits, date_from = context.app_service.run_department_action(
            user_id=session.user_id,
            action_department=action_department,
            question=question,
            recent_messages=list(session.chat_session.recent_messages),
            api_key=personal_api_key,
            custom_prompt=custom_prompt,
            prompt_profile=prompt_profile,
        )
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="department_action",
            sender_profile=sender_profile,
            charged=used_bonus,
            details={"department": action_department, "question": question[:500], "hits": len(hits), "date_from": date_from, "surface": "web"},
        )
        self._set_result(
            session,
            "Спец-режим департамента",
            context.app_service.append_remaining(answer, remaining, unlimited=False),
            user_text=question,
        )
        return self._dashboard_response(context, session)

    async def _handle_api_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        api_key = (await self._read_simple_fields(request)).get("api_key", "").strip()
        if not api_key:
            self._set_error(session, "Введите API token.")
            return self._dashboard_response(context, session, status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_set_api", "surface": "web"},
        )
        ok, error_text = context.app_service.validate_user_api_key(api_key)
        if not ok:
            context.app_service.save_user_api_error(session.user_id, error_text or "unknown error")
            self._set_error(session, f"API token не прошел проверку.\n\nТекст ошибки: {(error_text or 'unknown error')[:400]}")
            return self._dashboard_response(context, session, status=400)
        prefs = context.app_service.get_user_preferences(session.user_id)
        has_saved_prompt = bool((prefs.get("custom_prompt") or "").strip())
        context.app_service.save_user_api_key(session.user_id, api_key)
        text = "Ваш API token сохранен и проверен. Для вас включен безлимит."
        if has_saved_prompt:
            text += " Ранее сохраненный prompt снова активирован."
        self._set_notice(session, text)
        return self._dashboard_response(context, session)

    async def _handle_api_delete(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_delete_api", "surface": "web"},
        )
        prefs = context.app_service.get_user_preferences(session.user_id)
        had_prompt = bool((prefs.get("custom_prompt") or "").strip())
        context.app_service.clear_user_api_key(session.user_id)
        text = "Ваш API token удален. Безлимит отключен."
        if had_prompt:
            text = "Ваш API token удален. Безлимит отключен, пользовательский prompt сохранен, но не будет применяться, пока вы снова не добавите API token."
        self._set_notice(session, text)
        return self._dashboard_response(context, session)

    async def _handle_prompt_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        prompt_text = (await self._read_simple_fields(request)).get("prompt_text", "").strip()
        if not prompt_text:
            self._set_error(session, "Введите пользовательский prompt.")
            return self._dashboard_response(context, session, status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_set_prompt", "surface": "web"},
        )
        prefs = context.app_service.get_user_preferences(session.user_id)
        if not context.app_service.get_active_api_key(prefs):
            self._set_error(session, "Сначала добавьте рабочий API token.")
            return self._dashboard_response(context, session, status=400)
        context.app_service.save_user_prompt(session.user_id, prompt_text)
        self._set_notice(session, "Ваш пользовательский prompt сохранен.")
        return self._dashboard_response(context, session)

    async def _handle_prompt_delete(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_delete_prompt", "surface": "web"},
        )
        context.app_service.clear_user_prompt(session.user_id)
        self._set_notice(session, "Ваш пользовательский prompt удален.")
        return self._dashboard_response(context, session)

    async def _handle_prompt_profile_save(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        profile_label = (await self._read_simple_fields(request)).get("prompt_profile", "").strip()
        normalized = context.app_service.normalize_prompt_profile(profile_label)
        if normalized is None:
            self._set_error(session, "Выберите один из доступных профилей prompt.")
            return self._dashboard_response(context, session, status=400)
        context.app_service.save_user_prompt_profile(session.user_id, normalized)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="settings",
            sender_profile=self._sender_profile(session),
            details={"command": "web_prompt_profile", "profile": normalized, "surface": "web"},
        )
        self._set_notice(session, f"Профиль prompt переключен на: {context.app_service.PROMPT_PROFILE_LABELS.get(normalized, normalized)}.")
        return self._dashboard_response(context, session)

    async def _handle_access_request(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        if not self._ensure_department_selected(context, session):
            return self._dashboard_response(context, session, status=400)
        fields = await self._read_simple_fields(request)
        request_name = fields.get("request_name", "").strip()
        reason = fields.get("reason", "").strip()
        request_type = fields.get("request_type", "").strip() or "daily_limit"
        if not request_name:
            self._set_error(session, "Укажите имя заявки.")
            return self._dashboard_response(context, session, status=400)
        if not reason:
            self._set_error(session, "Опишите причину заявки.")
            return self._dashboard_response(context, session, status=400)
        mode_bucket = None
        if request_type == "department_mode":
            mode_bucket = context.app_service.department_mode_bucket_for_user(
                session.user_id,
                context.app_service.get_user_department(session.user_id) or "",
            )
        request_id = context.app_service.create_access_request(
            user_id=session.user_id,
            request_name=request_name,
            reason=reason,
            request_type=request_type,
            mode_bucket=mode_bucket,
        )
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="access_request",
            sender_profile=self._sender_profile(session),
            details={
                "request_id": request_id,
                "reason": reason[:500],
                "request_type": request_type,
                "mode_bucket": mode_bucket or "",
                "surface": "web",
            },
        )
        self._set_notice(session, f"Запрос #{request_id} отправлен.")
        return self._dashboard_response(context, session)

    def _dashboard_response(self, context: PublicPlatformContext, session: PublicSiteSession, *, status: int = 200) -> web.Response:
        return web.Response(text=self._render_dashboard(context, session), content_type="text/html", status=status)

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        password_required = bool(self._site_password())
        platform_options = "".join(
            f'<option value="{escape(context.slug)}">{escape(context.title)}</option>'
            for context in self._ordered_platforms()
        )
        platform_cards = "".join(
            (
                f'<section class="entry {escape(context.accent)}">'
                f'<span class="pill">{escape(context.title)}</span>'
                f'<h2>{escape(context.title)}</h2>'
                f'<p>{escape(context.subtitle)}</p>'
                '<p class="muted">Материалы и память поиска доступны здесь в отдельном веб-интерфейсе с собственным аккаунтом.</p>'
                "</section>"
            )
            for context in self._ordered_platforms()
        )
        password_html = (
            '<label>Пароль сайта<input type="password" name="password" placeholder="Если настроен" required></label>'
            if password_required
            else '<div class="inline-note">Пароль для пользовательского сайта не настроен. Вход локальный и работает без дополнительного секрета.</div>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            "<body class=\"site home\"><main class=\"page\">"
            "<section class=\"hero\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Публичный интерфейс</span>"
            "<h1>Letovo Assistant</h1>"
            "<p class=\"lead\">Отдельный веб-интерфейс для вопросов, поиска по материалам, сменам, промокодам и персональным настройкам.</p>"
            f"<p class=\"muted\">Сайт работает отдельно по адресу <code>{escape(self.settings.public_web_base_url)}</code>. Общими с ботами остаются только материалы и память поиска, а аккаунт сайта живет отдельно.</p>"
            f"{error_html}"
            "</div>"
            "<div class=\"hero-side\">"
            "<form method=\"post\" action=\"/login\" class=\"card auth-form\">"
            "<h2>Вход в сайт</h2>"
            "<label>Платформа<select name=\"platform\" required>"
            f"{platform_options}"
            "</select></label>"
            "<label>User ID<input name=\"user_id\" inputmode=\"numeric\" placeholder=\"Например 8258050467\" required></label>"
            "<p class=\"hint\">Используйте отдельный сайт-аккаунт. Веб-версия работает независимо от аккаунтов в ботах.</p>"
            "<label>Имя на сайте<input name=\"display_name\" placeholder=\"Как подписывать ваши действия\"></label>"
            f"{password_html}"
            "<button type=\"submit\">Открыть сайт</button>"
            "</form>"
            "</div>"
            "</section>"
            f"<section class=\"entry-grid\">{platform_cards}</section>"
            "</main></body></html>"
        )

    def _render_dashboard(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        banned, ban_reason = context.app_service.is_banned(session.user_id)
        prefs = context.app_service.get_user_preferences(session.user_id)
        department = context.app_service.get_user_department(session.user_id)
        prompt_profile = context.app_service.get_prompt_profile(prefs)
        prompt_profile_label = context.app_service.PROMPT_PROFILE_LABELS.get(prompt_profile, "Департаментный")
        has_api = bool(context.app_service.get_active_api_key(prefs))
        bonus_requests = context.app_service.get_user_bonus_requests(session.user_id)
        shifts = context.app_service.list_shifts(limit=60)
        custom_commands = [row for row in context.app_service.list_custom_commands() if int(row.get("enabled") or 0)]
        recent_events = context.app_service.list_user_events(user_id=session.user_id, limit=16)
        user_stats = context.app_service.get_user_statistics(str(session.user_id))
        stats_row = user_stats[0] if user_stats else {}
        notice_html = f'<div class="banner ok">{escape(session.notice_text)}</div>' if session.notice_text else ""
        error_html = f'<div class="banner err">{escape(session.error_text)}</div>' if session.error_text else ""
        commands_html = self._render_command_buttons(context, custom_commands)
        shifts_html = self._render_shift_list(shifts)
        events_html = self._render_event_list(recent_events)
        stats_html = escape(context.app_service.build_user_settings_text(session.user_id))
        prompt_options_html = "".join(
            f'<option value="{escape(label)}"{" selected" if label == prompt_profile_label else ""}>{escape(label)}</option>'
            for label in context.app_service.prompt_profile_options()
        )
        department_options_html = "".join(
            f'<option value="{escape(option)}"{" selected" if option == department else ""}>{escape(option)}</option>'
            for option in context.app_service.department_options()
        )
        action = context.app_service.department_action_for_user(session.user_id)
        action_html = self._render_department_action_card(context, session, action)
        managed_choice_html = self._render_managed_choice_card(context, session.chat_session.pending_managed_choice)
        account_note = f"Логин: {session.username}" if session.username else f"ID: {session.user_id}"
        blocked_html = ""
        content_html = ""
        if banned:
            reason_text = ban_reason or "Причина не указана."
            blocked_html = (
                '<section class="card blocked">'
                '<span class="eyebrow">Доступ ограничен</span>'
                '<h2>Ваш аккаунт заблокирован</h2>'
                f'<p class="lead">{escape(reason_text)}</p>'
                '<p class="muted">Если это ошибка, свяжитесь с командой поддержки.</p>'
                '</section>'
            )
        elif not department:
            content_html = (
                '<section class="card spotlight">'
                '<span class="eyebrow">Обязательный шаг</span>'
                '<h2>Выберите департамент</h2>'
                '<p class="lead">Пока департамент не выбран, рабочие функции недоступны. После выбора сайт сразу подстроит режим работы под ваш профиль.</p>'
                f'<form method="post" action="{context.base_path}/department/save" class="grid one">'
                f'<label>Департамент<select name="department" required>{department_options_html}</select></label>'
                '<div class="action-wrap"><button type="submit">Сохранить департамент</button></div>'
                '</form>'
                '</section>'
            )
        else:
            content_html = self._render_work_area(
                context=context,
                session=session,
                stats_row=stats_row,
                prompt_profile_label=prompt_profile_label,
                has_api=has_api,
                bonus_requests=bonus_requests,
                prompt_options_html=prompt_options_html,
                commands_html=commands_html,
                shifts_html=shifts_html,
                action_html=action_html,
                managed_choice_html=managed_choice_html,
                events_html=events_html,
                stats_html=stats_html,
            )
        return (
            f"{self._head_html(context.title)}"
            "<body class=\"site\"><main class=\"page\">"
            '<section class="hero dashboard-hero">'
            '<div class="hero-copy">'
            f'<span class="eyebrow">{escape(context.title)}</span>'
            "<h1>Letovo Assistant</h1>"
            f'<p class="lead">{escape(context.subtitle)}</p>'
            '<p class="muted">Здесь собраны материалы, смены, лимиты, настройки и история запросов в одном интерфейсе.</p>'
            "</div>"
            '<div class="hero-side summary">'
            f'<div class="summary-tile"><span class="meta-label">Пользователь</span><strong>{escape(session.display_name)}</strong><span class="meta-note">{escape(account_note)}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Департамент</span><strong>{escape(department or "не выбран")}</strong><span class="meta-note">Активный профиль</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Prompt</span><strong>{escape(prompt_profile_label)}</strong><span class="meta-note">API: {"подключен" if has_api else "не подключен"}</span></div>'
            f'<div class="summary-tile"><span class="meta-label">Бонусы</span><strong>{bonus_requests}</strong><span class="meta-note">Сегодня: {int(stats_row.get("charged_today_count") or 0)} списаний</span></div>'
            "</div>"
            "</section>"
            '<div class="toolbar">'
            '<a class="ghost-link" href="/">Сменить аккаунт</a>'
            '<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>'
            "</div>"
            f"{notice_html}{error_html}"
            f"{blocked_html or content_html}"
            "</main></body></html>"
        )

    def _render_work_area(
        self,
        *,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        stats_row: dict[str, Any],
        prompt_profile_label: str,
        has_api: bool,
        bonus_requests: int,
        prompt_options_html: str,
        commands_html: str,
        shifts_html: str,
        action_html: str,
        managed_choice_html: str,
        events_html: str,
        stats_html: str,
    ) -> str:
        chat_html = self._render_chat_panel(context, session)
        return (
            '<section class="workspace">'
            '<aside class="sidebar-column">'
            '<section class="card side-panel sticky-panel">'
            '<span class="eyebrow">Быстрые действия</span>'
            '<h2>Инструменты</h2>'
            '<p class="muted">На компьютере это боковая панель, а на телефоне блоки автоматически складываются в удобный вертикальный формат.</p>'
            '<details class="tool-panel" open><summary>Быстрый поиск</summary>'
            f'<form method="post" action="{context.base_path}/search" class="stack compact-form">'
            '<input name="query" placeholder="Ключевые слова, люди, компании" required>'
            '<button type="submit">Искать</button>'
            '</form></details>'
            '<details class="tool-panel"><summary>Дата или смена</summary>'
            f'<form method="post" action="{context.base_path}/list" class="stack compact-form">'
            '<input name="query" placeholder="21-03-2026 или название смены" required>'
            '<button type="submit">Показать список</button>'
            '</form></details>'
            '<details class="tool-panel"><summary>Материал по ID</summary>'
            f'<form method="post" action="{context.base_path}/file" class="stack compact-form">'
            '<input name="item_id" inputmode="numeric" placeholder="Например 123" required>'
            '<button type="submit">Показать материал</button>'
            '</form>'
            '<p class="muted">Ответ всегда приходит текстом: показывается извлеченная информация, а не сам файл.</p>'
            '</details>'
            '<details class="tool-panel"><summary>Промокод</summary>'
            f'<form method="post" action="{context.base_path}/promo" class="stack compact-form">'
            '<input name="code" placeholder="Введите промокод" required>'
            '<button type="submit">Активировать</button>'
            '</form>'
            f'<div class="mini-item"><strong>Сводка</strong><span>Профиль: {escape(prompt_profile_label)}</span><span>API: {"подключен" if has_api else "не подключен"}</span><span>Бонусные запросы: {bonus_requests}</span><span>Сегодня списано: {int(stats_row.get("charged_today_count") or 0)}</span></div>'
            '</details>'
            '</section>'
            f'{action_html}'
            '<section class="card side-panel">'
            '<span class="eyebrow">Настройки</span>'
            '<h2>API, prompt и профиль</h2>'
            '<div class="settings-grid settings-grid-web">'
            f'<form method="post" action="{context.base_path}/settings/api/save" class="stack">'
            '<label>Личный OpenAI API token<input type="password" name="api_key" placeholder="sk-..." required></label>'
            '<button type="submit">Сохранить API token</button>'
            '</form>'
            f'<form method="post" action="{context.base_path}/settings/api/delete" class="inline-form"><button type="submit" class="ghost">Удалить API token</button></form>'
            f'<form method="post" action="{context.base_path}/settings/prompt/save" class="stack">'
            '<label>Пользовательский prompt<textarea name="prompt_text" rows="4" placeholder="Собственные правила и стиль ответа"></textarea></label>'
            '<button type="submit">Сохранить prompt</button>'
            '</form>'
            f'<form method="post" action="{context.base_path}/settings/prompt/delete" class="inline-form"><button type="submit" class="ghost">Удалить prompt</button></form>'
            f'<form method="post" action="{context.base_path}/settings/profile/save" class="stack">'
            f'<label>Профиль prompt<select name="prompt_profile">{prompt_options_html}</select></label>'
            '<button type="submit">Переключить профиль</button>'
            '</form>'
            '</div>'
            f'<pre class="pre compact-pre">{stats_html}</pre>'
            '</section>'
            '<section class="card side-panel">'
            '<span class="eyebrow">Дополнительно</span>'
            '<h2>Команды, запросы и смены</h2>'
            '<details class="tool-panel" open><summary>Кастомные команды</summary>'
            f'{commands_html}'
            '</details>'
            '<details class="tool-panel"><summary>Запрос на доступ</summary>'
            f'<form method="post" action="{context.base_path}/requests/create" class="stack compact-form">'
            '<label>Имя заявки<input name="request_name" placeholder="Например: нужен дополнительный доступ" required></label>'
            '<label>Тип<select name="request_type"><option value="daily_limit">Обычный лимит</option><option value="department_mode">Спец-режим департамента</option></select></label>'
            '<label>Причина<textarea name="reason" rows="4" placeholder="Опишите, зачем нужен доступ или дополнительные запросы" required></textarea></label>'
            '<button type="submit">Отправить запрос</button>'
            '</form>'
            '</details>'
            '<details class="tool-panel"><summary>Смены</summary>'
            f'{shifts_html}'
            '</details>'
            '<details class="tool-panel"><summary>Последние события</summary>'
            f'{events_html}'
            '</details>'
            '</section>'
            '</aside>'
            '<section class="chat-column">'
            '<section class="card chat-shell">'
            '<div class="chat-shell-head">'
            '<div>'
            '<span class="eyebrow">Чат</span>'
            '<h2>Диалог</h2>'
            '<p class="muted">Задавайте вопрос в свободной форме. Ниже останется лента последних сообщений в рамках текущей веб-сессии.</p>'
            '</div>'
            f'<div class="chat-metrics"><span class="metric"><strong>{bonus_requests}</strong><small>бонусов</small></span><span class="metric"><strong>{int(stats_row.get("charged_today_count") or 0)}</strong><small>списаний сегодня</small></span></div>'
            '</div>'
            f'<form method="post" action="{context.base_path}/ask" class="chat-composer">'
            '<textarea name="question" rows="4" placeholder="Например: что было по World News 24 в смене 01-07-2025..11-07-2025?" required></textarea>'
            '<div class="composer-actions"><button type="submit">Отправить вопрос</button></div>'
            '</form>'
            f'{managed_choice_html}'
            f'{chat_html}'
            '</section>'
            '</section>'
            '</section>'
        )

    def _render_chat_panel(self, context: PublicPlatformContext, session: PublicSiteSession) -> str:
        rows: list[str] = []
        if not session.chat_session.recent_messages and not session.result_text:
            rows.append(
                '<article class="chat-row assistant latest">'
                '<div class="chat-bubble">'
                '<div class="chat-meta"><span>Сайт</span><span>старт</span></div>'
                '<div class="chat-title">Чат готов</div>'
                '<p>Задайте вопрос, выполните поиск, откройте материал по ID, активируйте промокод или запустите спец-режим. Ответы будут появляться в этой ленте.</p>'
                '</div>'
                '</article>'
            )
        else:
            for item in session.chat_session.recent_messages:
                role = str(item.get("role") or "assistant")
                is_user = role == "user"
                label = "Вы" if is_user else context.title
                content = escape(str(item.get("content") or ""))
                rows.append(
                    f'<article class="chat-row {"user" if is_user else "assistant"}">'
                    '<div class="chat-bubble">'
                    f'<div class="chat-meta"><span>{escape(label)}</span><span>{"вопрос" if is_user else "ответ"}</span></div>'
                    f'<p>{content}</p>'
                    '</div>'
                    '</article>'
                )
            if session.result_text:
                rows.append(
                    '<article class="chat-row assistant latest">'
                    '<div class="chat-bubble">'
                    f'<div class="chat-meta"><span>{escape(context.title)}</span><span>последний результат</span></div>'
                    f'<div class="chat-title">{escape(session.result_title or "Результат")}</div>'
                    f'<p>{escape(session.result_text)}</p>'
                    '</div>'
                    '</article>'
                )
        return f'<div class="chat-log">{"".join(rows)}</div>'

    def _render_department_action_card(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        action: dict[str, str] | None,
    ) -> str:
        user_department = context.app_service.get_user_department(session.user_id)
        if not action and user_department != "проект 11":
            return ""
        if user_department == "проект 11":
            options_html = "".join(
                f'<option value="{escape(label)}">{escape(label)}</option>'
                for label in context.app_service.all_department_action_labels()
            )
            return (
                '<section class="card primary">'
                '<span class="eyebrow">Проект 11</span>'
                '<h2>Любой спец-режим на сегодня</h2>'
                f'<p class="muted">{escape(context.app_service.department_action_picker_prompt())}</p>'
                f'<form method="post" action="{context.base_path}/department/action" class="stack">'
                f'<label>Режим<select name="action_label" required>{options_html}</select></label>'
                '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
                '<button type="submit">Запустить анализ</button>'
                '</form>'
                '</section>'
            )
        if action is None:
            return ""
        return (
            '<section class="card primary">'
            f'<span class="eyebrow">{escape(str(action.get("department") or ""))}</span>'
            f'<h2>{escape(str(action.get("title") or "Спец-режим"))}</h2>'
            '<p class="muted">Базовый лимит этого режима обновляется каждый день. Если для вас доступны дополнительные спец-запросы, они будут учтены автоматически.</p>'
            f'<form method="post" action="{context.base_path}/department/action" class="stack">'
            '<label>Вопрос<textarea name="question" rows="4" placeholder="Сформулируйте запрос для спец-режима" required></textarea></label>'
            f'<button type="submit">{escape(str(action.get("button") or "Запустить"))}</button>'
            '</form>'
            '</section>'
        )

    def _render_managed_choice_card(
        self,
        context: PublicPlatformContext,
        pending: ManagedAnswerChoice | None,
    ) -> str:
        if pending is None:
            return ""
        buttons_html = "".join(
            f'<form method="post" action="{context.base_path}/managed-answer" class="choice-form"><input type="hidden" name="option_id" value="{option.option_id}"><button type="submit">{escape(option.option_label)}</button></form>'
            for option in pending.options
        )
        return (
            '<section class="card spotlight managed-choice">'
            '<span class="eyebrow">Готовые ответы</span>'
            '<h2>Выберите вариант</h2>'
            f'<p class="muted">Вопрос: {escape(pending.question)}</p>'
            f'<div class="choice-grid">{buttons_html}</div>'
            '</section>'
        )

    def _render_result_panel(self, session: PublicSiteSession) -> str:
        if not session.result_text:
            return (
                '<section class="card result-card">'
                '<span class="eyebrow">Результат</span>'
                '<h2>Пока пусто</h2>'
                '<p class="muted">Когда вы зададите вопрос, выполните поиск, активируете промокод или смените настройки, результат появится здесь.</p>'
                '</section>'
            )
        title = escape(session.result_title or "Результат")
        text = escape(session.result_text)
        return (
            '<section class="card result-card">'
            '<span class="eyebrow">Результат</span>'
            f'<h2>{title}</h2>'
            f'<pre class="pre result-pre">{text}</pre>'
            '</section>'
        )

    def _render_history_panel(self, session: PublicSiteSession) -> str:
        if not session.chat_session.recent_messages:
            return (
                '<section class="card">'
                '<span class="eyebrow">Контекст</span>'
                '<h2>История диалога</h2>'
                '<p class="muted">Здесь появятся последние вопросы и ответы сайта. Они используются как контекст для следующих запросов в этой веб-сессии.</p>'
                '</section>'
            )
        rows = []
        for item in session.chat_session.recent_messages:
            role = "Вы" if item.get("role") == "user" else "Сайт"
            rows.append(f'<div class="history-item"><strong>{escape(role)}</strong><p>{escape(str(item.get("content") or ""))}</p></div>')
        return (
            '<section class="card">'
            '<span class="eyebrow">Контекст</span>'
            '<h2>История диалога</h2>'
            f'<div class="history-list">{"".join(rows)}</div>'
            '</section>'
        )

    def _render_command_buttons(self, context: PublicPlatformContext, commands: list[dict[str, Any]]) -> str:
        if not commands:
            return '<p class="muted">Дополнительные команды пока не настроены.</p>'
        buttons = []
        for row in commands[:18]:
            command_name = str(row.get("command_name") or "").strip()
            if not command_name:
                continue
            buttons.append(
                f'<form method="post" action="{context.base_path}/command/run" class="command-form">'
                f'<input type="hidden" name="command_name" value="{escape(command_name)}">'
                f'<button type="submit" class="ghost">{escape(command_name)}</button>'
                '</form>'
            )
        return f'<div class="command-grid">{"".join(buttons)}</div>'

    def _render_shift_list(self, shifts: list[dict[str, Any]]) -> str:
        if not shifts:
            return '<p class="muted">Смены пока не настроены.</p>'
        items = []
        for row in shifts[:12]:
            label = str(row.get("name") or "").strip() or format_display_date_range(str(row.get("date_from") or ""), str(row.get("date_to") or ""))
            range_text = format_display_date_range(str(row.get("date_from") or ""), str(row.get("date_to") or ""))
            items.append(f'<div class="mini-item"><strong>{escape(label)}</strong><span>{escape(range_text)}</span></div>')
        return f'<div class="mini-list">{"".join(items)}</div>'

    def _render_event_list(self, recent_events: list[dict[str, Any]]) -> str:
        if not recent_events:
            return '<p class="muted">Событий пока нет.</p>'
        rows = []
        for row in recent_events[:16]:
            details = row.get("details") or {}
            detail_text = ", ".join(
                f"{key}={value}"
                for key, value in details.items()
                if value not in (None, "", [], {})
            )
            rows.append(
                '<div class="mini-item">'
                f'<strong>{escape(str(row.get("event_type") or "-"))}</strong>'
                f'<span>{escape(str(row.get("created_at") or "-"))}</span>'
                f'<p>{escape(detail_text[:220] or "Без деталей")}</p>'
                '</div>'
            )
        return f'<div class="mini-list">{"".join(rows)}</div>'

    @staticmethod
    def _head_html(title: str) -> str:
        return (
            "<!doctype html><html lang=\"ru\"><head><meta charset=\"utf-8\">"
            "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            f"<title>{escape(title)}</title><style>{PublicWebServer._styles()}</style></head>"
        )

    @staticmethod
    def _styles() -> str:
        return (
            "@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;700&display=swap');"
            ":root{--bg:#f7efe2;--bg-2:#f3e5cf;--surface:rgba(255,255,255,.82);--line:rgba(33,39,53,.12);--text:#182033;--muted:#6b7485;--accent:#1f6fe5;--accent-2:#0f766e;--accent-soft:rgba(31,111,229,.11);--danger:#c15335;--shadow:0 28px 70px rgba(20,25,40,.14);--shadow-soft:0 14px 28px rgba(20,25,40,.08);--radius:28px}"
            "*{box-sizing:border-box}html{scroll-behavior:smooth}body{margin:0;min-height:100vh;background:radial-gradient(circle at top left,rgba(31,111,229,.16),transparent 28%),radial-gradient(circle at top right,rgba(15,118,110,.14),transparent 26%),linear-gradient(180deg,var(--bg) 0%,var(--bg-2) 100%);color:var(--text);font:16px/1.55 'IBM Plex Sans','Segoe UI Variable Text','Trebuchet MS',sans-serif}body::before{content:'';position:fixed;inset:0;pointer-events:none;background:linear-gradient(135deg,rgba(255,255,255,.4),transparent 42%,rgba(31,111,229,.06))}"
            ".page{position:relative;z-index:1;max-width:1380px;margin:0 auto;padding:28px 22px 72px}.hero{display:grid;grid-template-columns:minmax(0,1.25fr) minmax(320px,.95fr);gap:24px;margin-bottom:22px}.hero-copy,.hero-side,.card,.entry{background:var(--surface);backdrop-filter:blur(16px);border:1px solid rgba(255,255,255,.6);border-radius:var(--radius);box-shadow:var(--shadow);padding:28px}.dashboard-hero .summary{display:grid;grid-template-columns:1fr 1fr;gap:14px}.summary-tile{padding:16px;border-radius:22px;background:rgba(255,255,255,.72);border:1px solid var(--line);box-shadow:var(--shadow-soft)}"
            ".eyebrow{display:inline-flex;align-items:center;gap:8px;font-size:.76rem;letter-spacing:.12em;text-transform:uppercase;color:#416ea4;font-weight:700;margin-bottom:12px}.lead{font-size:1.06rem;max-width:58ch}.muted{color:var(--muted)}h1,h2,h3{font-family:'Space Grotesk','IBM Plex Sans',sans-serif;line-height:1.05;margin:0 0 12px}h1{font-size:clamp(2.4rem,4vw,4.4rem)}h2{font-size:clamp(1.25rem,2vw,2rem)}p{margin:0 0 10px}"
            ".entry-grid,.grid-layout{display:grid;gap:18px}.entry-grid{grid-template-columns:repeat(auto-fit,minmax(260px,1fr))}.entry{min-height:220px}.entry.telegram{background:linear-gradient(180deg,rgba(31,111,229,.08),rgba(255,255,255,.82))}.entry.vk{background:linear-gradient(180deg,rgba(39,135,245,.08),rgba(255,255,255,.82))}.card.primary{background:linear-gradient(180deg,rgba(31,111,229,.08),rgba(255,255,255,.88))}.card.spotlight{background:linear-gradient(180deg,rgba(15,118,110,.08),rgba(255,255,255,.88))}.card.blocked{background:linear-gradient(180deg,rgba(193,83,53,.08),rgba(255,255,255,.9))}"
            ".grid-layout{grid-template-columns:repeat(2,minmax(0,1fr))}.card,.auth-form{display:flex;flex-direction:column;gap:14px}.stack,.settings-grid,.grid.one{display:grid;gap:12px}.toolbar{display:flex;flex-wrap:wrap;gap:12px;align-items:center;justify-content:space-between;margin:0 0 18px}.ghost-link{display:inline-flex;align-items:center;justify-content:center;padding:11px 14px;border-radius:999px;border:1px solid var(--line);background:rgba(255,255,255,.68);text-decoration:none;color:var(--text);font-weight:700;box-shadow:var(--shadow-soft)}"
            "form{margin:0}label{display:grid;gap:8px;font-weight:600;color:var(--muted)}input,select,textarea{width:100%;padding:14px 15px;border-radius:18px;border:1px solid rgba(33,39,53,.14);background:rgba(255,255,255,.84);color:var(--text);font:inherit;outline:none;transition:border-color .16s ease,box-shadow .16s ease,transform .16s ease}textarea{resize:vertical;min-height:120px}input:focus,select:focus,textarea:focus{border-color:rgba(31,111,229,.55);box-shadow:0 0 0 4px rgba(31,111,229,.12);transform:translateY(-1px)}"
            "button{padding:13px 16px;border:0;border-radius:18px;background:linear-gradient(135deg,var(--accent),var(--accent-2));color:#fff;font-weight:700;cursor:pointer;box-shadow:0 14px 28px rgba(31,111,229,.24);transition:transform .16s ease,box-shadow .16s ease}button:hover{transform:translateY(-1px);box-shadow:0 18px 34px rgba(31,111,229,.28)}button.ghost{background:rgba(255,255,255,.78);color:var(--text);border:1px solid var(--line);box-shadow:none}.inline-form{display:inline-flex}.action-wrap{display:flex;align-items:end}"
            ".banner{padding:14px 16px;border-radius:18px;margin:0 0 14px;font-weight:700;border:1px solid transparent;box-shadow:var(--shadow-soft)}.ok{background:rgba(227,245,235,.92);border-color:rgba(16,185,129,.22);color:#0c6e48}.err{background:rgba(255,239,234,.96);border-color:rgba(193,83,53,.22);color:#8a2d1d}.pre{margin:0;white-space:pre-wrap;word-break:break-word;padding:16px 18px;border-radius:20px;background:rgba(250,246,239,.88);border:1px solid var(--line);box-shadow:inset 0 1px 0 rgba(255,255,255,.72)}.result-pre{min-height:180px}"
            ".command-grid,.choice-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px}.mini-list{display:grid;gap:10px}.mini-item{padding:14px 16px;border-radius:18px;background:rgba(255,255,255,.68);border:1px solid var(--line);box-shadow:var(--shadow-soft)}.mini-item strong{display:block;margin-bottom:4px}.mini-item span{display:block;color:var(--muted);font-size:.92rem}.mini-item p{margin-top:8px;color:var(--text)}.history-list{display:grid;gap:12px}.history-item{padding:14px 16px;border-radius:20px;background:rgba(255,255,255,.74);border:1px solid var(--line)}.history-item strong{display:block;margin-bottom:6px}.history-item p{white-space:pre-wrap}.pill{display:inline-flex;align-items:center;padding:8px 12px;border-radius:999px;background:var(--accent-soft);color:#25518f;font-size:.8rem;font-weight:700}.inline-note{padding:14px 15px;border-radius:18px;background:rgba(255,255,255,.76);border:1px dashed var(--line);color:var(--muted)}code{background:rgba(24,32,51,.08);padding:2px 6px;border-radius:8px}"
            ".workspace{display:grid;grid-template-columns:minmax(300px,380px) minmax(0,1fr);gap:18px;align-items:start}.sidebar-column,.chat-column{display:grid;gap:18px}.side-panel{gap:14px}.sticky-panel{position:sticky;top:18px}.tool-panel{border:1px solid var(--line);border-radius:20px;background:rgba(255,255,255,.62);padding:10px 12px}.tool-panel+ .tool-panel{margin-top:10px}.tool-panel summary{cursor:pointer;font-weight:700;list-style:none}.tool-panel summary::-webkit-details-marker{display:none}.tool-panel[open] summary{margin-bottom:12px}.compact-form textarea,.compact-form input,.compact-form select{margin:0}.settings-grid-web{display:grid;gap:12px}.compact-pre{max-height:260px;overflow:auto}.chat-shell{min-height:74vh;display:grid;grid-template-rows:auto auto auto minmax(320px,1fr);gap:14px}.chat-shell-head{display:flex;justify-content:space-between;gap:16px;align-items:flex-start}.chat-metrics{display:grid;grid-template-columns:repeat(2,minmax(92px,1fr));gap:10px;min-width:210px}.metric{display:grid;gap:4px;padding:12px 14px;border-radius:18px;background:rgba(255,255,255,.74);border:1px solid var(--line);text-align:center}.metric strong{font-size:1.2rem}.metric small{color:var(--muted)}.chat-composer{display:grid;gap:12px;padding:16px;border-radius:24px;background:linear-gradient(180deg,rgba(31,111,229,.08),rgba(255,255,255,.78));border:1px solid rgba(31,111,229,.14)}.chat-composer textarea{min-height:108px;border-radius:22px}.composer-actions{display:flex;justify-content:flex-end}.chat-log{display:grid;gap:12px;align-content:start;min-height:320px;max-height:calc(100vh - 320px);overflow:auto;padding-right:4px}.chat-row{display:flex}.chat-row.user{justify-content:flex-end}.chat-row.assistant{justify-content:flex-start}.chat-bubble{max-width:min(100%,760px);padding:16px 18px;border-radius:24px;border:1px solid var(--line);box-shadow:var(--shadow-soft)}.chat-row.user .chat-bubble{background:linear-gradient(135deg,rgba(31,111,229,.95),rgba(15,118,110,.92));color:#fff;border-color:transparent;border-bottom-right-radius:10px}.chat-row.assistant .chat-bubble{background:rgba(255,255,255,.82);border-bottom-left-radius:10px}.chat-row.assistant.latest .chat-bubble{background:linear-gradient(180deg,rgba(15,118,110,.08),rgba(255,255,255,.9))}.chat-meta{display:flex;gap:12px;justify-content:space-between;align-items:center;font-size:.78rem;letter-spacing:.08em;text-transform:uppercase;font-weight:700;opacity:.8;margin-bottom:8px}.chat-title{font-family:'Space Grotesk','IBM Plex Sans',sans-serif;font-size:1rem;font-weight:700;margin-bottom:8px}.chat-bubble p{margin:0;white-space:pre-wrap;word-break:break-word}.managed-choice{margin:0}"
            "@media (max-width:1120px){.hero,.grid-layout,.workspace{grid-template-columns:1fr}.dashboard-hero .summary{grid-template-columns:1fr 1fr}.sticky-panel{position:static}.chat-log{max-height:none}}@media (max-width:760px){.page{padding:16px 14px 46px}.hero-copy,.hero-side,.card,.entry{padding:20px}.dashboard-hero .summary,.chat-metrics{grid-template-columns:1fr}.toolbar{flex-direction:column;align-items:stretch}.ghost-link,.inline-form,button{width:100%}.chat-shell{min-height:auto;grid-template-rows:auto auto auto auto}.chat-composer{padding:14px}.chat-bubble{max-width:100%}.tool-panel{padding:10px}}"
        )

    async def _notify_custom_command_admins(self, context: PublicPlatformContext, session: PublicSiteSession, command_name: str) -> None:
        admin_ids = sorted(context.app_service.external_admin_user_ids())
        if not admin_ids:
            return
        text = (
            f"На сайте вызвали команду {command_name}.\n"
            f"Пользователь: {session.display_name}\n"
            f"User ID: {session.user_id}\n"
            f"Платформа: {context.title}"
        )
        for admin_id in admin_ids:
            try:
                await context.notification_gateway.notify_user(user_id=admin_id, text=text)
            except Exception:
                LOGGER.exception("Failed to notify admin %s about web command %s", admin_id, command_name)

    def _ensure_department_selected(self, context: PublicPlatformContext, session: PublicSiteSession) -> bool:
        if context.app_service.has_completed_department_survey(session.user_id):
            return True
        self._set_error(session, "Сначала выберите департамент.")
        return False

    def _resolve_managed_option(self, option_raw: str, options: list[ManagedAnswerOption]) -> ManagedAnswerOption | None:
        if option_raw.isdigit():
            option_id = int(option_raw)
            for option in options:
                if option.option_id == option_id:
                    return option
        normalized = option_raw.strip().lower()
        for option in options:
            if str(option.option_label).strip().lower() == normalized:
                return option
        return None

    def _apply_managed_answer_option(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        *,
        question: str,
        option: ManagedAnswerOption,
        remaining: int,
        unlimited_mode: bool,
    ) -> None:
        sender_profile = self._sender_profile(session)
        context.app_service.log_event(
            user_id=session.user_id,
            chat_id=session.user_id,
            event_type="managed_answer_choice",
            sender_profile=sender_profile,
            details={
                "option_id": option.option_id,
                "option_label": option.option_label,
                "has_media": bool(option.media_path),
                "surface": "web",
            },
        )
        assistant_text = option.response_text.strip() or f"Отправил вариант: {option.option_label}."
        if option.response_text.strip():
            self._set_result(
                session,
                f"Вариант: {option.option_label}",
                context.app_service.append_remaining(option.response_text.strip(), remaining, unlimited=unlimited_mode),
                user_text=question,
            )
            return
        if option.media_path:
            self._set_result(
                session,
                f"Вариант: {option.option_label}",
                context.app_service.append_remaining("Для этого варианта настроен файл, но сайт отвечает только текстом.", remaining, unlimited=unlimited_mode),
                user_text=question,
            )
            return
        self._set_result(
            session,
            f"Вариант: {option.option_label}",
            context.app_service.append_remaining("Для этого варианта пока не настроен ответ.", remaining, unlimited=unlimited_mode),
            user_text=question,
        )

    def _resolve_display_name(self, context: PublicPlatformContext, user_id: int) -> str:
        try:
            rows = context.app_service.get_user_statistics(str(user_id))
        except Exception:
            rows = []
        if rows:
            display_name = context.app_service.display_name(rows[0]).strip()
            if display_name and display_name != "-":
                return display_name
        return f"Пользователь {user_id}"

    def _sender_profile(self, session: PublicSiteSession) -> SenderProfile:
        return SenderProfile(first_name=session.display_name)

    @staticmethod
    def _append_history(chat_session: ChatSession, role: str, content: str) -> None:
        clean = content.strip()
        if clean:
            chat_session.recent_messages.append({"role": role, "content": clean})

    @staticmethod
    def _history_result_text(title: str, text: str) -> str:
        normalized = title.strip().lower()
        if normalized in {"ответ"}:
            return text
        return f"{title}\n\n{text}".strip()

    def _set_result(self, session: PublicSiteSession, title: str, text: str, *, user_text: str | None = None) -> None:
        session.result_title = title
        session.result_text = text
        session.notice_text = ""
        session.error_text = ""
        if user_text:
            self._append_history(session.chat_session, "user", user_text)
            self._append_history(session.chat_session, "assistant", self._history_result_text(title, text))

    def _set_notice(self, session: PublicSiteSession, text: str) -> None:
        session.notice_text = text
        session.error_text = ""

    def _set_error(self, session: PublicSiteSession, text: str) -> None:
        session.error_text = text
        session.notice_text = ""

    async def _read_simple_fields(self, request: web.Request) -> dict[str, str]:
        data = await request.post()
        return {key: str(value).strip() for key, value in data.items()}

    def _site_password(self) -> str:
        return self.settings.public_web_password.strip() or self.settings.bot_access_password.strip()

    def _check_password(self, password: str) -> bool:
        expected = self._site_password()
        if not expected:
            return True
        return secrets.compare_digest(password.strip(), expected)

    def _current_session(self, request: web.Request) -> PublicSiteSession | None:
        session_id = request.cookies.get(self.SESSION_COOKIE_NAME, "").strip()
        if not session_id:
            return None
        return self._sessions.get(session_id)

    def _require_session(self, request: web.Request) -> tuple[PublicPlatformContext, PublicSiteSession]:
        session = self._current_session(request)
        if session is None:
            raise web.HTTPFound("/")
        context = self.platforms.get(str(request.match_info.get("platform") or "").strip().lower())
        if context is None:
            raise web.HTTPFound("/")
        if session.platform_slug != context.slug:
            raise web.HTTPFound(self.platforms[session.platform_slug].base_path)
        return context, session

    def _ordered_platforms(self) -> list[PublicPlatformContext]:
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
    ) -> dict[str, PublicPlatformContext]:
        contexts: dict[str, PublicPlatformContext] = {}
        for slug, app_service in platform_services.items():
            gateway = notification_gateways.get(slug)
            if gateway is None:
                raise RuntimeError(f"Missing notification gateway for public platform: {slug}")
            title = "Letovo Assistant"
            subtitle = "Материалы, поиск, смены, промокоды и персональные настройки в одном веб-интерфейсе."
            accent = "telegram" if slug == "telegram" else "vk"
            contexts[slug] = PublicPlatformContext(
                slug=slug,
                title=title,
                subtitle=subtitle,
                accent=accent,
                app_service=app_service,
                notification_gateway=gateway,
            )
        return contexts

    def _find_site_account(self, username: str) -> tuple[PublicPlatformContext | None, dict[str, Any] | None]:
        normalized = username.strip().lower()
        if not normalized:
            return None, None
        for context in self._ordered_platforms():
            account = context.app_service.get_site_account(normalized)
            if account is not None:
                return context, account
        return None, None

    def _find_site_account_any(self, username: str) -> tuple[PublicPlatformContext | None, dict[str, Any] | None]:
        normalized = username.strip().lower()
        if not normalized:
            return None, None
        for context in self._ordered_platforms():
            account = context.app_service.get_site_account_any(normalized)
            if account is not None:
                return context, account
        return None, None

    async def _handle_login(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        if not username:
            return web.Response(text=self._render_landing(error_text="Введите логин сайта."), content_type="text/html", status=400)
        if not password:
            return web.Response(text=self._render_landing(error_text="Введите пароль сайта."), content_type="text/html", status=400)

        context, account = self._find_site_account(username)
        if context is None or account is None:
            return web.Response(
                text=self._render_landing(error_text="Доступ для этого логина еще не открыт. Обратитесь к команде проекта."),
                content_type="text/html",
                status=403,
            )
        if not verify_password(password, str(account.get("password_hash") or "")):
            return web.Response(text=self._render_landing(error_text="Неверный логин или пароль сайта."), content_type="text/html", status=403)

        try:
            user_id = int(account.get("platform_user_id") or 0)
        except (TypeError, ValueError):
            user_id = 0
        if user_id == 0:
            return web.Response(
                text=self._render_landing(error_text="Аккаунт сайта настроен некорректно. Обратитесь к команде проекта."),
                content_type="text/html",
                status=400,
            )

        banned, ban_reason = context.app_service.is_banned(user_id)
        if banned:
            reason_text = f"Причина: {ban_reason}" if ban_reason else "Доступ заблокирован."
            return web.Response(text=self._render_landing(error_text=reason_text), content_type="text/html", status=403)

        resolved_name = str(account.get("display_name") or "").strip() or self._resolve_display_name(context, user_id)
        return self._open_public_session(
            context=context,
            user_id=user_id,
            display_name=resolved_name,
            username=username,
            notice_text="Вход выполнен. Материалы, история и настройки уже доступны.",
        )

    async def _handle_register(self, request: web.Request) -> web.StreamResponse:
        fields = await self._read_simple_fields(request)
        username = fields.get("username", "").strip().lower()
        password = fields.get("password", "").strip()
        password_repeat = fields.get("password_repeat", "").strip()
        display_name = fields.get("display_name", "").strip()
        ordered_contexts = self._ordered_platforms()
        context = ordered_contexts[0] if ordered_contexts else None
        if context is None:
            return web.Response(text=self._render_landing(error_text="Сайт временно недоступен."), content_type="text/html", status=503)
        if len(username) < 3:
            return web.Response(text=self._render_landing(error_text="Логин должен содержать минимум 3 символа."), content_type="text/html", status=400)
        if any(ch.isspace() for ch in username):
            return web.Response(text=self._render_landing(error_text="Логин не должен содержать пробелы."), content_type="text/html", status=400)
        if len(password) < 8:
            return web.Response(text=self._render_landing(error_text="Пароль должен содержать минимум 8 символов."), content_type="text/html", status=400)
        if password != password_repeat:
            return web.Response(
                text=self._render_landing(error_text="Пароли не совпадают. Регистрация не выполнена."),
                content_type="text/html",
                status=400,
            )
        existing_context, existing_account = self._find_site_account_any(username)
        if existing_context is not None and existing_account is not None:
            return web.Response(text=self._render_landing(error_text="Такой логин уже занят."), content_type="text/html", status=409)

        user_id = context.app_service.next_site_platform_user_id()
        resolved_name = display_name or f"Пользователь {username}"
        context.app_service.upsert_site_account(
            username=username,
            password_hash=hash_password(password),
            display_name=resolved_name,
            platform_user_id=user_id,
            is_active=True,
        )
        context.app_service.log_event(
            user_id=user_id,
            chat_id=user_id,
            event_type="site_registration",
            sender_profile=SenderProfile(first_name=resolved_name),
            details={"platform": context.slug, "surface": "web"},
        )
        return self._open_public_session(
            context=context,
            user_id=user_id,
            display_name=resolved_name,
            username=username,
            notice_text="Регистрация завершена. Аккаунт сайта готов к работе.",
        )

    def _open_public_session(
        self,
        *,
        context: PublicPlatformContext,
        user_id: int,
        display_name: str,
        username: str,
        notice_text: str,
    ) -> web.StreamResponse:
        session_id = secrets.token_urlsafe(32)
        self._sessions[session_id] = PublicSiteSession(
            session_id=session_id,
            platform_slug=context.slug,
            user_id=user_id,
            display_name=display_name,
            chat_session=ChatSession(recent_messages=deque(maxlen=max(self.settings.conversation_context_messages * 2, 12))),
            username=username,
            notice_text=notice_text,
        )
        response = web.HTTPFound(context.base_path)
        response.set_cookie(self.SESSION_COOKIE_NAME, session_id, httponly=True, samesite="Lax")
        raise response

    def _render_landing(self, *, error_text: str) -> str:
        error_html = f'<div class="banner err">{escape(error_text)}</div>' if error_text else ""
        feature_cards = (
            '<section class="entry telegram"><span class="pill">Поиск</span><h2>Поиск по материалам</h2><p>Находите нужные сюжеты, события, компании и фрагменты по содержанию.</p><p class="muted">Память материалов и поиска доступна в одном месте.</p></section>'
            '<section class="entry vk"><span class="pill">Смены</span><h2>Работа со сменами</h2><p>Открывайте материалы по датам и именованным диапазонам в едином интерфейсе.</p><p class="muted">Сайт использует собственные аккаунты и отдельный вход.</p></section>'
            '<section class="entry telegram"><span class="pill">Настройки</span><h2>Персональный режим</h2><p>Сохраняйте API token, prompt и профиль ответа прямо в веб-версии.</p><p class="muted">Интерфейс одинаково удобно работает на компьютере и телефоне.</p></section>'
        )
        return (
            f"{self._head_html('Letovo Assistant')}"
            "<body class=\"site home\"><main class=\"page\">"
            "<section class=\"hero\">"
            "<div class=\"hero-copy\">"
            "<span class=\"eyebrow\">Публичный интерфейс</span>"
            "<h1>Letovo Assistant</h1>"
            "<p class=\"lead\">Единый веб-интерфейс для вопросов, поиска по материалам, сменам, промокодам и персональным настройкам.</p>"
            f"<p class=\"muted\">Сайт работает отдельно по адресу <code>{escape(self.settings.public_web_base_url)}</code> и использует собственный логин и пароль.</p>"
            f"{error_html}"
            "</div>"
            "<div class=\"hero-side\">"
            "<form method=\"post\" action=\"/login\" class=\"card auth-form\">"
            "<h2>Вход в сайт</h2>"
            "<label>Логин сайта<input name=\"username\" placeholder=\"Например: user_web\" autocomplete=\"username\" required></label>"
            "<label>Пароль сайта<input type=\"password\" name=\"password\" autocomplete=\"current-password\" required></label>"
            "<p class=\"hint\">Используйте логин и пароль сайта. После входа откроются ваши материалы, история и персональные настройки именно для веб-версии.</p>"
            "<div class=\"inline-note\">Если доступ еще не открыт, обратитесь к команде проекта.</div>"
            "<button type=\"submit\">Открыть сайт</button>"
            "</form>"
            "<form method=\"post\" action=\"/register\" class=\"card auth-form\" id=\"register-form\">"
            "<h2>Регистрация</h2>"
            "<label>Имя на сайте<input name=\"display_name\" placeholder=\"Как к вам обращаться\"></label>"
            "<label>Логин сайта<input name=\"username\" placeholder=\"Например: user_web\" autocomplete=\"username\" required></label>"
            "<label>Пароль<input type=\"password\" name=\"password\" id=\"register-password\" autocomplete=\"new-password\" required></label>"
            "<label>Повтор пароля<input type=\"password\" name=\"password_repeat\" id=\"register-password-repeat\" autocomplete=\"new-password\" required></label>"
            "<div class=\"banner err\" id=\"register-password-error\" style=\"display:none\"></div>"
            "<p class=\"hint\">После регистрации сайт создаст для вас отдельный веб-аккаунт и сразу откроет рабочее пространство. Пароль хранится только в виде хеша.</p>"
            "<button type=\"submit\">Создать аккаунт</button>"
            "</form>"
            "</div>"
            "</section>"
            f"<section class=\"entry-grid\">{feature_cards}</section>"
            "<script>"
            "(() => {"
            "const form = document.getElementById('register-form');"
            "const password = document.getElementById('register-password');"
            "const repeat = document.getElementById('register-password-repeat');"
            "const errorBox = document.getElementById('register-password-error');"
            "if (!form || !password || !repeat || !errorBox) return;"
            "const syncState = () => {"
            "if (password.value === repeat.value) {"
            "errorBox.style.display = 'none';"
            "errorBox.textContent = '';"
            "}"
            "};"
            "password.addEventListener('input', syncState);"
            "repeat.addEventListener('input', syncState);"
            "form.addEventListener('submit', (event) => {"
            "if (password.value !== repeat.value) {"
            "event.preventDefault();"
            "errorBox.textContent = 'Пароли не совпадают. Регистрация не выполнена.';"
            "errorBox.style.display = 'block';"
            "repeat.focus();"
            "}"
            "});"
            "})();"
            "</script>"
            "</main></body></html>"
        )

    def _site_context(self) -> PublicPlatformContext:
        ordered = self._ordered_platforms()
        if not ordered:
            raise RuntimeError("Для сайта не найден ни один активный сервис данных.")
        return ordered[0]

    def _cookie_secure(self) -> bool:
        return str(self.settings.public_web_base_url or "").strip().lower().startswith("https://")

    @staticmethod
    def _normalize_return_to(raw_value: str) -> str:
        normalized = raw_value.strip().lower()
        if normalized in {"settings", "account"}:
            return "settings"
        if normalized in {"settings-api", "api", "ai"}:
            return "settings-api"
        if normalized == "support":
            return "support"
        return "dashboard"

    def _page_response(
        self,
        context: PublicPlatformContext,
        session: PublicSiteSession,
        *,
        page: str,
        status: int = 200,
    ) -> web.Response:
        normalized = self._normalize_return_to(page)
        if normalized == "settings":
            return self._settings_response(context, session, status=status)
        if normalized == "settings-api":
            return self._api_settings_response(context, session, status=status)
        if normalized == "support":
            return self._support_response(context, session, status=status)
        return self._dashboard_response(context, session, status=status)

    def _render_public_nav(self, active: str) -> str:
        items = [
            ("dashboard", "/app", "Чат"),
            ("settings", "/settings", "Настройки"),
            ("settings-api", "/settings/api", "AI и API"),
            ("support", "/support", "Поддержка"),
        ]
        parts = ['<div class="toolbar">', '<div class="toolbar">']
        for key, href, label in items:
            if active == key:
                parts.append(f'<span class="pill">{escape(label)}</span>')
            else:
                parts.append(f'<a class="ghost-link" href="{href}">{escape(label)}</a>')
        parts.append("</div>")
        parts.append('<div class="toolbar">')
        parts.append('<a class="ghost-link" href="/">Главная</a>')
        parts.append('<form method="post" action="/logout" class="inline-form"><button type="submit" class="ghost">Выйти</button></form>')
        parts.append("</div>")
        parts.append("</div>")
        return "".join(parts)

    def _site_account(self, context: PublicPlatformContext, session: PublicSiteSession) -> dict[str, Any] | None:
        if not session.username:
            return None
        return context.app_service.get_site_account_any(session.username)

    async def _handle_root(self, request: web.Request) -> web.StreamResponse:
        if self._current_session(request) is not None:
            raise web.HTTPFound("/app")
        return web.Response(text=self._render_landing(error_text=""), content_type="text/html")

    async def _handle_login_page(self, request: web.Request) -> web.Response:
        if self._current_session(request) is not None:
            raise web.HTTPFound("/app")
        return web.Response(text=self._render_login_page(error_text=""), content_type="text/html")

    async def _handle_register_page(self, request: web.Request) -> web.Response:
        if self._current_session(request) is not None:
            raise web.HTTPFound("/app")
        return web.Response(text=self._render_register_page(error_text=""), content_type="text/html")

    async def _handle_logout(self, request: web.Request) -> web.StreamResponse:
        session_id = request.cookies.get(self.SESSION_COOKIE_NAME, "").strip()
        if session_id:
            self._sessions.pop(session_id, None)
        response = web.HTTPFound("/login")
        response.del_cookie(self.SESSION_COOKIE_NAME, path="/")
        raise response

    def _require_session(self, request: web.Request) -> tuple[PublicPlatformContext, PublicSiteSession]:
        session = self._current_session(request)
        if session is None:
            raise web.HTTPFound("/login")
        platform_slug = str(request.match_info.get("platform") or session.platform_slug).strip().lower()
        context = self.platforms.get(platform_slug) or self.platforms.get(session.platform_slug) or self._site_context()
        if session.platform_slug != context.slug:
            session.platform_slug = context.slug
        return context, session

    async def _handle_dashboard(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        return self._dashboard_response(context, session)

    async def _handle_settings_page(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        return self._settings_response(context, session)

    async def _handle_api_settings_page(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        return self._api_settings_response(context, session)

    async def _handle_support_page(self, request: web.Request) -> web.Response:
        context, session = self._require_session(request)
        return self._support_response(context, session)
