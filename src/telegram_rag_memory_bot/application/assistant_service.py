"""
????: assistant_service.py
??????? ?????????? ?????? ???????: ????????? ????????, ???????????,
??????????????, ???????????, ???????? ? orchestration RAG-????????.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
import re
from types import SimpleNamespace
from typing import Any

from telegram_rag_memory_bot.config import Settings
from telegram_rag_memory_bot.domain.models import LocalUploadRequest, LocalUploadResult, ManagedAnswerOption, SenderProfile
from telegram_rag_memory_bot.domain.ports import NotificationGateway, StorageGateway
from telegram_rag_memory_bot.schemas import SearchHit
from telegram_rag_memory_bot.services.rag_service import RagService
from telegram_rag_memory_bot.utils.dates import DateParseError, format_display_date, format_display_date_range, parse_iso_date, today_iso
from telegram_rag_memory_bot.utils.text import trim_text

LOGGER = logging.getLogger(__name__)


class AssistantApplicationService:
    DEPARTMENT_OPTIONS = ("X", "Дизайн", "Управление", "IT", "проект 11", "научный", "инженерный")
    PROMPT_PROFILE_LABELS = {
        "department": "Департаментный",
        "universal": "Универсальный",
        "brief": "Краткий",
        "deep": "Подробный",
    }
    DEPARTMENT_ACTIONS = {
        "X": {
            "mode_key": "investigation",
            "bucket": "dept_x",
            "button": "Расследование",
            "title": "Расследование",
            "days": 5,
            "system_prompt": (
                "Ты работаешь как цифровой детектив. Ищи связи между событиями последних дней, упоминаниями компаний,"
                " повторяющимися именами, датами, аномалиями и противоречиями. Если данных мало, честно обозначай"
                " гипотезы как гипотезы."
            ),
        },
        "Дизайн": {
            "mode_key": "design_review",
            "bucket": "dept_design",
            "button": "Дизайн-разбор",
            "title": "Дизайн-разбор",
            "days": 7,
            "system_prompt": "Ты дизайн-стратег. Выделяй визуальные паттерны, UX-риски, коммуникационный стиль и сильные/слабые стороны решений.",
        },
        "Управление": {
            "mode_key": "management_review",
            "bucket": "dept_management",
            "button": "Управленческий обзор",
            "title": "Управленческий обзор",
            "days": 7,
            "system_prompt": "Ты управленческий аналитик. Смотри на решения, риски, зависимости, эскалации, сроки и последствия для команды.",
        },
        "IT": {
            "mode_key": "tech_review",
            "bucket": "dept_it",
            "button": "Тех-анализ",
            "title": "Тех-анализ",
            "days": 7,
            "system_prompt": "Ты технический аналитик. Ищи причины сбоев, архитектурные узкие места, интеграционные риски и технические зависимости.",
        },
        "проект 11": {
            "mode_key": "project11_any",
            "bucket": "dept_project11_any",
            "button": "Режим проект 11",
            "title": "Режим проект 11",
            "days": 7,
            "system_prompt": "Ты универсальный стратег проекта 11. Выбирай наиболее полезный угол анализа сам и связывай данные между департаментами.",
        },
        "научный": {
            "mode_key": "science_hypothesis",
            "bucket": "dept_science",
            "button": "Научная гипотеза",
            "title": "Научная гипотеза",
            "days": 10,
            "system_prompt": "Ты научный аналитик. Формулируй гипотезы, отделяй факты от интерпретаций и указывай, какие данные подтверждают выводы.",
        },
        "инженерный": {
            "mode_key": "engineering_assessment",
            "bucket": "dept_engineering",
            "button": "Инженерная оценка",
            "title": "Инженерная оценка",
            "days": 10,
            "system_prompt": "Ты инженерный эксперт. Разбирай конструктивные ограничения, эксплуатационные риски, причины отказов и практические меры.",
        },
    }
    RISK_PATTERNS = {
        "critical": [
            (r"\b(бомб|взрыв|теракт|kill|убить|оружи|ammo|патрон)\b", 40, "насилие/оружие"),
            (r"\b(malware|ransomware|ботнет|ddos|эксплойт|exploit|взлом)\b", 35, "вредоносная активность"),
        ],
        "high": [
            (r"\b(phish|фишинг|carding|кардинг|dox|докс|утечк|stealer)\b", 24, "мошенничество/утечки"),
            (r"\b(drug|наркот|отмыв|blackmail|шантаж)\b", 20, "незаконные действия"),
        ],
        "medium": [
            (r"\b(proxy|tor|анонимиз|обход блокиров)\b", 10, "уклонение/обход"),
            (r"\b(скрипт для атаки|подбор парол|bruteforce|sqlmap)\b", 14, "подозрительная техника"),
        ],
    }
    platform_code = "telegram"

    def __init__(self, settings: Settings, rag_service: RagService) -> None:
        self.settings = settings
        self.rag_service = rag_service

    def is_authorized(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        if self.settings.public_access:
            return True
        allowed = self.settings.authorized_user_ids
        if not allowed:
            return False
        return user_id in allowed or user_id in self.settings.uploader_user_ids

    def is_admin(self, user_id: int | None) -> bool:
        if user_id is None:
            return False
        return user_id in self.settings.uploader_user_ids

    def get_user_preferences(self, user_id: int) -> dict[str, Any]:
        return self.rag_service.get_user_preferences(user_id)

    def get_active_api_key(self, user_prefs: dict[str, object]) -> str | None:
        api_key = (user_prefs.get("api_key") or "") if user_prefs else ""
        return str(api_key).strip() or None

    def get_active_prompt(self, user_prefs: dict[str, object]) -> str | None:
        if not self.get_active_api_key(user_prefs):
            return None
        prompt = (user_prefs.get("custom_prompt") or "") if user_prefs else ""
        return str(prompt).strip() or None

    def get_prompt_profile(self, user_prefs: dict[str, object]) -> str:
        raw = str((user_prefs.get("prompt_profile") or "department") if user_prefs else "department").strip().lower()
        return raw if raw in self.PROMPT_PROFILE_LABELS else "department"

    def prompt_profile_options(self) -> list[str]:
        return list(self.PROMPT_PROFILE_LABELS.values())

    def normalize_prompt_profile(self, text: str) -> str | None:
        normalized = text.strip().lower().strip("\"' .,!?:;")
        aliases = {
            "департаментный": "department",
            "department": "department",
            "универсальный": "universal",
            "universal": "universal",
            "краткий": "brief",
            "brief": "brief",
            "подробный": "deep",
            "deep": "deep",
        }
        return aliases.get(normalized)

    def save_user_prompt_profile(self, user_id: int, prompt_profile: str) -> None:
        self.rag_service.set_user_prompt_profile(user_id, prompt_profile)

    def clear_user_prompt_profile(self, user_id: int) -> None:
        self.rag_service.clear_user_prompt_profile(user_id)

    def is_banned(self, user_id: int | None) -> tuple[bool, str]:
        if user_id is None:
            return False, ""
        return self.rag_service.is_user_banned(user_id)

    def has_sent_welcome(self, user_id: int) -> bool:
        return self.rag_service.has_sent_welcome(user_id)

    def mark_welcome_sent(self, user_id: int) -> None:
        self.rag_service.mark_welcome_sent(user_id)

    def consume_daily_limit(self, user_id: int, *, has_personal_api: bool, is_admin: bool) -> tuple[bool, int, bool]:
        unlimited_mode = has_personal_api or is_admin
        if unlimited_mode:
            return True, self.settings.daily_message_limit, True
        allowed, _used, remaining = self.rag_service.database.consume_daily_user_message(
            user_id,
            today_iso(),
            self.settings.daily_message_limit,
        )
        if allowed:
            bonus_requests = self.rag_service.database.get_user_bonus_requests(user_id)
            return True, remaining + bonus_requests, False
        bonus_used, bonus_remaining = self.rag_service.database.consume_bonus_request(user_id)
        if bonus_used:
            return True, bonus_remaining, False
        return False, 0, False

    def log_event(
        self,
        *,
        user_id: int,
        chat_id: int,
        event_type: str,
        sender_profile: SenderProfile,
        charged: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.rag_service.log_user_event(
            user_id=user_id,
            chat_id=chat_id,
            event_type=event_type,
            event_date=today_iso(),
            charged=charged,
            username=sender_profile.username,
            first_name=sender_profile.first_name,
            last_name=sender_profile.last_name,
            details=details,
        )

    def validate_user_api_key(self, api_key: str) -> tuple[bool, str | None]:
        return self.rag_service.validate_user_api_key(api_key)

    def save_user_api_key(self, user_id: int, api_key: str) -> None:
        self.rag_service.set_user_api_key(user_id, api_key)

    def clear_user_api_key(self, user_id: int) -> None:
        self.rag_service.clear_user_api_key(user_id)

    def save_user_api_error(self, user_id: int, error_text: str) -> None:
        self.rag_service.set_user_api_key_error(user_id, error_text)

    def save_user_prompt(self, user_id: int, prompt: str) -> None:
        self.rag_service.set_user_prompt(user_id, prompt)

    def clear_user_prompt(self, user_id: int) -> None:
        self.rag_service.clear_user_prompt(user_id)

    def get_user_bonus_requests(self, user_id: int) -> int:
        prefs = self.get_user_preferences(user_id)
        return max(int(prefs.get("bonus_requests") or 0), 0)

    def redeem_promo_code(self, user_id: int, code: str) -> tuple[bool, str]:
        ok, message, _bonus = self.rag_service.database.redeem_promo_code(user_id, code, today_iso())
        return ok, message

    def create_promo_code(
        self,
        code: str,
        *,
        bonus_requests: int,
        note: str = "",
        max_redemptions: int | None = None,
        expires_at: str | None = None,
        enabled: bool = True,
    ) -> None:
        self.rag_service.database.create_promo_code(
            code,
            bonus_requests=bonus_requests,
            note=note,
            max_redemptions=max_redemptions,
            expires_at=expires_at,
            enabled=enabled,
        )

    def delete_promo_code(self, code: str) -> bool:
        return self.rag_service.database.delete_promo_code(code)

    def list_promo_codes(self) -> list[dict[str, Any]]:
        return self.rag_service.database.list_promo_codes()

    def create_custom_command(
        self,
        command_name: str,
        *,
        platform: str = "telegram",
        response_text: str,
        media_path: str | None,
        notify_admin: bool = True,
        enabled: bool = True,
    ) -> None:
        self.rag_service.database.create_or_update_custom_command(
            command_name,
            platform=platform,
            response_text=response_text,
            media_path=media_path,
            notify_admin=notify_admin,
            enabled=enabled,
        )

    def delete_custom_command(self, command_name: str, platform: str = "telegram") -> bool:
        return self.rag_service.database.delete_custom_command(command_name, platform=platform)

    def get_custom_command(self, command_name: str, platform: str = "telegram") -> dict[str, Any] | None:
        return self.rag_service.database.get_custom_command(command_name, platform=platform)

    def list_custom_commands(self, platform: str | None = None) -> list[dict[str, Any]]:
        return self.rag_service.database.list_custom_commands(platform=platform)

    def create_managed_answer_option(
        self,
        *,
        trigger_text: str,
        match_mode: str,
        option_label: str,
        response_text: str,
        media_path: str | None,
        sort_order: int = 100,
        enabled: bool = True,
    ) -> int:
        normalized_mode = "contains" if str(match_mode).strip().lower() == "contains" else "exact"
        return self.rag_service.database.create_managed_answer_option(
            trigger_text=trigger_text,
            match_mode=normalized_mode,
            option_label=option_label,
            response_text=response_text,
            media_path=media_path,
            sort_order=sort_order,
            enabled=enabled,
        )

    def delete_managed_answer_option(self, option_id: int) -> bool:
        return self.rag_service.database.delete_managed_answer_option(option_id)

    def list_managed_answer_options(self) -> list[dict[str, Any]]:
        return self.rag_service.database.list_managed_answer_options()

    def find_managed_answer_options(self, question: str) -> list[ManagedAnswerOption]:
        raw_matches = self.rag_service.database.find_managed_answer_options(question)
        if not raw_matches:
            return []
        grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in raw_matches:
            key = (str(row.get("match_mode") or "exact"), str(row.get("trigger_text") or ""))
            grouped.setdefault(key, []).append(row)
        sorted_groups = sorted(
            grouped.items(),
            key=lambda item: (
                0 if item[0][0] == "exact" else 1,
                -len(item[0][1]),
                min(int(row.get("sort_order") or 100) for row in item[1]),
                min(int(row.get("id") or 0) for row in item[1]),
            ),
        )
        _, selected_rows = sorted_groups[0]
        ordered_rows = sorted(selected_rows, key=lambda row: (int(row.get("sort_order") or 100), int(row.get("id") or 0)))
        return [
            ManagedAnswerOption(
                option_id=int(row["id"]),
                trigger_text=str(row.get("trigger_text") or ""),
                match_mode=str(row.get("match_mode") or "exact"),
                option_label=str(row.get("option_label") or "Вариант"),
                response_text=str(row.get("response_text") or ""),
                media_path=str(row["media_path"]) if row.get("media_path") else None,
            )
            for row in ordered_rows
        ]

    def list_recent_items(self, limit: int = 120) -> list[dict[str, Any]]:
        rows = self.rag_service.database.list_recent_items(limit=limit)
        return [self._decorate_item_with_shift(dict(row)) for row in rows]

    def create_pending_material_upload(self, upload_request: LocalUploadRequest, *, platform: str = "telegram") -> int:
        return self.rag_service.create_pending_material_upload(
            platform=platform,
            admin_user_id=upload_request.admin_user_id,
            content_date=upload_request.content_date,
            content_scope=upload_request.content_scope,
            description=upload_request.description,
            source_text=upload_request.source_text,
            local_file_path=str(upload_request.local_file_path) if upload_request.local_file_path is not None else None,
            original_file_name=upload_request.original_file_name,
        )

    def list_pending_material_uploads(self, *, status: str = "pending", platform: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.rag_service.list_pending_material_uploads(status=status, platform=platform, limit=limit)
        return [self._decorate_item_with_shift(dict(row)) for row in rows]

    def consume_pending_material_upload(
        self,
        *,
        claimed_chat_id: int,
        claimed_message_id: int,
        preferred_admin_user_id: int | None = None,
        preferred_platform: str | None = None,
    ) -> dict[str, Any] | None:
        return self.rag_service.consume_pending_material_upload(
            claimed_chat_id=claimed_chat_id,
            claimed_message_id=claimed_message_id,
            preferred_admin_user_id=preferred_admin_user_id,
            preferred_platform=preferred_platform,
        )

    def restore_pending_material_upload(self, pending_id: int) -> bool:
        return self.rag_service.restore_pending_material_upload(pending_id)

    def set_pending_material_upload_item(
        self,
        pending_id: int,
        *,
        item_id: int,
        local_file_path: str | None = None,
    ) -> bool:
        return self.rag_service.set_pending_material_upload_item(
            pending_id,
            item_id=item_id,
            local_file_path=local_file_path,
        )

    def complete_pending_material_upload(self, pending_id: int, *, item_id: int) -> bool:
        return self.rag_service.complete_pending_material_upload(pending_id, item_id=item_id)

    def delete_pending_material_upload(self, pending_id: int) -> bool:
        return self.rag_service.delete_pending_material_upload(pending_id)

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
        return self.rag_service.attach_item_source(
            item_id,
            source_chat_id=source_chat_id,
            source_message_id=source_message_id,
            source_sender_id=source_sender_id,
            telegram_message_date=telegram_message_date,
            metadata=metadata,
        )

    def normalize_content_scope(self, raw_scope: str | None) -> str:
        value = str(raw_scope or "dated").strip().lower()
        if value in {"timeless", "company", "company_timeless", "без даты", "компания"}:
            return "timeless"
        return "dated"

    def display_content_date(self, content_date: str | None, content_scope: str | None = None) -> str:
        if (content_scope or "dated") == "timeless":
            return "без даты"
        return format_display_date(content_date) or "без даты"

    def find_shift_for_content(self, content_date: str | None, content_scope: str | None = None) -> dict[str, Any] | None:
        if (content_scope or "dated") == "timeless":
            return None
        safe_date = str(content_date or "").strip()
        if not safe_date:
            return None
        return self.rag_service.find_shift_for_date(safe_date)

    def shift_label_for_content(self, content_date: str | None, content_scope: str | None = None) -> str | None:
        shift_row = self.find_shift_for_content(content_date, content_scope)
        if shift_row is None:
            return None
        return str(shift_row.get("name") or "").strip() or None

    def display_content_with_shift(self, content_date: str | None, content_scope: str | None = None) -> str:
        date_text = self.display_content_date(content_date, content_scope)
        shift_label = self.shift_label_for_content(content_date, content_scope)
        if shift_label:
            return f"{date_text} | смена: {shift_label}"
        return date_text

    def list_shifts(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.rag_service.list_shifts(limit=limit)

    def create_shift(self, *, name: str, date_from: str, date_to: str) -> int:
        return self.rag_service.create_shift(name=name, date_from=date_from, date_to=date_to)

    def update_shift(self, shift_id: int, *, name: str, date_from: str, date_to: str) -> bool:
        return self.rag_service.update_shift(shift_id, name=name, date_from=date_from, date_to=date_to)

    def delete_shift(self, shift_id: int) -> bool:
        return self.rag_service.delete_shift(shift_id)

    def department_options(self) -> list[str]:
        return list(self.DEPARTMENT_OPTIONS)

    def get_user_department(self, user_id: int) -> str | None:
        prefs = self.get_user_preferences(user_id)
        department = (prefs.get("department") or "") if prefs else ""
        department_text = str(department).strip()
        return department_text or None

    def has_completed_department_survey(self, user_id: int) -> bool:
        return bool(self.get_user_department(user_id))

    def normalize_department(self, text: str) -> str | None:
        normalized = text.strip().lower().strip("\"' .,!?:;")
        aliases = {
            "x": "X",
            "дизайн": "Дизайн",
            "управление": "Управление",
            "it": "IT",
            "ит": "IT",
            "project 11": "проект 11",
            "проект 11": "проект 11",
            "научный": "научный",
            "инженерный": "инженерный",
        }
        return aliases.get(normalized)

    def save_user_department(self, user_id: int, department: str) -> None:
        self.rag_service.set_user_department(user_id, department)

    def department_action_for_user(self, user_id: int) -> dict[str, str] | None:
        department = self.get_user_department(user_id)
        if not department:
            return None
        action = self.DEPARTMENT_ACTIONS.get(department)
        if not action:
            return None
        return {"department": department, **{key: str(value) for key, value in action.items()}}

    def department_button_label(self, user_id: int) -> str | None:
        action = self.department_action_for_user(user_id)
        return action.get("button") if action else None

    def all_department_action_labels(self) -> list[str]:
        labels: list[str] = []
        for department in self.DEPARTMENT_OPTIONS:
            if department == "проект 11":
                continue
            action = self.DEPARTMENT_ACTIONS.get(department)
            if action:
                labels.append(str(action["button"]))
        return labels

    def resolve_department_action_by_label(self, text: str, user_department: str | None) -> dict[str, str] | None:
        normalized = text.strip().lower().strip("\"' .,!?:;")
        if user_department == "проект 11":
            for department, action in self.DEPARTMENT_ACTIONS.items():
                if normalized == str(action["button"]).lower():
                    return {"department": department, **{key: str(value) for key, value in action.items()}}
        if not user_department:
            return None
        action = self.DEPARTMENT_ACTIONS.get(user_department)
        if action and normalized == str(action["button"]).lower():
            return {"department": user_department, **{key: str(value) for key, value in action.items()}}
        return None

    def department_action_picker_prompt(self) -> str:
        return "Проект 11 может использовать любую департаментную функцию 1 раз в день. Дополнительные спец-запросы может выдать администратор. Выберите режим кнопкой ниже."

    def department_mode_bucket_for_user(self, user_id: int, action_department: str) -> str:
        user_department = self.get_user_department(user_id)
        if user_department == "проект 11":
            return str(self.DEPARTMENT_ACTIONS["проект 11"]["bucket"])
        action = self.DEPARTMENT_ACTIONS.get(action_department) or {}
        return str(action.get("bucket") or f"dept_{action_department.lower()}")

    def consume_department_action_limit(self, user_id: int, action_department: str) -> tuple[bool, int, bool, str]:
        bucket = self.department_mode_bucket_for_user(user_id, action_department)
        allowed, _used, remaining = self.rag_service.consume_daily_department_mode(user_id, today_iso(), bucket, daily_limit=1)
        if allowed:
            return True, remaining, False, bucket
        bonus_used, bonus_remaining = self.rag_service.consume_mode_credit(user_id, bucket)
        if bonus_used:
            return True, bonus_remaining, True, bucket
        return False, 0, False, bucket

    def grant_department_special_requests(self, user_id: int, credits: int) -> tuple[str, str]:
        safe_credits = max(int(credits), 0)
        if safe_credits <= 0:
            raise ValueError("Количество спец-запросов должно быть больше нуля.")
        department = self.get_user_department(user_id)
        if not department:
            raise ValueError("У пользователя не выбран департамент.")
        bucket = self.department_mode_bucket_for_user(user_id, department)
        self.rag_service.add_mode_credits(user_id, bucket, safe_credits)
        return department, bucket

    def build_effective_prompt(
        self,
        *,
        department: str | None,
        prompt_profile: str,
        custom_prompt: str | None,
        department_prompt: str | None = None,
    ) -> str | None:
        prompt_parts: list[str] = []
        base_prompt = department_prompt or self._department_prompt_text(department)
        if prompt_profile == "department":
            prompt_parts.append(base_prompt)
        elif prompt_profile == "universal":
            prompt_parts.append("Отвечай универсально, нейтрально и полезно для смешанных задач без узкого профессионального уклона.")
        elif prompt_profile == "brief":
            prompt_parts.append((base_prompt + " Отвечай максимально коротко, без потери ключевых выводов.").strip())
        elif prompt_profile == "deep":
            prompt_parts.append((base_prompt + " Делай ответ глубже: выводы, аргументы, риски, альтернативы и явные допущения.").strip())
        if custom_prompt:
            prompt_parts.append(custom_prompt.strip())
        joined = "\n\n".join(part for part in prompt_parts if part.strip())
        return joined or None

    def create_access_request(
        self,
        *,
        user_id: int,
        request_name: str,
        reason: str,
        request_type: str,
        mode_bucket: str | None = None,
    ) -> int:
        return self.rag_service.create_access_request(
            user_id=user_id,
            platform=self.platform_code,
            request_type=request_type,
            request_name=request_name,
            reason=reason,
            mode_bucket=mode_bucket,
        )

    def list_access_requests(self, status: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
        requests = self.rag_service.list_access_requests(status=status, limit=limit)
        return [row for row in requests if str(row.get("platform") or "telegram") == self.platform_code]

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
        return self.rag_service.review_access_request(
            request_id,
            status=status,
            reviewed_by_user_id=reviewed_by_user_id,
            decision_note=decision_note,
            granted_bonus_requests=granted_bonus_requests,
            granted_mode_credits=granted_mode_credits,
        )

    def set_ban(self, user_id: int, *, reason: str, banned_by_user_id: int) -> None:
        self.rag_service.set_ban(user_id, reason=reason, banned_by_user_id=banned_by_user_id)

    def clear_ban(self, user_id: int) -> bool:
        return self.rag_service.clear_ban(user_id)

    def list_banned_users(self, limit: int = 200) -> list[dict[str, Any]]:
        return self.rag_service.list_banned_users(limit=limit)

    def list_user_events(self, user_id: int | None = None, limit: int = 300) -> list[dict[str, Any]]:
        return self.rag_service.list_user_events(user_id=user_id, limit=limit)

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

    def resolve_request(self, text: str) -> tuple[str, str]:
        if text.startswith("/search "):
            return "search", text[len("/search ") :].strip()
        if text.startswith("/ask "):
            return "ask", text[len("/ask ") :].strip()
        if text.startswith("/list "):
            return "list", text[len("/list ") :].strip()
        if text.startswith("/file "):
            return "file", text[len("/file ") :].strip()
        if text.startswith("/stats "):
            return "stats", text[len("/stats ") :].strip()
        return "ask", text.strip()

    def search(self, query: str, *, api_key: str | None) -> list[SearchHit]:
        return self.rag_service.search(query, api_key=api_key)

    def list_by_date(self, raw_date: str) -> tuple[str, list[dict[str, Any]]]:
        raw = raw_date.strip()
        if not raw:
            raise ValueError("Укажите дату в формате DD-MM-YYYY или название смены.")
        try:
            content_date = parse_iso_date(raw.split()[0])
        except Exception:
            shift_row = self.rag_service.find_shift_by_query(raw)
            if shift_row is None:
                raise ValueError("Укажите дату в формате DD-MM-YYYY или точное название смены.")
            items = [
                self._decorate_item_with_shift(dict(item))
                for item in self.rag_service.list_items_in_date_range(str(shift_row["date_from"]), str(shift_row["date_to"]), limit=120)
                if str(item.get("content_scope") or "dated") != "timeless"
            ]
            label = f"{shift_row['name']} ({format_display_date_range(str(shift_row['date_from']), str(shift_row['date_to']))})"
            return label, items
        items = [self._decorate_item_with_shift(dict(item)) for item in self.rag_service.list_by_date(content_date)]
        return self.display_content_with_shift(content_date, "dated"), items

    def get_item(self, item_id: int) -> dict[str, Any] | None:
        return self.rag_service.get_item(item_id)

    def describe_item_for_text_only(self, item: dict[str, Any]) -> str:
        item_id = int(item.get("id") or 0)
        item_type = str(item.get("item_type") or "-")
        file_name = str(item.get("file_name") or "-")
        content_scope = str(item.get("content_scope") or "dated")
        content_date = self.display_content_with_shift(str(item.get("content_date") or ""), content_scope)
        summary = str(item.get("summary") or "").strip() or "Краткое описание пока отсутствует."
        source_ready = bool(int(item.get("source_chat_id") or 0) and int(item.get("source_message_id") or 0))
        lines = [
            f"Материал #{item_id}",
            f"Дата: {content_date}",
            f"Тип: {item_type}",
            f"Файл: {file_name}",
            "",
            summary,
            "",
            "Отправка файлов ботом отключена. Доступно только текстовое описание.",
        ]
        if source_ready:
            lines.append("Оригинал привязан в хранилище, но бот его не пересылает.")
        return "\n".join(lines)

    def delete_item(self, item_id: int) -> bool:
        return self.rag_service.delete_item_by_id(item_id)

    def merge_pending_upload_caption(self, pending_upload: dict[str, Any], original_caption: str) -> str:
        description = str(pending_upload.get("description") or "").strip()
        original_caption = str(original_caption or "").strip()
        parts: list[str] = []
        if description:
            parts.append(f"Описание из админки: {description}")
        if original_caption and original_caption != description:
            parts.append(f"Подпись в группе: {original_caption}")
        merged = "\n".join(parts).strip()
        return trim_text(merged or original_caption or description, 1600)

    def pending_upload_source_text(self, pending_upload: dict[str, Any]) -> str:
        source_text = str(pending_upload.get("source_text") or "").strip()
        return trim_text(source_text, self.settings.max_index_chars)

    async def analyze_pending_material_upload(
        self,
        pending_id: int,
        upload_request: LocalUploadRequest,
    ) -> LocalUploadResult:
        LOGGER.info(
            "Localhost upload: запускаю немедленный AI-анализ заявки | pending_id=%s | admin=%s | файл=%s",
            pending_id,
            upload_request.admin_user_id,
            upload_request.original_file_name or "text-only",
        )
        synthetic_message = self._build_pending_message_stub(pending_id, upload_request)
        caption = self._manual_upload_caption(upload_request)
        ingested = await self.rag_service.ingest_message(
            client=None,
            message=synthetic_message,
            content_date=upload_request.content_date,
            content_scope=upload_request.content_scope,
            ingested_by_user_id=upload_request.admin_user_id,
            local_media_path=upload_request.local_file_path,
            caption_override=caption,
            source_text_hint=upload_request.source_text,
        )
        self.set_pending_material_upload_item(
            pending_id,
            item_id=ingested.item_id,
            local_file_path=None,
        )
        if upload_request.local_file_path is not None:
            upload_request.local_file_path.unlink(missing_ok=True)
        LOGGER.info(
            "Localhost upload: AI-анализ завершен | pending_id=%s | item_id=%s",
            pending_id,
            ingested.item_id,
        )
        return LocalUploadResult(
            item_id=ingested.item_id,
            item_type=ingested.item_type,
            file_name=ingested.file_name,
            content_date=ingested.content_date,
            summary=ingested.summary,
            storage_chat_id=0,
            storage_message_id=0,
        )

    def retrieve_answer_hits(
        self,
        question: str,
        *,
        recent_messages: list[dict[str, str]],
        api_key: str | None,
    ) -> list[SearchHit]:
        return self.rag_service.retrieve_relevant_hits(
            question,
            recent_messages=recent_messages,
            api_key=api_key,
            limit=self.settings.max_context_chunks,
            unique_by_item=True,
        )

    def answer_from_hits(
        self,
        *,
        question: str,
        hits: list[SearchHit],
        recent_messages: list[dict[str, str]],
        api_key: str | None,
        custom_prompt: str | None,
    ) -> str:
        decorated_hits = self._decorate_hits_with_shifts(hits)
        answer = self.rag_service.answer_from_hits(
            question,
            decorated_hits,
            recent_messages=recent_messages,
            api_key=api_key,
            custom_prompt=custom_prompt,
        )
        shift_note = self._build_shift_note_for_hits(decorated_hits)
        if shift_note:
            return f"{answer}\n\n{shift_note}"
        return answer

    def _decorate_item_with_shift(self, row: dict[str, Any]) -> dict[str, Any]:
        payload = dict(row)
        shift_row = self.find_shift_for_content(str(payload.get("content_date") or ""), str(payload.get("content_scope") or "dated"))
        payload["shift_id"] = int(shift_row.get("id") or 0) if shift_row else 0
        payload["shift_name"] = str(shift_row.get("name") or "").strip() if shift_row else ""
        payload["shift_date_from"] = str(shift_row.get("date_from") or "").strip() if shift_row else ""
        payload["shift_date_to"] = str(shift_row.get("date_to") or "").strip() if shift_row else ""
        return payload

    def _decorate_hits_with_shifts(self, hits: list[SearchHit]) -> list[SearchHit]:
        for hit in hits:
            shift_row = self.find_shift_for_content(hit.content_date, getattr(hit, "content_scope", "dated"))
            if shift_row is None:
                hit.metadata.pop("shift_id", None)
                hit.metadata.pop("shift_name", None)
                hit.metadata.pop("shift_date_from", None)
                hit.metadata.pop("shift_date_to", None)
                continue
            hit.metadata["shift_id"] = int(shift_row.get("id") or 0)
            hit.metadata["shift_name"] = str(shift_row.get("name") or "").strip()
            hit.metadata["shift_date_from"] = str(shift_row.get("date_from") or "").strip()
            hit.metadata["shift_date_to"] = str(shift_row.get("date_to") or "").strip()
        return hits

    def _build_shift_note_for_hits(self, hits: list[SearchHit]) -> str:
        shift_labels: list[str] = []
        seen: set[str] = set()
        for hit in hits:
            shift_label = str(hit.metadata.get("shift_name") or "").strip()
            if not shift_label or shift_label in seen:
                continue
            seen.add(shift_label)
            shift_labels.append(shift_label)
        if len(shift_labels) < 2:
            return ""
        return "Смены в найденных материалах: " + "; ".join(shift_labels) + "."

    def run_department_action(
        self,
        *,
        user_id: int,
        action_department: str,
        question: str,
        recent_messages: list[dict[str, str]],
        api_key: str | None,
        custom_prompt: str | None,
        prompt_profile: str,
    ) -> tuple[str, list[SearchHit], str]:
        action = self.DEPARTMENT_ACTIONS.get(action_department)
        if action is None:
            raise ValueError("Для выбранного департамента специальный режим не настроен.")
        days_window = int(action.get("days", 7))
        date_to = today_iso()
        date_from = self._days_ago_iso(days_window - 1)
        base_hits = self.rag_service.retrieve_relevant_hits(
            f"{question}\nПоследние {days_window} дней",
            recent_messages=recent_messages,
            api_key=api_key,
            limit=10,
            unique_by_item=True,
        )
        base_hits = [hit for hit in base_hits if hit.content_scope == "timeless" or (date_from <= (hit.content_date or date_to) <= date_to)]
        company_names = self._extract_company_candidates(base_hits)
        expanded_hits: list[SearchHit] = list(base_hits)
        seen_item_ids = {hit.item_id for hit in expanded_hits}
        for company_name in company_names[:6]:
            for hit in self.rag_service.search(company_name, limit=3, api_key=api_key):
                if hit.item_id not in seen_item_ids:
                    expanded_hits.append(hit)
                    seen_item_ids.add(hit.item_id)
        effective_prompt = self.build_effective_prompt(
            department=action_department,
            prompt_profile=prompt_profile,
            custom_prompt=custom_prompt,
            department_prompt=str(action["system_prompt"]),
        )
        enriched_question = (
            f"{question}\n\n"
            f"Режим анализа: {action['title']}.\n"
            f"Окно анализа: {format_display_date_range(date_from, date_to)}.\n"
            f"Компании/сущности для дополнительной проверки: {', '.join(company_names) if company_names else 'не выявлены'}.\n"
            "Если строишь теорию, явно помечай ее как гипотезу."
        )
        decorated_hits = self._decorate_hits_with_shifts(expanded_hits[: self.settings.max_context_chunks])
        answer = self.rag_service.answer_from_hits(
            enriched_question,
            decorated_hits,
            recent_messages=recent_messages,
            api_key=api_key,
            custom_prompt=effective_prompt,
            model=self.settings.analysis_model,
        )
        shift_note = self._build_shift_note_for_hits(decorated_hits)
        if shift_note:
            answer = f"{answer}\n\n{shift_note}"
        return answer, expanded_hits, date_from

    def available_delivery_formats(self, hits: list[SearchHit]) -> list[str]:
        return ["текст"]
        formats: list[str] = []
        if any(hit.item_type == "image" for hit in hits):
            formats.append("фото")
        if any(hit.item_type == "audio" for hit in hits):
            formats.append("аудио")
        formats.append("текст")
        return formats

    def normalize_delivery_choice(self, text: str) -> str | None:
        normalized = text.strip().lower().strip("\"' .,!?:;")
        if normalized in {"отмена", "cancel"}:
            return "cancel"
        if normalized in {
            "текст",
            "text",
            "txt",
            "фото",
            "photo",
            "image",
            "картинка",
            "аудио",
            "audio",
            "voice",
            "голос",
            "видео",
            "video",
        }:
            return "текст"
        aliases = {
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

    def hits_for_delivery_choice(self, hits: list[SearchHit], choice: str, limit: int = 3) -> list[SearchHit]:
        type_map = {
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

    def get_user_statistics(self, raw_arg: str) -> list[dict[str, Any]]:
        raw_arg = raw_arg.strip()
        if raw_arg:
            if not raw_arg.isdigit():
                raise ValueError("Использование: /stats или /stats USER_ID")
            rows = self.rag_service.get_user_statistics(today_iso(), limit=1, user_id=int(raw_arg))
        else:
            rows = self.rag_service.get_user_statistics(today_iso(), limit=500)
        return [self._enrich_stats_row(row) for row in rows]

    def format_user_stats_row(self, row: dict[str, Any]) -> str:
        username = f"@{row['username']}" if row.get("username") else "-"
        api_text = "да" if row.get("has_api") else "нет"
        department = row.get('department') or '-'
        bonus_requests = row.get('bonus_requests') or 0
        return (
            f"{row['user_id']} | {username} | {self.display_name(row)} | dept {department} | risk {row.get('risk_level', 'low')}:{row.get('risk_score', 0)} | bonus {bonus_requests} | "
            f"сегодня {row['total_today_count']}/{row['charged_today_count']} | "
            f"ask {row['ask_count']} | search {row['search_count']} | list {row['list_count']} | file {row['file_count']} | "
            f"add {row['manual_add_count']} | media {row['media_delivery_count']} | api {api_text} | last {row['last_seen_at']}"
        )

    def format_detailed_user_stats(self, row: dict[str, Any]) -> str:
        username = f"@{row['username']}" if row.get("username") else "-"
        lines = [
            f"Пользователь: {row['user_id']}",
            f"Username: {username}",
            f"Имя: {self.display_name(row)}",
            f"Департамент: {row.get('department') or '-'}",
            f"Профиль prompt: {self.PROMPT_PROFILE_LABELS.get(str(row.get('prompt_profile') or 'department'), 'Департаментный')}",
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
            f"Бонусные запросы: {row.get('bonus_requests') or 0}",
            f"Бан: {'да' if row.get('is_banned') else 'нет'}",
            f"Risk score: {row.get('risk_score', 0)} ({row.get('risk_level', 'low')})",
        ]
        if row.get("ban_reason"):
            lines.append(f"Причина бана: {row['ban_reason']}")
        if row.get("risk_tags"):
            lines.append("Риск-теги: " + ", ".join(row["risk_tags"]))
        return "\n".join(lines)

    def append_remaining(self, text: str, remaining: int, *, unlimited: bool = False) -> str:
        return f"{text}\n\n{self.remaining_line(remaining, unlimited=unlimited)}"

    @staticmethod
    def remaining_line(remaining: int, *, unlimited: bool = False) -> str:
        if unlimited:
            return "Режим: безлимит."
        return f"Осталось запросов: {remaining}."

    @staticmethod
    def display_name(row: dict[str, Any]) -> str:
        name_parts = [part for part in [row.get("first_name"), row.get("last_name")] if part]
        return " ".join(name_parts) or "-"

    def recent_event_rows_for_user(self, user_id: int, limit: int = 80) -> list[dict[str, Any]]:
        rows = self.list_user_events(user_id=user_id, limit=limit)
        enriched: list[dict[str, Any]] = []
        for row in rows:
            risk = self.compute_risk_for_event(row)
            payload = dict(row)
            payload.update(risk)
            enriched.append(payload)
        return enriched

    def compute_risk_for_event(self, event_row: dict[str, Any]) -> dict[str, Any]:
        details = event_row.get("details") or {}
        text_parts = [
            str(details.get("question") or ""),
            str(details.get("query") or ""),
            str(details.get("reason") or ""),
            str(details.get("command") or ""),
        ]
        return self.compute_risk_profile("\n".join(part for part in text_parts if part.strip()))

    def compute_risk_profile(self, text: str) -> dict[str, Any]:
        normalized = text.lower()
        score = 0
        tags: list[str] = []
        for _severity, patterns in self.RISK_PATTERNS.items():
            for pattern, weight, tag in patterns:
                if re.search(pattern, normalized, flags=re.IGNORECASE):
                    score += weight
                    if tag not in tags:
                        tags.append(tag)
        if len(normalized) > 400 and score > 0:
            score += 4
        if score >= 45:
            level = "critical"
        elif score >= 25:
            level = "high"
        elif score >= 10:
            level = "medium"
        else:
            level = "low"
        return {"risk_score": score, "risk_level": level, "risk_tags": tags}

    def build_limit_request_prompt(self, *, department_mode: bool = False) -> str:
        if department_mode:
            return "Лимит департаментного режима на сегодня исчерпан. Можно оставить заявку администратору."
        return "Лимит запросов на сегодня исчерпан. Можно оставить заявку администратору."

    def _enrich_stats_row(self, row: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(row)
        recent_events = self.list_user_events(user_id=int(row["user_id"]), limit=80)
        risk_text = "\n".join(
            str((event.get("details") or {}).get(key) or "")
            for event in recent_events
            for key in ("question", "query", "reason")
        )
        enriched.update(self.compute_risk_profile(risk_text))
        return enriched

    def _department_prompt_text(self, department: str | None) -> str:
        if not department:
            return "Отвечай полезно, ясно и по делу."
        action = self.DEPARTMENT_ACTIONS.get(department)
        if action:
            return str(action["system_prompt"])
        return f"Отвечай как эксперт направления: {department}."

    def _days_ago_iso(self, days: int) -> str:
        base_day = date.fromisoformat(today_iso())
        return (base_day - timedelta(days=max(days, 0))).isoformat()

    def _extract_company_candidates(self, hits: list[SearchHit]) -> list[str]:
        candidates: list[str] = []
        for hit in hits:
            entities = hit.metadata.get("entities") if isinstance(hit.metadata, dict) else []
            for entity in entities or []:
                entity_text = str(entity).strip()
                if len(entity_text) >= 3 and entity_text not in candidates:
                    candidates.append(entity_text)
        return candidates

    async def process_local_upload(
        self,
        upload_request: LocalUploadRequest,
        *,
        storage_gateway: StorageGateway,
        notification_gateway: NotificationGateway | None = None,
    ) -> LocalUploadResult:
        LOGGER.info(
            "Localhost upload: получен материал | admin=%s | дата=%s | файл=%s",
            upload_request.admin_user_id,
            upload_request.content_date,
            upload_request.original_file_name or "текст без файла",
        )
        caption = self._manual_upload_caption(upload_request)
        LOGGER.info("Localhost upload: отправляю материал в Telegram-группу хранения")
        if upload_request.local_file_path is not None:
            stored_message = await storage_gateway.store_file(
                local_file_path=upload_request.local_file_path,
                caption=caption,
                original_file_name=upload_request.original_file_name,
            )
        else:
            stored_message = await storage_gateway.store_text(text=self._manual_upload_body(upload_request))

        LOGGER.info(
            "Localhost upload: материал отправлен в Telegram | chat_id=%s | message_id=%s",
            getattr(stored_message, "chat_id", self.settings.storage_chat_id),
            getattr(stored_message, "id", None),
        )
        LOGGER.info("Localhost upload: запускаю индексацию материала")
        ingested = await self.rag_service.ingest_message(
            client=storage_gateway.client,
            message=stored_message,
            content_date=upload_request.content_date,
            content_scope=upload_request.content_scope,
            ingested_by_user_id=upload_request.admin_user_id,
            local_media_path=upload_request.local_file_path,
            caption_override=caption,
            source_text_hint=upload_request.source_text,
        )
        result = LocalUploadResult(
            item_id=ingested.item_id,
            item_type=ingested.item_type,
            file_name=ingested.file_name,
            content_date=ingested.content_date,
            summary=ingested.summary,
            storage_chat_id=int(getattr(stored_message, "chat_id", self.settings.storage_chat_id)),
            storage_message_id=int(getattr(stored_message, "id", 0) or 0),
        )
        LOGGER.info("Localhost upload: индексация завершена | item_id=%s", result.item_id)
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

    @staticmethod
    def profile_from_sender(sender: Any) -> SenderProfile:
        if sender is None:
            return SenderProfile()
        return SenderProfile(
            username=getattr(sender, "username", None),
            first_name=getattr(sender, "first_name", None),
            last_name=getattr(sender, "last_name", None),
        )

    def _manual_upload_caption(self, upload_request: LocalUploadRequest) -> str:
        parts = [
            f"Категория: {'компания / без даты' if upload_request.content_scope == 'timeless' else 'материал с датой'}",
            f"Описание: {upload_request.description.strip()}",
        ]
        if upload_request.content_scope != "timeless" and upload_request.content_date:
            parts.insert(0, f"Дата: {format_display_date(upload_request.content_date)}")
        source_text = upload_request.source_text.strip()
        if source_text and source_text != upload_request.description.strip():
            parts.append(f"Исходный текст: {trim_text(source_text, 450)}")
        return trim_text("\n".join(parts), 900)

    def _manual_upload_body(self, upload_request: LocalUploadRequest) -> str:
        parts = [
            f"Категория: {'компания / без даты' if upload_request.content_scope == 'timeless' else 'материал с датой'}",
            f"Описание: {upload_request.description.strip()}",
        ]
        if upload_request.content_scope != "timeless" and upload_request.content_date:
            parts.insert(0, f"Дата: {format_display_date(upload_request.content_date)}")
        source_text = upload_request.source_text.strip()
        if source_text and source_text != upload_request.description.strip():
            parts.extend(["", trim_text(source_text, 3000)])
        return trim_text("\n".join(parts), 3500)

    @staticmethod
    def _pending_stub_chat_id(pending_id: int) -> int:
        return -900_000_000 - max(int(pending_id), 1)

    @staticmethod
    def _pending_stub_message_id(pending_id: int) -> int:
        return -max(int(pending_id), 1)

    def _build_pending_message_stub(self, pending_id: int, upload_request: LocalUploadRequest) -> SimpleNamespace:
        body = self._manual_upload_body(upload_request)
        return SimpleNamespace(
            chat_id=self._pending_stub_chat_id(pending_id),
            id=self._pending_stub_message_id(pending_id),
            sender_id=upload_request.admin_user_id,
            date=datetime.now(timezone.utc),
            raw_text=body,
            message=body,
            media=None,
            file=None,
            photo=None,
            video=None,
            audio=None,
            voice=None,
            document=None,
        )





