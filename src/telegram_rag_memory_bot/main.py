"""
Файл: main.py
Собирает зависимости приложения и запускает Telegram-бота,
VK-бота и локальную HTTP-админку в одном runtime.
"""

from __future__ import annotations

import asyncio
import logging

from telegram_rag_memory_bot.application.telegram.service import TelegramAssistantApplicationService
from telegram_rag_memory_bot.application.vk.service import VkAssistantApplicationService
from telegram_rag_memory_bot.config import get_settings
from telegram_rag_memory_bot.infrastructure.http.public_web_server import PublicWebServer
from telegram_rag_memory_bot.infrastructure.telegram.menu_bot import TelegramMenuBot
from telegram_rag_memory_bot.infrastructure.vk.menu_bot import VkMenuBot
from telegram_rag_memory_bot.services.database import Database
from telegram_rag_memory_bot.services.media_service import MediaService
from telegram_rag_memory_bot.services.openai_service import OpenAIService
from telegram_rag_memory_bot.services.rag_service import RagService
from telegram_rag_memory_bot.utils.process_lock import ProcessLock

LOGGER = logging.getLogger(__name__)


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def _run() -> None:
    settings = get_settings()
    process_lock = ProcessLock(settings.database_path.parent / "telegram_rag_memory_bot.lock", label="telegram_rag_memory_bot")
    process_lock.acquire()
    database = Database(settings.database_path)
    media_service = MediaService(settings)
    openai_service = OpenAIService(settings)
    rag_service = RagService(
        settings=settings,
        database=database,
        media_service=media_service,
        openai_service=openai_service,
    )
    telegram_app_service = TelegramAssistantApplicationService(settings, rag_service)
    vk_app_service = VkAssistantApplicationService(settings, rag_service)
    telegram_bot = TelegramMenuBot(settings, telegram_app_service)
    vk_bot = VkMenuBot(settings, vk_app_service, telegram_client=telegram_bot.client) if settings.vk_enabled else None
    upload_server = None
    public_server = None

    try:
        if settings.local_upload_enabled:
            from telegram_rag_memory_bot.infrastructure.http.local_upload_server import LocalUploadServer

        platform_services = {"telegram": telegram_app_service}
        notification_gateways = {"telegram": telegram_bot}
        if vk_bot is not None:
            platform_services["vk"] = vk_app_service
            notification_gateways["vk"] = vk_bot

        if settings.local_upload_enabled:
            upload_server = LocalUploadServer(
                settings,
                platform_services,
                telegram_bot.storage_gateway,
                notification_gateways,
            )
        if settings.public_web_enabled:
            public_server = PublicWebServer(
                settings,
                platform_services,
                notification_gateways,
            )

        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(telegram_bot.run())
            if vk_bot is not None:
                task_group.create_task(vk_bot.run())
            else:
                LOGGER.info("VK bot is not started because API_VK is empty.")
            if upload_server is not None:
                task_group.create_task(upload_server.run())
            else:
                LOGGER.info("Local upload server is disabled because LOCAL_UPLOAD_ENABLED=false.")
            if public_server is not None:
                task_group.create_task(public_server.run())
            else:
                LOGGER.info("Public web server is disabled because PUBLIC_WEB_ENABLED=false.")
    finally:
        if public_server is not None:
            await public_server.close()
        if upload_server is not None:
            await upload_server.close()
        database.close()
        process_lock.release()


def main() -> None:
    configure_logging()
    try:
        asyncio.run(_run())
    except RuntimeError as exc:
        print(exc)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
