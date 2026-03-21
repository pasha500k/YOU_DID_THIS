"""
????: application/vk/service.py
?????????? VK-????????????? ??????????? ??????? ? ?????????
VK-????????????? ?? ?????????? namespace ????? ????.
"""

from __future__ import annotations

from telegram_rag_memory_bot.application.platform_service import PlatformAssistantService


class VkAssistantApplicationService(PlatformAssistantService):
    platform_code = "vk"
    admin_path_segment = "vk"
    VK_NAMESPACE_OFFSET = 1_500_000_000

    def external_authorized_user_ids(self) -> set[int]:
        return self.settings.vk_authorized_user_ids

    def external_admin_user_ids(self) -> set[int]:
        return self.settings.vk_uploader_user_ids

    def to_internal_user_id(self, user_id: int) -> int:
        return -(self.VK_NAMESPACE_OFFSET + int(user_id))

    def to_external_user_id(self, user_id: int) -> int:
        raw = int(user_id)
        if raw >= 0:
            return raw
        return abs(raw) - self.VK_NAMESPACE_OFFSET

    def to_internal_chat_id(self, chat_id: int) -> int:
        return self.to_internal_user_id(chat_id)

    def _match_internal_user_id(self, internal_user_id: int) -> bool:
        return int(internal_user_id) < 0
