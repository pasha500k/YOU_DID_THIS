"""
Файл: config.py
Загружает настройки проекта из .env, задает значения по умолчанию
и готовит директории для базы, кэша и рабочих файлов.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = Path.home() / "PycharmProjects" / "data"
ENV_FILE = PROJECT_ROOT / ".env"
DEFAULT_PUBLIC_HOST = "letovoai.ru"
DEFAULT_PUBLIC_SITE_URL = "https://letovoai.ru"


def _parse_id_list(raw_value: str | None) -> set[int]:
    if not raw_value:
        return set()

    values: set[int] = set()
    for token in raw_value.replace(";", ",").split(","):
        token = token.strip()
        if not token:
            continue
        values.add(int(token))
    return values


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    openai_api_key: str = Field(alias="OPENAI_API_KEY")
    telegram_api_id: int = Field(default=0, alias="TELEGRAM_API_ID")
    telegram_api_hash: str = Field(default="", alias="TELEGRAM_API_HASH")

    telegram_bot_token: str = Field(default="", alias="TELEGRAM_BOT_TOKEN")
    vk_bot_enabled: bool = Field(default=True, alias="VK_BOT_ENABLED")
    vk_api_token: str = Field(default="", alias="API_VK")
    vk_group_id: int = Field(default=0, alias="VK_GROUP_ID")
    vk_api_version: str = Field(default="5.199", alias="VK_API_VERSION")
    bot_session_name: str = Field(default="rag_memory_bot", alias="BOT_SESSION_NAME")

    settings_bot_token: str = Field(default="", alias="SETTINGS_BOT_TOKEN")
    settings_bot_session_name: str = Field(default="rag_memory_settings_bot", alias="SETTINGS_BOT_SESSION_NAME")
    telegram_session_name: str = Field(default="rag_memory_userbot", alias="TELEGRAM_SESSION_NAME")

    storage_chat_id: int = Field(alias="STORAGE_CHAT_ID")
    video_download_chat_ids_raw: str = Field(default="-5109571428", alias="VIDEO_DOWNLOAD_CHAT_IDS")
    auto_ingest_chat_ids_raw: str = Field(default="-5109571428", alias="AUTO_INGEST_CHAT_IDS")

    public_access: bool = Field(default=True, alias="PUBLIC_ACCESS")
    daily_message_limit: int = Field(default=10, alias="DAILY_MESSAGE_LIMIT")
    bot_access_password: str = Field(default="", alias="BOT_ACCESS_PASSWORD")

    authorized_user_ids_raw: str = Field(default="", alias="AUTHORIZED_USER_IDS")
    uploader_user_ids_raw: str = Field(default="", alias="UPLOADER_USER_IDS")
    vk_authorized_user_ids_raw: str = Field(default="", alias="VK_AUTHORIZED_USER_IDS")
    vk_uploader_user_ids_raw: str = Field(default="", alias="VK_UPLOADER_USER_IDS")

    database_path: Path = Field(default=DEFAULT_DATA_ROOT / "rag_memory.db", alias="DATABASE_PATH")
    database_url: str = Field(default="", alias="DATABASE_URL")
    media_cache_dir: Path = Field(default=DEFAULT_DATA_ROOT / "media_cache", alias="MEDIA_CACHE_DIR")
    video_download_dir: Path = Field(default=DEFAULT_DATA_ROOT / "downloads" / "videos", alias="VIDEO_DOWNLOAD_DIR")
    homosap_video_path: Path = Field(default=DEFAULT_DATA_ROOT / "HOMOSAP.mp4", alias="HOMOSAP_VIDEO_PATH")

    local_upload_enabled: bool = Field(default=True, alias="LOCAL_UPLOAD_ENABLED")
    local_upload_host: str = Field(default="0.0.0.0", alias="LOCAL_UPLOAD_HOST")
    local_upload_port: int = Field(default=8787, alias="LOCAL_UPLOAD_PORT")
    local_upload_password: str = Field(default="Hehetoto123", alias="LOCAL_UPLOAD_PASSWORD")
    local_upload_token: str = Field(default="", alias="LOCAL_UPLOAD_TOKEN")
    local_upload_public_url: str = Field(
        default=f"http://{DEFAULT_PUBLIC_HOST}:8787",
        alias="LOCAL_UPLOAD_PUBLIC_URL",
    )
    public_web_enabled: bool = Field(default=True, alias="PUBLIC_WEB_ENABLED")
    public_web_host: str = Field(default="0.0.0.0", alias="PUBLIC_WEB_HOST")
    public_web_port: int = Field(default=8790, alias="PUBLIC_WEB_PORT")
    public_web_public_url: str = Field(
        default=DEFAULT_PUBLIC_SITE_URL,
        alias="PUBLIC_WEB_PUBLIC_URL",
    )
    public_web_password: str = Field(default="", alias="PUBLIC_WEB_PASSWORD")

    answer_model: str = Field(default="gpt-4o-mini", alias="ANSWER_MODEL")
    analysis_model: str = Field(default="gpt-4o", alias="ANALYSIS_MODEL")
    vision_model: str = Field(default="gpt-4o", alias="VISION_MODEL")
    transcription_model: str = Field(default="gpt-4o-mini-transcribe", alias="TRANSCRIPTION_MODEL")
    embedding_model: str = Field(default="text-embedding-3-small", alias="EMBEDDING_MODEL")
    openai_web_search_enabled: bool = Field(default=True, alias="OPENAI_WEB_SEARCH_ENABLED")
    openai_web_search_context_size: Literal["low", "medium", "high"] = Field(
        default="medium",
        alias="OPENAI_WEB_SEARCH_CONTEXT_SIZE",
    )
    openai_web_search_max_tool_calls: int = Field(default=3, alias="OPENAI_WEB_SEARCH_MAX_TOOL_CALLS")

    max_context_chunks: int = Field(default=8, alias="MAX_CONTEXT_CHUNKS")
    conversation_context_messages: int = Field(default=5, alias="CONTEXT_MESSAGES")
    query_top_k: int = Field(default=12, alias="QUERY_TOP_K")
    max_chunk_chars: int = Field(default=1200, alias="MAX_CHUNK_CHARS")
    chunk_overlap_chars: int = Field(default=200, alias="CHUNK_OVERLAP_CHARS")
    max_index_chars: int = Field(default=50000, alias="MAX_INDEX_CHARS")
    max_video_frames: int = Field(default=6, alias="MAX_VIDEO_FRAMES")
    audio_segment_seconds: int = Field(default=600, alias="AUDIO_SEGMENT_SECONDS")
    telegram_upload_part_size_kb: int = Field(default=512, alias="TELEGRAM_UPLOAD_PART_SIZE_KB")
    telegram_upload_pipeline_workers: int = Field(default=8, alias="TELEGRAM_UPLOAD_PIPELINE_WORKERS")
    telegram_bot_api_upload_limit_mb: int = Field(default=1024, alias="TELEGRAM_BOT_API_UPLOAD_LIMIT_MB")
    telegram_bot_api_connect_timeout_seconds: int = Field(default=180, alias="TELEGRAM_BOT_API_CONNECT_TIMEOUT_SECONDS")
    telegram_bot_api_read_timeout_seconds: int = Field(default=7200, alias="TELEGRAM_BOT_API_READ_TIMEOUT_SECONDS")
    telegram_bot_api_failure_cooldown_seconds: int = Field(default=1800, alias="TELEGRAM_BOT_API_FAILURE_COOLDOWN_SECONDS")

    ffmpeg_binary: str = Field(default="ffmpeg", alias="FFMPEG_BINARY")
    ffprobe_binary: str = Field(default="ffprobe", alias="FFPROBE_BINARY")

    @property
    def bot_token(self) -> str:
        return self.telegram_bot_token.strip() or self.settings_bot_token.strip()

    @property
    def bot_enabled(self) -> bool:
        return bool(self.bot_token)

    @property
    def authorized_user_ids(self) -> set[int]:
        return _parse_id_list(self.authorized_user_ids_raw)

    @property
    def uploader_user_ids(self) -> set[int]:
        parsed = _parse_id_list(self.uploader_user_ids_raw)
        return parsed or self.authorized_user_ids

    @property
    def vk_authorized_user_ids(self) -> set[int]:
        return _parse_id_list(self.vk_authorized_user_ids_raw)

    @property
    def vk_uploader_user_ids(self) -> set[int]:
        parsed = _parse_id_list(self.vk_uploader_user_ids_raw)
        return parsed or self.vk_authorized_user_ids

    @property
    def video_download_chat_ids(self) -> set[int]:
        return _parse_id_list(self.video_download_chat_ids_raw)

    @property
    def auto_ingest_chat_ids(self) -> set[int]:
        return _parse_id_list(self.auto_ingest_chat_ids_raw)

    @property
    def vk_enabled(self) -> bool:
        return bool(self.vk_api_token.strip())

    @property
    def local_upload_base_url(self) -> str:
        if self.local_upload_public_url.strip():
            return self.local_upload_public_url.strip().rstrip("/")
        return f"http://{self.local_upload_host}:{self.local_upload_port}"

    @property
    def public_web_base_url(self) -> str:
        if self.public_web_public_url.strip():
            return self.public_web_public_url.strip().rstrip("/")
        return f"http://{self.public_web_host}:{self.public_web_port}"

    def ensure_directories(self) -> None:
        self.database_path = self._resolve_project_path(self.database_path)
        self.media_cache_dir = self._resolve_project_path(self.media_cache_dir)
        self.video_download_dir = self._resolve_project_path(self.video_download_dir)
        self.homosap_video_path = self._resolve_project_path(self.homosap_video_path)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self.media_cache_dir.mkdir(parents=True, exist_ok=True)
        self.video_download_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _resolve_project_path(path: Path) -> Path:
        if path.is_absolute():
            return path
        return (PROJECT_ROOT / path).resolve()


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    try:
        settings = Settings()
    except ValidationError as exc:
        missing_fields = []
        for error in exc.errors():
            field = error.get("loc", [""])[0]
            if isinstance(field, str):
                missing_fields.append(field)
        missing_text = ", ".join(missing_fields) if missing_fields else str(exc)
        raise RuntimeError(f"Missing or invalid settings in {ENV_FILE}: {missing_text}") from exc

    if not settings.bot_enabled:
        raise RuntimeError(
            f"Missing or invalid settings in {ENV_FILE}: TELEGRAM_BOT_TOKEN or SETTINGS_BOT_TOKEN"
        )
    settings.ensure_directories()
    return settings
