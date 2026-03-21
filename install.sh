#!/usr/bin/env bash
# Файл: install.sh
# Устанавливает проект на Linux-сервер, поднимает виртуальное окружение,
# готовит .env, создает systemd-сервис и запускает Telegram/VK-ботов,
# админку и пользовательский сайт как единое серверное приложение.

set -Eeuo pipefail

APP_NAME="${APP_NAME:-telegram-rag-memory-bot}"
APP_USER="${APP_USER:-ragbot}"
APP_GROUP="${APP_GROUP:-$APP_USER}"
APP_DIR="${APP_DIR:-/opt/$APP_NAME}"
APP_DATA_DIR="${APP_DATA_DIR:-/var/lib/$APP_NAME}"
APP_LOG_DIR="${APP_LOG_DIR:-/var/log/$APP_NAME}"
SERVICE_NAME="${SERVICE_NAME:-$APP_NAME}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
APP_BRANCH="${APP_BRANCH:-main}"
APP_REPO_URL="${APP_REPO_URL:-}"
SOURCE_DIR="${SOURCE_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
LOCAL_UPLOAD_PORT="${LOCAL_UPLOAD_PORT:-8787}"
PUBLIC_WEB_PORT="${PUBLIC_WEB_PORT:-8790}"
ENABLE_UFW_RULES="${ENABLE_UFW_RULES:-0}"

log() {
  printf '\n[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
  printf '\n[ERROR] %s\n' "$*" >&2
  exit 1
}

require_root() {
  if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    fail "Запустите install.sh от root: sudo bash install.sh"
  fi
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Не найдена команда: $1"
}

set_env_default() {
  local env_file="$1"
  local key="$2"
  local value="$3"
  if grep -qE "^${key}=" "$env_file"; then
    return 0
  fi
  printf '%s=%s\n' "$key" "$value" >>"$env_file"
}

get_env_value() {
  local env_file="$1"
  local key="$2"
  local line=""
  if [[ -f "$env_file" ]]; then
    line="$(grep -E "^${key}=" "$env_file" | tail -n 1 || true)"
  fi
  printf '%s' "${line#*=}"
}

normalize_prompt_value() {
  local key="$1"
  local value="$2"
  case "${key}:${value}" in
    OPENAI_API_KEY:sk-...|TELEGRAM_API_ID:123456|TELEGRAM_API_HASH:your_telegram_api_hash|STORAGE_CHAT_ID:-1001234567890|VK_GROUP_ID:0)
      printf ''
      ;;
    *)
      printf '%s' "$value"
      ;;
  esac
}

upsert_env_value() {
  local env_file="$1"
  local key="$2"
  local value="$3"
  "$PYTHON_BIN" - "$env_file" "$key" "$value" <<'PY'
from pathlib import Path
import sys

env_path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
prefix = f"{key}="

if env_path.exists():
    lines = env_path.read_text(encoding="utf-8").splitlines()
else:
    lines = []

updated = []
replaced = False
for line in lines:
    if line.startswith(prefix):
        updated.append(prefix + value)
        replaced = True
    else:
        updated.append(line)

if not replaced:
    updated.append(prefix + value)

env_path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY
}

prompt_env_value() {
  local env_file="$1"
  local key="$2"
  local label="$3"
  local required="${4:-0}"
  local secret="${5:-0}"
  local current=""
  local answer=""

  current="$(normalize_prompt_value "$key" "$(get_env_value "$env_file" "$key")")"

  while true; do
    if [[ "$secret" == "1" ]]; then
      if [[ -n "$current" ]]; then
        read -r -s -p "${label} [Enter = оставить текущее значение]: " answer
      else
        read -r -s -p "${label}: " answer
      fi
      printf '\n'
    else
      if [[ -n "$current" ]]; then
        read -r -p "${label} [${current}]: " answer
      else
        read -r -p "${label}: " answer
      fi
    fi

    if [[ -z "$answer" ]]; then
      answer="$current"
    fi

    if [[ "$required" == "1" && -z "$answer" ]]; then
      printf 'Поле обязательно. Попробуйте еще раз.\n'
      continue
    fi

    upsert_env_value "$env_file" "$key" "$answer"
    break
  done
}

install_packages() {
  require_cmd apt-get
  log "Устанавливаю системные зависимости"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y \
    bash \
    build-essential \
    curl \
    ffmpeg \
    git \
    python3 \
    python3-dev \
    python3-venv \
    rsync
}

ensure_user_and_dirs() {
  log "Подготавливаю пользователя и директории"
  if ! getent group "$APP_GROUP" >/dev/null 2>&1; then
    groupadd --system "$APP_GROUP"
  fi
  if ! id -u "$APP_USER" >/dev/null 2>&1; then
    useradd --system --gid "$APP_GROUP" --home "$APP_DIR" --create-home --shell /usr/sbin/nologin "$APP_USER"
  fi

  mkdir -p "$APP_DIR" "$APP_DATA_DIR/media_cache" "$APP_DATA_DIR/downloads/videos" "$APP_LOG_DIR"
  chown -R "$APP_USER:$APP_GROUP" "$APP_DIR" "$APP_DATA_DIR" "$APP_LOG_DIR"
}

sync_project() {
  log "Загружаю код проекта"
  if [[ -n "$APP_REPO_URL" ]]; then
    if [[ -d "$APP_DIR/.git" ]]; then
      git -C "$APP_DIR" fetch --all --prune
      git -C "$APP_DIR" checkout "$APP_BRANCH"
      git -C "$APP_DIR" pull --ff-only origin "$APP_BRANCH"
    else
      rm -rf "$APP_DIR"
      git clone --branch "$APP_BRANCH" "$APP_REPO_URL" "$APP_DIR"
    fi
  else
    require_cmd rsync
    rsync -a --delete \
      --exclude '.git' \
      --exclude '.venv' \
      --exclude '__pycache__' \
      --exclude '*.pyc' \
      --exclude '.mypy_cache' \
      --exclude '.pytest_cache' \
      --exclude '.ruff_cache' \
      "$SOURCE_DIR"/ "$APP_DIR"/
  fi
  chown -R "$APP_USER:$APP_GROUP" "$APP_DIR"
}

setup_virtualenv() {
  log "Создаю виртуальное окружение и ставлю Python-зависимости"
  runuser -u "$APP_USER" -- "$PYTHON_BIN" -m venv "$APP_DIR/.venv"
  runuser -u "$APP_USER" -- "$APP_DIR/.venv/bin/pip" install --upgrade pip setuptools wheel
  runuser -u "$APP_USER" -- "$APP_DIR/.venv/bin/pip" install -e "$APP_DIR"
}

prepare_env_file() {
  log "Подготавливаю .env для сервера"
  local env_file="$APP_DIR/.env"
  if [[ ! -f "$env_file" ]]; then
    if [[ -f "$APP_DIR/.env.example" ]]; then
      cp "$APP_DIR/.env.example" "$env_file"
    else
      touch "$env_file"
    fi
  fi

  set_env_default "$env_file" "DATABASE_PATH" "$APP_DATA_DIR/rag_memory.db"
  set_env_default "$env_file" "MEDIA_CACHE_DIR" "$APP_DATA_DIR/media_cache"
  set_env_default "$env_file" "VIDEO_DOWNLOAD_DIR" "$APP_DATA_DIR/downloads/videos"
  set_env_default "$env_file" "HOMOSAP_VIDEO_PATH" "$APP_DATA_DIR/HOMOSAP.mp4"
  set_env_default "$env_file" "LOCAL_UPLOAD_ENABLED" "true"
  set_env_default "$env_file" "LOCAL_UPLOAD_HOST" "0.0.0.0"
  set_env_default "$env_file" "LOCAL_UPLOAD_PORT" "$LOCAL_UPLOAD_PORT"
  set_env_default "$env_file" "PUBLIC_WEB_ENABLED" "true"
  set_env_default "$env_file" "PUBLIC_WEB_HOST" "0.0.0.0"
  set_env_default "$env_file" "PUBLIC_WEB_PORT" "$PUBLIC_WEB_PORT"

  chown "$APP_USER:$APP_GROUP" "$env_file"
  chmod 640 "$env_file"

  log "Проверьте и заполните обязательные ключи в $env_file"
  printf '  - OPENAI_API_KEY\n'
  printf '  - TELEGRAM_BOT_TOKEN или SETTINGS_BOT_TOKEN\n'
  printf '  - STORAGE_CHAT_ID\n'
  printf '  - API_VK и VK_GROUP_ID, если нужен VK\n'
}

interactive_env_wizard() {
  local env_file="$APP_DIR/.env"

  if [[ ! -t 0 || ! -t 1 ]]; then
    log "Интерактивная настройка пропущена: нет терминала. При необходимости отредактируйте $env_file вручную."
    return 0
  fi

  log "Заполняю основные переменные окружения. Нажимайте Enter, чтобы оставить текущее значение."

  prompt_env_value "$env_file" "OPENAI_API_KEY" "OPENAI API key" 1 1
  prompt_env_value "$env_file" "TELEGRAM_BOT_TOKEN" "Telegram bot token" 1 1
  prompt_env_value "$env_file" "STORAGE_CHAT_ID" "ID Telegram-группы хранения" 1 0
  prompt_env_value "$env_file" "UPLOADER_USER_IDS" "Telegram admin user id или список через запятую" 0 0
  prompt_env_value "$env_file" "AUTHORIZED_USER_IDS" "Разрешенные Telegram user id через запятую (можно пусто)" 0 0
  prompt_env_value "$env_file" "LOCAL_UPLOAD_PASSWORD" "Пароль админки" 1 1

  prompt_env_value "$env_file" "API_VK" "VK API token (можно оставить пустым)" 0 1
  if [[ -n "$(normalize_prompt_value "API_VK" "$(get_env_value "$env_file" "API_VK")")" ]]; then
    prompt_env_value "$env_file" "VK_GROUP_ID" "VK group id" 1 0
    prompt_env_value "$env_file" "VK_UPLOADER_USER_IDS" "VK admin user id или список через запятую" 0 0
    prompt_env_value "$env_file" "VK_AUTHORIZED_USER_IDS" "Разрешенные VK user id через запятую (можно пусто)" 0 0
  fi

  chown "$APP_USER:$APP_GROUP" "$env_file"
  chmod 640 "$env_file"
}

write_systemd_service() {
  log "Создаю systemd-сервис"
  cat >"/etc/systemd/system/${SERVICE_NAME}.service" <<EOF
[Unit]
Description=Telegram + VK RAG Memory Bot
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${APP_USER}
Group=${APP_GROUP}
WorkingDirectory=${APP_DIR}
Environment=PYTHONUNBUFFERED=1
ExecStart=${APP_DIR}/.venv/bin/python -m telegram_rag_memory_bot
Restart=always
RestartSec=5
TimeoutStopSec=30
LimitNOFILE=65535
StandardOutput=append:${APP_LOG_DIR}/app.log
StandardError=append:${APP_LOG_DIR}/app.log

[Install]
WantedBy=multi-user.target
EOF

  systemctl daemon-reload
  systemctl enable --now "$SERVICE_NAME"
}

configure_firewall() {
  if [[ "$ENABLE_UFW_RULES" != "1" ]]; then
    return 0
  fi
  if ! command -v ufw >/dev/null 2>&1; then
    log "UFW не найден, пропускаю открытие портов"
    return 0
  fi
  log "Открываю порты ${LOCAL_UPLOAD_PORT} и ${PUBLIC_WEB_PORT} в UFW"
  ufw allow "${LOCAL_UPLOAD_PORT}/tcp" || true
  ufw allow "${PUBLIC_WEB_PORT}/tcp" || true
}

print_summary() {
  log "Готово"
  printf 'Сервис: %s\n' "$SERVICE_NAME"
  printf 'Код: %s\n' "$APP_DIR"
  printf 'Данные: %s\n' "$APP_DATA_DIR"
  printf 'Логи: %s/app.log\n' "$APP_LOG_DIR"
  printf 'Админка: http://SERVER_IP:%s/\n' "$LOCAL_UPLOAD_PORT"
  printf 'Сайт: http://SERVER_IP:%s/\n' "$PUBLIC_WEB_PORT"
  printf '\nПолезные команды:\n'
  printf '  systemctl status %s --no-pager\n' "$SERVICE_NAME"
  printf '  journalctl -u %s -f\n' "$SERVICE_NAME"
  printf '  tail -f %s/app.log\n' "$APP_LOG_DIR"
}

main() {
  require_root
  install_packages
  ensure_user_and_dirs
  sync_project
  setup_virtualenv
  prepare_env_file
  interactive_env_wizard
  write_systemd_service
  configure_firewall
  print_summary
}

main "$@"
