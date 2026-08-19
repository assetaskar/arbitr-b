"""Серверные настройки по умолчанию.

Настройки принадлежат клиенту (хранятся в браузере и приходят в запросе), а здесь
лежат только дефолты для первого запуска. Их можно переопределить файлом
settings.json — удобно для деплоя.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from .models import Settings

SETTINGS_PATH = Path(os.environ.get("ARBITR_SETTINGS", "settings.json"))


def default_settings() -> Settings:
    """Дефолты для новых клиентов; settings.json (если есть) их переопределяет."""
    if SETTINGS_PATH.exists():
        try:
            return Settings(**json.loads(SETTINGS_PATH.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, ValueError, OSError):
            pass  # битый файл не должен ронять приложение
    return Settings()
