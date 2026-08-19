#!/usr/bin/env bash
# Запуск бэкенда в режиме разработки (с автоперезагрузкой).
set -e
cd "$(dirname "$0")"

if [ ! -d ".venv" ]; then
  echo "Создаю виртуальное окружение..."
  python3 -m venv .venv
fi

source .venv/bin/activate
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo "Бэкенд поднимается на http://localhost:8000  (docs: /docs)"
exec uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
