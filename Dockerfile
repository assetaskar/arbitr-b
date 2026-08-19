# Бэкенд: FastAPI + ccxt на uvicorn.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ARBITR_SETTINGS=/data/settings.json \
    ARBITR_DB=/data/arbitr.db

WORKDIR /app

# Сначала зависимости — так слой кэшируется, пока requirements.txt не менялся.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# Настройки сканера храним в volume, чтобы переживали пересоздание контейнера.
VOLUME ["/data"]
EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
