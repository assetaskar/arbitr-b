"""FastAPI-приложение: расчёт под каждого клиента поверх общего кэша бирж.

Модель работы: настройки принадлежат клиенту и приходят по WebSocket; бэкенд
кэширует сырые данные бирж и считает выборку под конкретного клиента. Фонового
периодического сканирования нет — сеть трогается только по запросу и не чаще TTL.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import ValidationError

from . import arbitrage
from .cache import MarketCache
from .config import default_settings
from .exchange_manager import ExchangeManager, supported_exchanges
from .models import ExchangeInfo, Settings, Snapshot
from .storage import Storage

# Клиент не может просить чаще — всё равно вернётся кэш, но сокет не будет молотить.
MIN_CLIENT_INTERVAL_SEC = 5


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage = Storage()
    await storage.init()
    manager = ExchangeManager()
    cache = MarketCache(manager, storage=storage)

    # Тёплый старт: поднимаем в память то, что успели сохранить до рестарта.
    for kind, ex_id, market_type, data, fetched_at in await storage.load_raw():
        try:
            cache.prime(kind, ex_id, market_type, data, fetched_at)
        except Exception:
            pass

    # Рынки грузятся долго — прогреваем их в фоне, чтобы первый запрос не ждал.
    prewarm = asyncio.create_task(cache.prewarm(default_settings().exchanges))

    app.state.manager = manager
    app.state.cache = cache
    app.state.storage = storage
    try:
        yield
    finally:
        prewarm.cancel()
        await storage.close()
        await manager.close()


app = FastAPI(title="Crypto Arbitrage Scanner", version="2.0.0", lifespan=lifespan)

# В деве фронт крутится на другом порту (Vite) — разрешаем CORS.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def run_scan(settings: Settings) -> Snapshot:
    """Считает снимок под настройки клиента и пишет заметные находки в историю."""
    snapshot = await arbitrage.scan(app.state.cache, settings)
    await app.state.storage.record_snapshot(snapshot)
    return snapshot


@app.get("/api/exchanges", response_model=List[ExchangeInfo])
async def list_exchanges() -> List[ExchangeInfo]:
    """Список поддерживаемых бирж для селектора на фронте."""
    return supported_exchanges()


@app.get("/api/settings", response_model=Settings)
async def get_default_settings() -> Settings:
    """Настройки по умолчанию — фронт берёт их при первом запуске."""
    return default_settings()


@app.post("/api/scan", response_model=Snapshot)
async def scan_once(settings: Settings) -> Snapshot:
    """Разовый расчёт под переданные настройки (без WebSocket)."""
    return await run_scan(settings)


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok"}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket) -> None:
    """Сессия клиента: он присылает свои настройки, получает свои снимки.

    Настройки одного клиента не влияют на других — общий у них только кэш бирж.
    """
    await ws.accept()
    state = {"settings": default_settings()}
    updated = asyncio.Event()

    # Ждём первые настройки клиента, чтобы не тратить первый расчёт на чужие дефолты.
    try:
        state["settings"] = Settings(**await asyncio.wait_for(ws.receive_json(), timeout=3))
    except (asyncio.TimeoutError, ValidationError, TypeError, KeyError):
        pass

    async def receive_settings() -> None:
        """Читает настройки из сокета и будит цикл расчёта."""
        while True:
            raw = await ws.receive_json()
            try:
                state["settings"] = Settings(**raw)
                updated.set()
            except (ValidationError, TypeError):
                await ws.send_json({"error": "Некорректные настройки"})

    reader = asyncio.create_task(receive_settings())
    try:
        while True:
            # Сбрасываем ДО расчёта: настройки, пришедшие во время него, не потеряются.
            updated.clear()
            snapshot = await run_scan(state["settings"])
            await ws.send_json(snapshot.model_dump())

            # Ждём интервал клиента, но просыпаемся сразу при смене настроек.
            interval = max(MIN_CLIENT_INTERVAL_SEC, state["settings"].refresh_interval_sec)
            try:
                await asyncio.wait_for(updated.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
    except (WebSocketDisconnect, RuntimeError):
        pass
    finally:
        reader.cancel()
