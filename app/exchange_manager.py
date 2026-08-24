"""Управление жизненным циклом CCXT-инстансов бирж (спот и бессрочные фьючерсы)."""
from __future__ import annotations

from typing import Dict, List, Tuple

import ccxt.async_support as ccxt

from .models import ExchangeInfo

# Курируемый список бирж с хорошей поддержкой публичных данных в ccxt.
# Реальный список фильтруется по тому, что доступно в установленной версии ccxt.
CURATED = [
    "binance",
    "bybit",
    "okx",
    "kucoin",
    "gate",
    "mexc",
    "bitget",
    "htx",
    "kraken",
    "coinbase",
    "bingx",
    "cryptocom",
    "bigone",
]

# Человекочитаемые названия для UI.
DISPLAY_NAMES = {
    "binance": "Binance",
    "bybit": "Bybit",
    "okx": "OKX",
    "kucoin": "KuCoin",
    "gate": "Gate.io",
    "mexc": "MEXC",
    "bitget": "Bitget",
    "htx": "HTX (Huobi)",
    "kraken": "Kraken",
    "coinbase": "Coinbase",
    "bingx": "BingX",
    "cryptocom": "Crypto.com",
    "bigone": "BigONE",
}


def supported_exchanges() -> List[ExchangeInfo]:
    """Пересекает курируемый список с тем, что реально есть в ccxt, и читает возможности."""
    infos: List[ExchangeInfo] = []
    for ex_id in CURATED:
        if ex_id not in ccxt.exchanges:
            continue
        try:
            probe = getattr(ccxt, ex_id)()
        except Exception:
            continue
        has = probe.has
        infos.append(
            ExchangeInfo(
                id=ex_id,
                name=DISPLAY_NAMES.get(ex_id, ex_id.capitalize()),
                has_spot=bool(has.get("fetchTickers") or has.get("fetchTicker")),
                # Бессрочные фьючерсы: ccxt объявляет типы рынков статически,
                # поэтому сеть здесь по-прежнему не нужна.
                has_perp=bool(has.get("swap")),
                has_funding=bool(
                    has.get("fetchFundingRates") or has.get("fetchFundingRate")
                ),
            )
        )
    return infos


class ExchangeManager:
    """Кэширует инстансы бирж по (id, market_type). Загружает рынки лениво, один раз."""

    def __init__(self) -> None:
        # ключ: (exchange_id, "spot" | "swap")
        self._instances: Dict[Tuple[str, str], ccxt.Exchange] = {}
        self._markets_loaded: Dict[Tuple[str, str], bool] = {}

    def _create(self, ex_id: str, market_type: str) -> ccxt.Exchange:
        klass = getattr(ccxt, ex_id)
        return klass(
            {
                "enableRateLimit": True,
                "timeout": 15000,
                "options": {"defaultType": market_type},
            }
        )

    async def get(self, ex_id: str, market_type: str = "spot") -> ccxt.Exchange:
        """Возвращает готовый инстанс с загруженными рынками."""
        key = (ex_id, market_type)
        if key not in self._instances:
            self._instances[key] = self._create(ex_id, market_type)
        inst = self._instances[key]
        if not self._markets_loaded.get(key):
            await inst.load_markets()
            self._markets_loaded[key] = True
        return inst

    async def close(self) -> None:
        """Закрывает все открытые сетевые сессии (важно для aiohttp)."""
        for inst in self._instances.values():
            try:
                await inst.close()
            except Exception:
                pass
        self._instances.clear()
        self._markets_loaded.clear()
