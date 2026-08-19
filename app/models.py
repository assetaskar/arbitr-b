"""Pydantic-модели: настройки сканера, результаты арбитража, метаданные бирж."""
from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Settings(BaseModel):
    """Пользовательские настройки сканера. Меняются с фронтенда, сохраняются в JSON."""

    exchanges: List[str] = Field(
        default_factory=lambda: ["binance", "bybit", "okx", "kucoin", "gate"],
        description="Список id бирж (ccxt), между которыми ищем расхождения.",
    )
    pair_mode: str = Field(
        default="all",
        description="'all' — все пары по котировкам минус исключения; 'include' — только symbols.",
    )
    symbols: List[str] = Field(
        default_factory=lambda: [
            "BTC/USDT",
            "ETH/USDT",
            "SOL/USDT",
            "XRP/USDT",
            "DOGE/USDT",
        ],
        description="Торговые пары (формат ccxt). Используются только при pair_mode='include'.",
    )
    quote_currencies: List[str] = Field(
        default_factory=lambda: ["USDT"],
        description="Котировки для режима 'all' (напр. USDT).",
    )
    exclude_symbols: List[str] = Field(
        default_factory=list,
        description="Исключения для режима 'all': полный символ (BTC/USDT) или база (BTC).",
    )
    min_volume_usd: float = Field(
        default=50000,
        ge=0,
        description="Мин. суточный объём пары (в котировке) для режима 'all'; 0 = без фильтра. Отсекает неликвид.",
    )
    top_n: int = Field(
        default=100,
        ge=1,
        le=1000,
        description="Сколько строк отдавать на фронт в каждой таблице.",
    )
    min_spread_pct: float = Field(
        default=0.5,
        ge=0,
        description="Минимальный спред в %, ниже которого расхождение не показываем.",
    )
    refresh_interval_sec: int = Field(
        default=20,
        ge=5,
        le=600,
        description="Период обновления данных в секундах.",
    )
    track_spot: bool = Field(default=True, description="Искать межбиржевой спот-арбитраж.")
    track_perp: bool = Field(
        default=True, description="Искать межбиржевой арбитраж по фьючерсам (перпам)."
    )
    track_basis: bool = Field(
        default=True, description="Искать базис спот↔фьючерс внутри одной биржи."
    )
    track_funding: bool = Field(
        default=True, description="Искать funding-арбитраж по бессрочным фьючерсам."
    )


class SpotOpportunity(BaseModel):
    """Одно межбиржевое расхождение по споту: купить дешевле — продать дороже."""

    symbol: str
    buy_exchange: str
    buy_price: float
    sell_exchange: str
    sell_price: float
    spread_pct: float


class PerpOpportunity(BaseModel):
    """Межбиржевое расхождение по фьючерсам (перпам): купить дешевле — продать дороже."""

    symbol: str
    buy_exchange: str
    buy_price: float
    sell_exchange: str
    sell_price: float
    spread_pct: float


class BasisOpportunity(BaseModel):
    """Базис спот↔фьючерс на одной бирже: разница между ценой перпа и спота."""

    symbol: str
    exchange: str
    spot_price: float
    perp_price: float
    basis_pct: float              # (perp - spot) / spot * 100; >0 — фьючерс дороже
    direction: str                # 'perp_premium' | 'perp_discount'


class FundingLeg(BaseModel):
    """Ставка финансирования по перпетуалу на конкретной бирже."""

    exchange: str
    funding_rate: float           # ставка за интервал (доля, напр. 0.0001 = 0.01%)
    funding_apr: float            # грубая годовая оценка в %
    mark_price: Optional[float] = None
    next_funding_time: Optional[int] = None  # ms epoch


class FundingOpportunity(BaseModel):
    """Расхождение ставок финансирования одной монеты между биржами."""

    symbol: str
    long_exchange: str            # где ставка ниже (лонг здесь) — платим меньше / получаем
    long_rate: float
    short_exchange: str           # где ставка выше (шорт здесь) — получаем больше
    short_rate: float
    spread_pct: float             # разница ставок за интервал, в %
    spread_apr: float             # разница в годовом выражении, в %
    legs: List[FundingLeg] = Field(default_factory=list)


class Snapshot(BaseModel):
    """Полный снимок состояния, который уходит на фронт (REST + WebSocket)."""

    updated_at: float             # unix seconds
    scanning: bool
    universe_size: int = 0        # сколько пар реально сканируется (после пересечения/лимита)
    data_age_sec: float = 0       # возраст самых старых данных в ответе (0 — только что с биржи)
    spot: List[SpotOpportunity] = Field(default_factory=list)
    perp: List[PerpOpportunity] = Field(default_factory=list)
    basis: List[BasisOpportunity] = Field(default_factory=list)
    funding: List[FundingOpportunity] = Field(default_factory=list)
    errors: Dict[str, str] = Field(default_factory=dict)


class ExchangeInfo(BaseModel):
    """Метаданные поддерживаемой биржи для селектора на фронте."""

    id: str
    name: str
    has_spot: bool
    has_funding: bool
