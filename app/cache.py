"""Кэш сырых рыночных данных по биржам.

Ключевая идея: кэшируется СЫРЬЁ (тикеры, ставки funding), которое не зависит от
настроек клиентов. Фильтры и пороги применяются позже, на этапе расчёта. Благодаря
этому несколько клиентов с разными настройками используют один и тот же кэш, а
биржи опрашиваются не чаще одного раза в TTL.

Стратегия отдачи — stale-while-revalidate: если данные есть, но устарели, они
возвращаются немедленно, а обновление уходит в фон. Реальный возраст данных
сообщается наверх (data_age_sec), поэтому клиент всегда видит, насколько они свежи.
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Dict, List, Optional, Tuple

from .exchange_manager import ExchangeManager
from .models import FundingLeg

# Минимальный интервал похода на биржи. Запросы чаще обслуживаются из памяти.
TTL_SEC = float(os.environ.get("ARBITR_CACHE_TTL", "30"))

FUNDING_PERIODS_PER_YEAR = 3 * 365
FUNDING_PER_SYMBOL_CAP = 100   # макс. по-символьных запросов за раз (биржи без bulk)
FUNDING_CONCURRENCY = 8

# {symbol: {"bid": float, "ask": float, "volume": float | None}}
Tickers = Dict[str, Dict[str, Optional[float]]]

Key = Tuple[str, str]


def _short_err(exc: Exception) -> str:
    msg = str(exc).strip() or exc.__class__.__name__
    return msg[:160]


def error_label(err_key: Key) -> str:
    """(bybit, spot) -> 'bybit · spot' — как ошибка выглядит в интерфейсе."""
    ex_id, label = err_key
    return f"{ex_id} · {label}"


def _maybe_float(value) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _spot_symbol_of(market: dict) -> Optional[str]:
    """BASE/QUOTE из описания рынка — единый ключ для спота, перпов и funding."""
    base = (market.get("base") or "").upper()
    quote = (market.get("quote") or "").upper()
    return f"{base}/{quote}" if base and quote else None


class MarketCache:
    """Кэширует тикеры (spot/swap) и ставки funding по биржам с TTL."""

    def __init__(self, manager: ExchangeManager, storage=None, ttl_sec: float = TTL_SEC):
        self.manager = manager
        self.storage = storage
        self.ttl = ttl_sec
        self._tickers: Dict[Key, Tuple[Tickers, float]] = {}
        # ex_id -> {"rates": {sym: FundingLeg}, "fetched_at": ts, "sym_ts": {...}, "bulk": bool}
        self._funding: Dict[str, dict] = {}
        self._locks: Dict[Key, asyncio.Lock] = {}
        self._refreshing: Dict[Key, asyncio.Task] = {}
        # Ошибки фоновых обновлений по ключу (биржа, тип) — показываем их при
        # следующем запросе, но ТОЛЬКО тому клиенту, который выбрал эту биржу.
        self.errors: Dict[Key, str] = {}

    # ---------- служебное ----------

    def _lock(self, key: Key) -> asyncio.Lock:
        """Один поход на биржу на ключ: параллельные клиенты ждут первого."""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    def _fresh(self, ts: float) -> bool:
        return (time.time() - ts) < self.ttl

    def _refresh_in_background(self, key: Key, coro_factory) -> None:
        """Ставит обновление в фон, не плодя дубликатов по одному ключу."""
        task = self._refreshing.get(key)
        if task and not task.done():
            return
        self._refreshing[key] = asyncio.create_task(coro_factory())

    def prime(self, kind: str, ex_id: str, market_type: str, data: dict, fetched_at: float) -> None:
        """Заполнить кэш сохранёнными данными при старте (тёплый старт)."""
        if kind == "tickers":
            self._tickers[(ex_id, market_type)] = (data, fetched_at)
        elif kind == "funding":
            legs = {s: FundingLeg(**v) for s, v in data.items()}
            self._funding[ex_id] = {
                "rates": legs,
                "fetched_at": fetched_at,
                "sym_ts": {s: fetched_at for s in legs},
                "bulk": True,
            }

    # ---------- тикеры ----------

    async def tickers(
        self, ex_id: str, market_type: str, errors: Dict[str, str]
    ) -> Tuple[Tickers, float]:
        """Все тикеры биржи. Свежие — из памяти, устаревшие — сразу с фоновым обновлением."""
        key = (ex_id, market_type)
        cached = self._tickers.get(key)
        if cached and self._fresh(cached[1]):
            return cached
        if cached:
            self._refresh_in_background(key, lambda: self._fetch_tickers(ex_id, market_type))
            return cached
        # Данных нет вовсе — придётся подождать.
        return await self._fetch_tickers(ex_id, market_type, errors)

    async def _fetch_tickers(
        self, ex_id: str, market_type: str, errors: Optional[Dict[str, str]] = None
    ) -> Tuple[Tickers, float]:
        key = (ex_id, market_type)
        label = "spot" if market_type == "spot" else "perp"
        err_key = (ex_id, label)

        async with self._lock(key):
            cached = self._tickers.get(key)
            if cached and self._fresh(cached[1]):
                return cached

            def fail(exc: Exception):
                msg = _short_err(exc)
                self.errors[err_key] = msg
                if errors is not None:
                    errors[error_label(err_key)] = msg
                return (cached[0] if cached else {}), (cached[1] if cached else 0.0)

            try:
                inst = await self.manager.get(ex_id, market_type)
                raw = await inst.fetch_tickers()
            except Exception as exc:
                return fail(exc)

            out: Tickers = {}
            for sym, t in raw.items():
                market = inst.markets.get(sym)
                if not market or not market.get("active"):
                    continue
                if market_type == "spot":
                    if not market.get("spot"):
                        continue
                    norm = sym
                else:
                    if not (market.get("swap") and market.get("linear")):
                        continue
                    norm = _spot_symbol_of(market)
                if not norm:
                    continue
                bid = t.get("bid") or t.get("last") or t.get("close")
                ask = t.get("ask") or t.get("last") or t.get("close")
                if not bid or not ask or bid <= 0 or ask <= 0:
                    continue
                out[norm] = {
                    "bid": float(bid),
                    "ask": float(ask),
                    "volume": _maybe_float(t.get("quoteVolume")),
                }

            now = time.time()
            self._tickers[key] = (out, now)
            self.errors.pop(err_key, None)
            if self.storage:
                await self.storage.save_raw("tickers", ex_id, market_type, out, now)
            return out, now

    # ---------- funding ----------

    async def funding(
        self, ex_id: str, universe: List[str], errors: Dict[str, str]
    ) -> Tuple[Dict[str, FundingLeg], float]:
        """Ставки финансирования биржи. Bulk где возможно, иначе — по символам."""
        entry = self._funding.get(ex_id)
        if entry and self._fresh(entry["fetched_at"]):
            return entry["rates"], entry["fetched_at"]
        if entry and entry["rates"]:
            syms = list(universe)
            self._refresh_in_background(
                (ex_id, "funding"), lambda: self._fetch_funding(ex_id, syms)
            )
            return entry["rates"], entry["fetched_at"]
        return await self._fetch_funding(ex_id, universe, errors)

    async def _fetch_funding(
        self, ex_id: str, universe: List[str], errors: Optional[Dict[str, str]] = None
    ) -> Tuple[Dict[str, FundingLeg], float]:
        key = (ex_id, "funding")
        err_key = (ex_id, "funding")

        async with self._lock(key):
            entry = self._funding.setdefault(
                ex_id, {"rates": {}, "fetched_at": 0.0, "sym_ts": {}, "bulk": False}
            )
            if self._fresh(entry["fetched_at"]):
                return entry["rates"], entry["fetched_at"]

            def fail(exc: Exception):
                msg = _short_err(exc)
                self.errors[err_key] = msg
                if errors is not None:
                    errors[error_label(err_key)] = msg
                return entry["rates"], entry["fetched_at"]

            try:
                inst = await self.manager.get(ex_id, "swap")
            except Exception as exc:
                return fail(exc)

            has_bulk = bool(inst.has.get("fetchFundingRates"))
            has_single = bool(inst.has.get("fetchFundingRate"))
            if not has_bulk and not has_single:
                return entry["rates"], entry["fetched_at"]

            now = time.time()
            if has_bulk:
                try:
                    raw = await inst.fetch_funding_rates()
                except Exception as exc:
                    return fail(exc)
                rates: Dict[str, FundingLeg] = {}
                for perp_sym, r in raw.items():
                    market = inst.markets.get(perp_sym)
                    if not market:
                        continue
                    norm = _spot_symbol_of(market)
                    leg = self._leg(ex_id, r)
                    if norm and leg:
                        rates[norm] = leg
                entry.update(
                    rates=rates, fetched_at=now, bulk=True, sym_ts={s: now for s in rates}
                )
            else:
                # Без bulk: дотягиваем только те пары вселенной, что устарели.
                perp_of = {}
                for spot_sym in universe:
                    quote = spot_sym.split("/")[-1]
                    perp = f"{spot_sym}:{quote}"
                    if perp in inst.markets:
                        perp_of[spot_sym] = perp
                stale = [s for s in perp_of if not self._fresh(entry["sym_ts"].get(s, 0.0))]
                sem = asyncio.Semaphore(FUNDING_CONCURRENCY)

                async def one(spot_sym: str):
                    async with sem:
                        try:
                            return spot_sym, await inst.fetch_funding_rate(perp_of[spot_sym])
                        except Exception:
                            return spot_sym, None

                for spot_sym, r in await asyncio.gather(
                    *[one(s) for s in stale[:FUNDING_PER_SYMBOL_CAP]]
                ):
                    leg = self._leg(ex_id, r) if r else None
                    if leg:
                        entry["rates"][spot_sym] = leg
                        entry["sym_ts"][spot_sym] = now
                entry["fetched_at"] = now
                entry["bulk"] = False

            self.errors.pop(err_key, None)
            if self.storage:
                await self.storage.save_raw(
                    "funding",
                    ex_id,
                    "swap",
                    {s: leg.model_dump() for s, leg in entry["rates"].items()},
                    entry["fetched_at"],
                )
            return entry["rates"], entry["fetched_at"]

    def _leg(self, ex_id: str, r: dict) -> Optional[FundingLeg]:
        fr = r.get("fundingRate")
        if fr is None:
            return None
        fr = float(fr)
        return FundingLeg(
            exchange=ex_id,
            funding_rate=fr,
            funding_apr=fr * FUNDING_PERIODS_PER_YEAR * 100,
            mark_price=_maybe_float(r.get("markPrice")),
            next_funding_time=r.get("fundingTimestamp") or r.get("nextFundingTimestamp"),
        )

    async def prewarm(self, exchanges: List[str]) -> None:
        """Загружает описания рынков заранее, чтобы первый запрос не ждал их."""
        for ex_id in exchanges:
            for market_type in ("spot", "swap"):
                try:
                    await self.manager.get(ex_id, market_type)
                except Exception:
                    pass
