"""Расчёт арбитражных расхождений поверх кэша сырых данных.

Сети здесь нет — данные берутся из MarketCache (cache.py). Фильтры клиента
(объём, порог спреда, top_n) применяются на этом этапе, поэтому один и тот же
кэш обслуживает клиентов с разными настройками.
"""
from __future__ import annotations

import asyncio
import collections
import time
from typing import Dict, List, Optional, Set, Tuple

from .cache import MarketCache, Tickers, error_label
from .models import (
    BasisOpportunity,
    FundingLeg,
    FundingOpportunity,
    PerpOpportunity,
    Settings,
    Snapshot,
    SpotOpportunity,
)

# Спред выше этого по споту — почти наверняка ошибка данных / коллизия тикеров
# (один тикер = разные монеты на разных биржах), а не реальный арбитраж.
SANITY_MAX_SPOT_SPREAD_PCT = 60

# {exchange: {symbol: (bid, ask)}}
Prices = Dict[str, Dict[str, Tuple[float, float]]]


def _short_err(exc: Exception) -> str:
    msg = str(exc).strip() or exc.__class__.__name__
    return msg[:160]


def _split_excludes(exclude_symbols: List[str]) -> Tuple[Set[str], Set[str]]:
    """Разбивает исключения на полные символы (BTC/USDT) и базы (BTC)."""
    syms: Set[str] = set()
    bases: Set[str] = set()
    for e in exclude_symbols:
        e = (e or "").strip().upper()
        if not e:
            continue
        (syms if "/" in e else bases).add(e)
    return syms, bases


def build_universe(
    tickers_by_exchange: Dict[str, Tickers],
    quote_currencies: List[str],
    exclude_symbols: List[str],
    min_volume_usd: float,
) -> List[str]:
    """Пары нужных котировок, ликвидные минимум на 2 биржах, минус исключения.

    Строится из уже полученных тикеров (они тянутся bulk'ом целиком и содержат
    объём), поэтому фильтр по объёму участвует в отборе, а не применяется после.
    Ни сортировки, ни обрезки: показываем всё, что подходит.
    """
    quotes = {q.strip().upper() for q in quote_currencies if q.strip()}
    excl_syms, excl_bases = _split_excludes(exclude_symbols)
    counts: collections.Counter = collections.Counter()

    for tickers in tickers_by_exchange.values():
        for sym, t in tickers.items():
            base, _, quote = sym.partition("/")
            if quote.upper() not in quotes or not base:
                continue
            if sym.upper() in excl_syms or base.upper() in excl_bases:
                continue
            if min_volume_usd > 0:
                vol = t.get("volume")
                if vol is not None and vol < min_volume_usd:
                    continue
            counts[sym] += 1

    # Арбитраж возможен только там, где пара есть минимум на двух биржах.
    return [s for s, c in counts.items() if c >= 2]


def order_by_liquidity(
    universe: List[str], tickers_by_exchange: Dict[str, Tickers]
) -> List[str]:
    """Сортирует по объёму «слабой ноги» — второму по величине среди бирж.

    Нужно там, где приходится ограничивать число запросов (funding на биржах без
    массового эндпоинта): под лимит должны попадать самые торгуемые пары.
    """

    def weak_leg_volume(sym: str) -> float:
        vols = sorted(
            (t[sym].get("volume") or 0.0)
            for t in tickers_by_exchange.values()
            if sym in t
        )
        return vols[-2] if len(vols) > 1 else 0.0

    return sorted(universe, key=weak_leg_volume, reverse=True)


def _prices_from_cache(
    tickers_by_exchange: Dict[str, Tickers],
    universe: Set[str],
    min_volume_usd: float,
) -> Prices:
    """Отбирает из сырых тикеров нужные пары и применяет фильтр по объёму."""
    out: Prices = {}
    for ex_id, tickers in tickers_by_exchange.items():
        picked: Dict[str, Tuple[float, float]] = {}
        for sym, t in tickers.items():
            if sym not in universe:
                continue
            if min_volume_usd > 0:
                vol = t.get("volume")
                if vol is not None and vol < min_volume_usd:
                    continue
            picked[sym] = (t["bid"], t["ask"])
        out[ex_id] = picked
    return out


def _cross_exchange_rows(
    prices: Prices, symbols: List[str], min_spread_pct: float, top_n: int
) -> List[dict]:
    """Общий расчёт межбиржевого расхождения: min ask (купить) vs max bid (продать)."""
    rows: List[dict] = []
    for sym in symbols:
        best_ask: Optional[Tuple[str, float]] = None  # (exchange, price) — где покупаем
        best_bid: Optional[Tuple[str, float]] = None  # (exchange, price) — где продаём
        for ex_id, per_symbol in prices.items():
            quote = per_symbol.get(sym)
            if not quote:
                continue
            bid, ask = quote
            if best_ask is None or ask < best_ask[1]:
                best_ask = (ex_id, ask)
            if best_bid is None or bid > best_bid[1]:
                best_bid = (ex_id, bid)
        if not best_ask or not best_bid:
            continue
        if best_ask[0] == best_bid[0]:
            continue  # цены с одной биржи — не арбитраж
        spread = (best_bid[1] - best_ask[1]) / best_ask[1] * 100
        if spread < min_spread_pct or spread > SANITY_MAX_SPOT_SPREAD_PCT:
            continue
        rows.append(
            dict(
                symbol=sym,
                buy_exchange=best_ask[0],
                buy_price=best_ask[1],
                sell_exchange=best_bid[0],
                sell_price=best_bid[1],
                spread_pct=round(spread, 4),
            )
        )
    rows.sort(key=lambda r: r["spread_pct"], reverse=True)
    return rows[:top_n]


def compute_spot_opportunities(prices, symbols, min_spread_pct, top_n) -> List[SpotOpportunity]:
    """Межбиржевой спот-арбитраж."""
    return [SpotOpportunity(**r) for r in _cross_exchange_rows(prices, symbols, min_spread_pct, top_n)]


def compute_perp_opportunities(prices, symbols, min_spread_pct, top_n) -> List[PerpOpportunity]:
    """Межбиржевой арбитраж по фьючерсам (перпам)."""
    return [PerpOpportunity(**r) for r in _cross_exchange_rows(prices, symbols, min_spread_pct, top_n)]


def compute_basis_opportunities(
    spot_prices: Prices,
    perp_prices: Prices,
    symbols: List[str],
    min_spread_pct: float,
    top_n: int,
) -> List[BasisOpportunity]:
    """Базис спот↔фьючерс внутри каждой биржи: (perp - spot) / spot."""
    out: List[BasisOpportunity] = []
    for ex_id in set(spot_prices) & set(perp_prices):
        sp = spot_prices[ex_id]
        pp = perp_prices[ex_id]
        for sym in symbols:
            sq = sp.get(sym)
            pq = pp.get(sym)
            if not sq or not pq:
                continue
            spot_mid = (sq[0] + sq[1]) / 2
            perp_mid = (pq[0] + pq[1]) / 2
            if spot_mid <= 0:
                continue
            basis = (perp_mid - spot_mid) / spot_mid * 100
            if abs(basis) < min_spread_pct or abs(basis) > SANITY_MAX_SPOT_SPREAD_PCT:
                continue
            out.append(
                BasisOpportunity(
                    symbol=sym,
                    exchange=ex_id,
                    spot_price=spot_mid,
                    perp_price=perp_mid,
                    basis_pct=round(basis, 4),
                    direction="perp_premium" if basis > 0 else "perp_discount",
                )
            )
    out.sort(key=lambda o: abs(o.basis_pct), reverse=True)
    return out[:top_n]


def compute_funding_opportunities(
    legs_by_exchange: Dict[str, Dict[str, FundingLeg]],
    symbols: List[str],
    top_n: int,
) -> List[FundingOpportunity]:
    """Для каждой пары считает разброс ставок финансирования между биржами."""
    out: List[FundingOpportunity] = []
    for sym in symbols:
        legs: List[FundingLeg] = []
        for per_symbol in legs_by_exchange.values():
            leg = per_symbol.get(sym)
            if leg:
                legs.append(leg)
        if len(legs) < 2:
            continue  # нужна минимум пара бирж, чтобы был спред
        legs.sort(key=lambda l: l.funding_rate)
        low = legs[0]   # ставка минимальна → выгодно быть в лонге
        high = legs[-1]  # ставка максимальна → выгодно быть в шорте
        out.append(
            FundingOpportunity(
                symbol=sym,
                long_exchange=low.exchange,
                long_rate=round(low.funding_rate * 100, 5),
                short_exchange=high.exchange,
                short_rate=round(high.funding_rate * 100, 5),
                spread_pct=round((high.funding_rate - low.funding_rate) * 100, 5),
                spread_apr=round(high.funding_apr - low.funding_apr, 3),
                legs=legs,
            )
        )
    out.sort(key=lambda o: o.spread_apr, reverse=True)
    return out[:top_n]


async def scan(cache: MarketCache, s: Settings) -> Snapshot:
    """Расчёт под конкретного клиента: данные из кэша, фильтры — из его настроек."""
    errors: Dict[str, str] = {}
    need_spot = s.track_spot or s.track_basis
    need_perp = s.track_perp or s.track_basis
    exchanges = s.exchanges
    min_volume = 0.0 if s.pair_mode == "include" else s.min_volume_usd
    oldest = time.time()

    # 1) Тикеры (bulk, из кэша) — по ним же строится вселенная пар.
    ticker_tasks = []
    if need_spot:
        ticker_tasks += [("spot", ex, cache.tickers(ex, "spot", errors)) for ex in exchanges]
    if need_perp or s.track_funding:
        ticker_tasks += [("perp", ex, cache.tickers(ex, "swap", errors)) for ex in exchanges]

    spot_raw: Dict[str, Tickers] = {}
    perp_raw: Dict[str, Tickers] = {}
    if ticker_tasks:
        for (kind, ex_id, _), (data, fetched_at) in zip(
            ticker_tasks, await asyncio.gather(*[t[2] for t in ticker_tasks])
        ):
            if not data:
                continue
            (spot_raw if kind == "spot" else perp_raw)[ex_id] = data
            oldest = min(oldest, fetched_at)

    # 2) Вселенная пар.
    if s.pair_mode == "include":
        spot_universe = list(dict.fromkeys(s.symbols))
        perp_universe = list(spot_universe)
    else:
        spot_universe = (
            build_universe(spot_raw, s.quote_currencies, s.exclude_symbols, min_volume)
            if need_spot
            else []
        )
        perp_universe = (
            build_universe(perp_raw, s.quote_currencies, s.exclude_symbols, min_volume)
            if (need_perp or s.track_funding)
            else []
        )

    spot_set, perp_set = set(spot_universe), set(perp_universe)
    union = list(spot_set | perp_set)

    # 3) Funding — отдельной фазой: биржам без массового эндпоинта нужен список пар,
    #    и под их лимит запросов должны попадать самые ликвидные.
    funding_legs: Dict[str, Dict[str, FundingLeg]] = {}
    if s.track_funding and perp_universe:
        ranked = order_by_liquidity(perp_universe, perp_raw)
        funding_tasks = [(ex, cache.funding(ex, ranked, errors)) for ex in exchanges]
        for (ex_id, _), (data, fetched_at) in zip(
            funding_tasks, await asyncio.gather(*[t[1] for t in funding_tasks])
        ):
            if not data:
                continue
            funding_legs[ex_id] = data
            oldest = min(oldest, fetched_at)

    # 4) Фильтрация под клиента и расчёт.
    spot_prices = _prices_from_cache(spot_raw, spot_set, min_volume) if need_spot else {}
    perp_prices = _prices_from_cache(perp_raw, perp_set, min_volume) if need_perp else {}

    # Ошибки фоновых обновлений кэша — иначе они остались бы невидимыми. Берём
    # только биржи этого клиента: чужие ошибки в его снимок попадать не должны.
    requested = set(exchanges)
    for err_key, msg in cache.errors.items():
        if err_key[0] in requested:
            errors.setdefault(error_label(err_key), msg)

    return Snapshot(
        updated_at=time.time(),
        scanning=False,
        universe_size=len(union),
        data_age_sec=round(max(0.0, time.time() - oldest), 1),
        spot=(
            compute_spot_opportunities(spot_prices, spot_universe, s.min_spread_pct, s.top_n)
            if s.track_spot
            else []
        ),
        perp=(
            compute_perp_opportunities(perp_prices, perp_universe, s.min_spread_pct, s.top_n)
            if s.track_perp
            else []
        ),
        basis=(
            compute_basis_opportunities(
                spot_prices, perp_prices, union, s.min_spread_pct, s.top_n
            )
            if s.track_basis
            else []
        ),
        funding=(
            compute_funding_opportunities(funding_legs, perp_universe, s.top_n)
            if s.track_funding
            else []
        ),
        errors=errors,
    )
