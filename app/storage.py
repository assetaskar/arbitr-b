"""SQLite-хранилище: тёплый старт кэша и история найденных расхождений.

Используется stdlib sqlite3 в отдельном потоке (asyncio.to_thread) — новых
зависимостей не требуется, а записи короткие и редкие.
"""
from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import time
from typing import List, Tuple

DB_PATH = os.environ.get("ARBITR_DB", "arbitr.db")

# История пишется не чаще этого интервала и только для заметных расхождений —
# иначе таблица растёт на сотни тысяч строк в сутки без всякой пользы.
HISTORY_MIN_INTERVAL_SEC = 60
HISTORY_MIN_SPREAD_PCT = 0.5
HISTORY_MIN_FUNDING_APR = 20.0
HISTORY_KEEP_DAYS = 30

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_cache (
    kind        TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    market_type TEXT NOT NULL,
    payload     TEXT NOT NULL,
    fetched_at  REAL NOT NULL,
    PRIMARY KEY (kind, exchange, market_type)
);

CREATE TABLE IF NOT EXISTS opportunity_history (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    kind        TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    exchange_a  TEXT,
    exchange_b  TEXT,
    spread_pct  REAL NOT NULL,
    payload     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_ts ON opportunity_history (ts);
CREATE INDEX IF NOT EXISTS idx_history_symbol ON opportunity_history (kind, symbol, ts);
"""


class Storage:
    def __init__(self, path: str = DB_PATH):
        self.path = path
        self._conn: sqlite3.Connection | None = None
        self._last_history_write = 0.0

    async def init(self) -> None:
        await asyncio.to_thread(self._init_sync)

    def _init_sync(self) -> None:
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.executescript(SCHEMA)
        cutoff = time.time() - HISTORY_KEEP_DAYS * 86400
        self._conn.execute("DELETE FROM opportunity_history WHERE ts < ?", (cutoff,))
        self._conn.commit()

    async def close(self) -> None:
        if self._conn:
            await asyncio.to_thread(self._conn.close)
            self._conn = None

    async def save_raw(
        self, kind: str, exchange: str, market_type: str, data: dict, fetched_at: float
    ) -> None:
        payload = json.dumps(data, separators=(",", ":"))
        await asyncio.to_thread(self._save_raw_sync, kind, exchange, market_type, payload, fetched_at)

    def _save_raw_sync(self, kind, exchange, market_type, payload, fetched_at) -> None:
        if not self._conn:
            return
        self._conn.execute(
            "INSERT INTO raw_cache (kind, exchange, market_type, payload, fetched_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(kind, exchange, market_type) DO UPDATE SET "
            "payload = excluded.payload, fetched_at = excluded.fetched_at",
            (kind, exchange, market_type, payload, fetched_at),
        )
        self._conn.commit()

    async def load_raw(self) -> List[Tuple[str, str, str, dict, float]]:
        """(kind, exchange, market_type, data, fetched_at) — для прогрева кэша."""
        rows = await asyncio.to_thread(self._load_raw_sync)
        out = []
        for kind, exchange, market_type, payload, fetched_at in rows:
            try:
                out.append((kind, exchange, market_type, json.loads(payload), fetched_at))
            except json.JSONDecodeError:
                continue
        return out

    def _load_raw_sync(self):
        if not self._conn:
            return []
        cur = self._conn.execute(
            "SELECT kind, exchange, market_type, payload, fetched_at FROM raw_cache"
        )
        return cur.fetchall()

    async def record_snapshot(self, snapshot) -> None:
        """Записывает заметные расхождения из снимка (с троттлингом)."""
        now = time.time()
        if now - self._last_history_write < HISTORY_MIN_INTERVAL_SEC:
            return
        self._last_history_write = now

        rows = []
        for o in snapshot.spot:
            if o.spread_pct >= HISTORY_MIN_SPREAD_PCT:
                rows.append(("spot", o.symbol, o.buy_exchange, o.sell_exchange, o.spread_pct, o))
        for o in snapshot.perp:
            if o.spread_pct >= HISTORY_MIN_SPREAD_PCT:
                rows.append(("perp", o.symbol, o.buy_exchange, o.sell_exchange, o.spread_pct, o))
        for o in snapshot.basis:
            if abs(o.basis_pct) >= HISTORY_MIN_SPREAD_PCT:
                rows.append(("basis", o.symbol, o.exchange, None, o.basis_pct, o))
        for o in snapshot.funding:
            if o.spread_apr >= HISTORY_MIN_FUNDING_APR:
                rows.append(
                    ("funding", o.symbol, o.long_exchange, o.short_exchange, o.spread_apr, o)
                )
        if not rows:
            return

        prepared = [
            (now, kind, symbol, ex_a, ex_b, spread, json.dumps(obj.model_dump(), default=str))
            for kind, symbol, ex_a, ex_b, spread, obj in rows
        ]
        await asyncio.to_thread(self._insert_history_sync, prepared)

    def _insert_history_sync(self, rows) -> None:
        if not self._conn:
            return
        self._conn.executemany(
            "INSERT INTO opportunity_history "
            "(ts, kind, symbol, exchange_a, exchange_b, spread_pct, payload) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        self._conn.commit()
