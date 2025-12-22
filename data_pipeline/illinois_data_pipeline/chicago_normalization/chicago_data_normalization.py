# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import math
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_sqlite import (
    DB_PATH,
    ensure_schema,
    get_conn,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

LOOKBACK_DAYS_BASELINE = 365
LOOKBACK_DAYS_30 = 30
LOOKBACK_DAYS_7 = 7

#  tune later
W_30D = 0.55
W_7D = 0.30
W_365D = 0.15

# clamp outputs
MIN_SCORE = 0
MAX_SCORE = 100


@dataclass
class GridCounts:
    c7: int
    c30: int
    c365: int
    sev30_sum: float
    sev30_n: int


def _iso_utc(dt_obj: datetime) -> str:
    return dt_obj.astimezone(timezone.utc).replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _parse_dt(s: str) -> datetime | None:
    if not s:
        return None
    try:
        # handles "2025-12-17T00:56:14" and "2025-12-17T00:56:14Z"
        if s.endswith("Z"):
            return datetime.fromisoformat(s[:-1]).replace(tzinfo=timezone.utc)
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _percentile(sorted_vals: List[int], p: float) -> int:
    """Nearest-rank percentile, p in [0,1]."""
    if not sorted_vals:
        return 0
    k = max(0, min(len(sorted_vals) - 1, int(math.ceil(p * len(sorted_vals))) - 1))
    return sorted_vals[k]


def _log_norm(x: float, p95: float) -> float:
    """Log normalize to [0,1] using p95 anchor."""
    if p95 <= 0:
        return 0.0
    return min(1.0, math.log1p(max(0.0, x)) / math.log1p(p95))


def _ensure_risk_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS risk_grid (
            grid_id TEXT PRIMARY KEY,
            count_7d INTEGER,
            count_30d INTEGER,
            count_365d INTEGER,
            severity_avg_30d REAL,
            score_0_100 INTEGER,
            updated_at TEXT
        )
        """
    )
    conn.commit()


def _load_counts(conn: sqlite3.Connection) -> Dict[str, GridCounts]:
    """
    Pull incidents from last 365d and aggregate in Python.
    Assumes violent_crimes table has:
      - datetime (ISO string)
      - grid_id
      - severity (REAL, nullable)
    """
    now = datetime.now(timezone.utc)
    cutoff_365 = now - timedelta(days=LOOKBACK_DAYS_BASELINE)
    cutoff_30 = now - timedelta(days=LOOKBACK_DAYS_30)
    cutoff_7 = now - timedelta(days=LOOKBACK_DAYS_7)

    
    cur = conn.execute(
        """
        SELECT datetime, grid_id, severity
        FROM violent_crimes
        WHERE datetime IS NOT NULL
          AND grid_id IS NOT NULL
        """
    )

    agg: Dict[str, GridCounts] = {}

    for dt_s, grid_id, severity in cur:
        dt_obj = _parse_dt(dt_s)
        if not dt_obj:
            continue
        if dt_obj < cutoff_365:
            continue  # we only need last 365d for baseline

        gc = agg.get(grid_id)
        if gc is None:
            gc = GridCounts(c7=0, c30=0, c365=0, sev30_sum=0.0, sev30_n=0)
            agg[grid_id] = gc

        # baseline
        gc.c365 += 1

        # 30d + severity
        if dt_obj >= cutoff_30:
            gc.c30 += 1
            if severity is not None:
                try:
                    gc.sev30_sum += float(severity)
                    gc.sev30_n += 1
                except Exception:
                    pass

        # 7d
        if dt_obj >= cutoff_7:
            gc.c7 += 1

    return agg


def _compute_scores(agg: Dict[str, GridCounts]) -> List[Tuple[str, int, int, int, float, int]]:
    """
    Returns rows: (grid_id, c7, c30, c365, sev_avg_30, score)
    """
    c7_vals = sorted([v.c7 for v in agg.values()])
    c30_vals = sorted([v.c30 for v in agg.values()])
    c365_vals = sorted([v.c365 for v in agg.values()])

    p95_7 = _percentile(c7_vals, 0.95)
    p95_30 = _percentile(c30_vals, 0.95)
    p95_365 = _percentile(c365_vals, 0.95)

    log.info("p95 anchors: 7d=%d 30d=%d 365d=%d (cells=%d)", p95_7, p95_30, p95_365, len(agg))

    out: List[Tuple[str, int, int, int, float, int]] = []
    for grid_id, v in agg.items():
        sev_avg_30 = (v.sev30_sum / v.sev30_n) if v.sev30_n else 0.0

        n7 = _log_norm(v.c7, p95_7)
        n30 = _log_norm(v.c30, p95_30)
        n365 = _log_norm(v.c365, p95_365)

        risk = (W_30D * n30) + (W_7D * n7) + (W_365D * n365)

        score = int(round(100 * max(0.0, min(1.0, risk))))
        score = max(MIN_SCORE, min(MAX_SCORE, score))

        out.append((grid_id, v.c7, v.c30, v.c365, float(sev_avg_30), score))

    return out


def _upsert_risk_grid(conn: sqlite3.Connection, rows: List[Tuple[str, int, int, int, float, int]]) -> None:
    updated_at = _iso_utc(datetime.now(timezone.utc))
    conn.executemany(
        """
        INSERT INTO risk_grid (
            grid_id, count_7d, count_30d, count_365d, severity_avg_30d, score_0_100, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(grid_id) DO UPDATE SET
            count_7d=excluded.count_7d,
            count_30d=excluded.count_30d,
            count_365d=excluded.count_365d,
            severity_avg_30d=excluded.severity_avg_30d,
            score_0_100=excluded.score_0_100,
            updated_at=excluded.updated_at
        """,
        [(gid, c7, c30, c365, sev, score, updated_at) for (gid, c7, c30, c365, sev, score) in rows],
    )
    conn.commit()


def build_risk_grid(db_path=DB_PATH) -> None:
    log.info("Building risk grid using DB=%s", DB_PATH)
    conn = get_conn(timeout=60)
    try:
        ensure_schema(conn)
        _ensure_risk_table(conn)

        agg = _load_counts(conn)
        if not agg:
            log.warning("No rows found to aggregate. Is your DB populated?")
            return

        rows = _compute_scores(agg)
        _upsert_risk_grid(conn, rows)

        # quick sanity print
        top = sorted(rows, key=lambda r: r[-1], reverse=True)[:10]
        log.info("Top 10 risk cells (grid_id, 7d, 30d, 365d, sev_avg_30, score):")
        for r in top:
            log.info("%s", r)

        total_rows = conn.execute("SELECT COUNT(*) FROM risk_grid").fetchone()[0]
        log.info("risk_grid updated (%d cells) -> %s (rows now %d)", len(rows), DB_PATH, total_rows)
    finally:
        conn.close()


if __name__ == "__main__":
    log.info("Building risk grid from DB=%s", DB_PATH)
    build_risk_grid()
