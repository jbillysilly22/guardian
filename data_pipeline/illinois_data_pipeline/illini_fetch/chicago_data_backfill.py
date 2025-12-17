# -*- coding: utf-8 -*-
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime as _dt
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

THIS_FILE = Path(__file__).resolve()
REPO_ROOT = THIS_FILE.parents[3]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_fetcher import (
    FetchConfig,
    _make_session,
)
from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_endpoint_catalog import (
    collect_datasets,
    ChicagoDataset,
)
from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_filter import (
    enrich_row,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CRIME_DATASET_ID = "ijzp-q8t2"




def _app_data_dir(app_name: str = "guardian") -> Path:
    override = os.environ.get("GUARDIAN_DATA_DIR")
    if override:
        return Path(override).expanduser().resolve()

    if sys.platform.startswith("win"):
        base = os.environ.get("LOCALAPPDATA") or os.environ.get("APPDATA")
        return (Path(base) / app_name) if base else (Path.home() / app_name)

    if sys.platform == "darwin":
        return Path.home() / "Library" / "Application Support" / app_name

    base = os.environ.get("XDG_DATA_HOME")
    return (Path(base) / app_name) if base else (Path.home() / ".local" / "share" / app_name)


DB_DIR = _app_data_dir("guardian")
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "violent_crimes.sqlite"




CREATE_VIOLENT_SQL = """
CREATE TABLE IF NOT EXISTS violent_crimes (
    case_number TEXT PRIMARY KEY,
    row_hash TEXT,
    datetime TEXT,
    primary_type TEXT,
    primary_type_norm TEXT,
    description TEXT,
    location_description TEXT,
    arrest INTEGER,
    domestic INTEGER,
    beat INTEGER,
    district INTEGER,
    ward INTEGER,
    community_area INTEGER,
    fbi_code TEXT,
    x_coordinate INTEGER,
    y_coordinate INTEGER,
    latitude REAL,
    longitude REAL,
    lat REAL,
    lon REAL,
    grid_id TEXT,
    severity REAL,
    is_violent INTEGER,
    domain TEXT,
    raw_json TEXT,
    last_seen_at TEXT
);
"""

CREATE_META_SQL = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

UPSERT_SQL = """
INSERT INTO violent_crimes (
    case_number, row_hash, datetime, primary_type, primary_type_norm, description,
    location_description, arrest, domestic, beat, district, ward, community_area,
    fbi_code, x_coordinate, y_coordinate, latitude, longitude, lat, lon, grid_id,
    severity, is_violent, domain, raw_json, last_seen_at
) VALUES (
    :case_number, :row_hash, :datetime, :primary_type, :primary_type_norm, :description,
    :location_description, :arrest, :domestic, :beat, :district, :ward, :community_area,
    :fbi_code, :x_coordinate, :y_coordinate, :latitude, :longitude, :lat, :lon, :grid_id,
    :severity, :is_violent, :domain, :raw_json, :last_seen_at
)
ON CONFLICT(case_number) DO UPDATE SET
    row_hash=excluded.row_hash,
    datetime=excluded.datetime,
    primary_type=excluded.primary_type,
    primary_type_norm=excluded.primary_type_norm,
    description=excluded.description,
    location_description=excluded.location_description,
    arrest=excluded.arrest,
    domestic=excluded.domestic,
    beat=excluded.beat,
    district=excluded.district,
    ward=excluded.ward,
    community_area=excluded.community_area,
    fbi_code=excluded.fbi_code,
    x_coordinate=excluded.x_coordinate,
    y_coordinate=excluded.y_coordinate,
    latitude=excluded.latitude,
    longitude=excluded.longitude,
    lat=excluded.lat,
    lon=excluded.lon,
    grid_id=excluded.grid_id,
    severity=excluded.severity,
    is_violent=excluded.is_violent,
    domain=excluded.domain,
    raw_json=excluded.raw_json,
    last_seen_at=excluded.last_seen_at;
"""


def _ensure_tables(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.executescript(CREATE_VIOLENT_SQL)
    conn.executescript(CREATE_META_SQL)
    conn.commit()


def _meta_get(conn: sqlite3.Connection, key: str) -> Optional[str]:
    cur = conn.execute("SELECT value FROM meta WHERE key=?", (key,))
    row = cur.fetchone()
    return row[0] if row else None


def _meta_set(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()


def _make_case_key(row: Dict[str, Any]) -> str:
    case = row.get("case_number") or row.get("case") or row.get("id")
    if case:
        return str(case)
    raw = json.dumps({k: row.get(k) for k in sorted(row.keys())}, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _vals_from_enriched(enriched: Dict[str, Any]) -> Dict[str, Any]:
    key = _make_case_key(enriched)
    row_hash = hashlib.sha1(json.dumps(enriched, sort_keys=True, default=str).encode()).hexdigest()
    now = _dt.utcnow().isoformat()

    return {
        "case_number": key,
        "row_hash": row_hash,
        "datetime": enriched.get("datetime"),
        "primary_type": enriched.get("primary_type") or enriched.get("offense") or enriched.get("category"),
        "primary_type_norm": enriched.get("primary_type_norm"),
        "description": enriched.get("description"),
        "location_description": enriched.get("location_description"),
        "arrest": int(bool(enriched.get("arrest"))) if enriched.get("arrest") is not None else None,
        "domestic": int(bool(enriched.get("domestic"))) if enriched.get("domestic") is not None else None,
        "beat": enriched.get("beat"),
        "district": enriched.get("district"),
        "ward": enriched.get("ward"),
        "community_area": enriched.get("community_area"),
        "fbi_code": enriched.get("fbi_code"),
        "x_coordinate": enriched.get("x_coordinate"),
        "y_coordinate": enriched.get("y_coordinate"),
        "latitude": enriched.get("latitude") or enriched.get("lat"),
        "longitude": enriched.get("longitude") or enriched.get("lon"),
        "lat": enriched.get("lat"),
        "lon": enriched.get("lon"),
        "grid_id": enriched.get("grid_id"),
        "severity": enriched.get("severity"),
        "is_violent": int(bool(enriched.get("is_violent"))),
        "domain": enriched.get("domain"),
        "raw_json": json.dumps(enriched, ensure_ascii=False),
        "last_seen_at": now,
    }




def _find_dataset(dataset_id: str) -> Optional[ChicagoDataset]:
    datasets = collect_datasets(limit=500)
    ds = next((d for d in datasets if d.dataset_id == dataset_id), None)
    if not ds:
        log.error("Dataset %s not found in catalog", dataset_id)
    return ds


def _request_json_with_retry(
    session: requests.Session,
    url: str,
    *,
    timeout_s: int,
    max_retries: int,
    backoff_base_s: float,
) -> List[Dict[str, Any]]:
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.get(url, timeout=timeout_s)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                raise ValueError(f"Unexpected payload type: {type(data).__name__}")
            return [r for r in data if isinstance(r, dict)]
        except Exception as exc:
            last_exc = exc
            sleep_s = backoff_base_s * (2 ** (attempt - 1))
            log.warning("Request failed (attempt %d/%d). sleeping %.1fs. url=%s err=%s",
                        attempt, max_retries, sleep_s, url, exc)
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed after {max_retries} retries: {url}") from last_exc


def _iter_rows_where(
    ds: ChicagoDataset,
    session: requests.Session,
    cfg: FetchConfig,
    where: Optional[str] = None,
    *,
    max_retries: int = 5,
    backoff_base_s: float = 1.0,
) -> Iterator[Dict[str, Any]]:
    offset = 0
    page = 0
    while True:
        params: Dict[str, Any] = {"$limit": cfg.per_page, "$offset": offset}
        if where:
            params["$where"] = where
        url = ds.api_url(fmt="json", **params)
        if not url:
            log.warning("No url for dataset %s", ds.dataset_id)
            return

        rows = _request_json_with_retry(
            session, url, timeout_s=cfg.timeout_s, max_retries=max_retries, backoff_base_s=backoff_base_s
        )

        if not rows:
            return

        for row in rows:
            yield row

        rows_fetched = len(rows)
        offset += rows_fetched
        page += 1

        if cfg.max_pages is not None and page >= cfg.max_pages:
            return
        if rows_fetched < cfg.per_page:
            return

        time.sleep(cfg.pause_s)


def _sql_escape(s: str) -> str:
    return s.replace("'", "''")


def _build_where_for_year_and_filters(
    year: int,
    *,
    crime_types: Optional[List[str]] = None,
    wards: Optional[List[int]] = None,
    community_areas: Optional[List[int]] = None,
    location_like: Optional[str] = None,
    extra_clause: Optional[str] = None,
) -> str:
    start_ts = f"{year:04d}-01-01T00:00:00"
    next_start = f"{year+1:04d}-01-01T00:00:00"
    clauses: List[str] = [f"date >= '{start_ts}' AND date < '{next_start}'"]

    if crime_types:
        upper_vals = ", ".join(f"'{_sql_escape(t.upper())}'" for t in crime_types)
        clauses.append(f"upper(primary_type) IN ({upper_vals})")

    if wards:
        nums = ", ".join(str(int(w)) for w in wards)
        clauses.append(f"ward IN ({nums})")

    if community_areas:
        nums = ", ".join(str(int(ca)) for ca in community_areas)
        clauses.append(f"community_area IN ({nums})")

    if location_like:
        pat = _sql_escape(location_like)
        clauses.append(f"location_description LIKE '%{pat}%'")

    if extra_clause:
        clauses.append(f"({extra_clause})")

    return " AND ".join(clauses)




def backfill_chicago_violent_crimes(
    *,
    start_year: int = 2000,
    end_year: int = 2024,
    dataset_id: str = CRIME_DATASET_ID,
    per_page: int = 1000,
    cfg: Optional[FetchConfig] = None,
    only_violent: bool = True,
    crime_types: Optional[List[str]] = None,
    wards: Optional[List[int]] = None,
    community_areas: Optional[List[int]] = None,
    location_like: Optional[str] = None,
    extra_where: Optional[str] = None,
    commit_every: int = 5000,
    resume: bool = True,
) -> None:
    cfg = cfg or FetchConfig(per_page=per_page, pause_s=0.2, timeout_s=30, app_token=None)
    ds = _find_dataset(dataset_id)
    if not ds:
        return

    session = _make_session(cfg.app_token)
    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    _ensure_tables(conn)

    try:
        if resume:
            last_year = _meta_get(conn, "backfill_last_completed_year")
            if last_year and last_year.isdigit():
                start_year = max(start_year, int(last_year) + 1)

        log.info("Backfill target DB=%s years=%d..%d", DB_PATH, start_year, end_year)

        total_saved = 0
        cur = conn.cursor()

        for year in range(start_year, end_year + 1):
            where = _build_where_for_year_and_filters(
                year,
                crime_types=crime_types,
                wards=wards,
                community_areas=community_areas,
                location_like=location_like,
                extra_clause=extra_where,
            )

            log.info("Year %d where=%s", year, where)

            saved = 0
            seen = 0
            batch = 0
            t0 = time.time()

            for raw in _iter_rows_where(ds, session=session, cfg=cfg, where=where):
                seen += 1
                try:
                    enriched = enrich_row(raw)
                except Exception:
                    continue

                if only_violent and not enriched.get("is_violent"):
                    continue

                try:
                    vals = _vals_from_enriched(enriched)
                    cur.execute(UPSERT_SQL, vals)
                    saved += 1
                    batch += 1
                except Exception:
                    continue

                if batch >= commit_every:
                    conn.commit()
                    batch = 0

            conn.commit()

            dt_s = time.time() - t0
            log.info("Year %d done: seen=%d saved=%d time=%.1fs", year, seen, saved, dt_s)

            total_saved += saved
            if resume:
                _meta_set(conn, "backfill_last_completed_year", str(year))

        log.info("Backfill complete: total_saved=%d DB=%s", total_saved, DB_PATH)

    finally:
        conn.close()


if __name__ == "__main__":
    # Real backfill: start simple; expand filters later
    backfill_chicago_violent_crimes(
        start_year=2000,
        end_year=2024,
        per_page=1000,
        only_violent=True,
        commit_every=5000,
        resume=True,
    )
