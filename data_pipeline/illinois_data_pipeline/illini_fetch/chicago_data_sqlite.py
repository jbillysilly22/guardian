# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_fetcher import (
    iter_dataset_rows,
    FetchConfig,
    _make_session,
)
from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_endpoint_catalog import (
    collect_datasets,
    ChicagoDataset,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CRIME_DATASET_ID = "ijzp-q8t2"  # Chicago crimes dataset id used elsewhere in repo
INTERVAL_SECONDS = 60 * 10

from core.paths import app_data_dir

DB_DIR = app_data_dir("guardian")
DB_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = DB_DIR / "violent_crimes.sqlite"


def get_conn(*, timeout: int = 30) -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=timeout)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    # optional but helpful if you ever have two writers briefly overlap
    conn.execute("PRAGMA busy_timeout=60000;")  # 60s
    return conn


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
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
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    conn.commit()


def _make_case_key(row: Dict[str, Any]) -> str:
    case = row.get("case_number") or row.get("case") or row.get("id")
    if case:
        return str(case)
    raw = json.dumps({k: row.get(k) for k in sorted(row.keys())}, sort_keys=True, default=str)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _upsert_enriched(conn: sqlite3.Connection, enriched: Dict[str, Any]) -> None:
    cur = conn.cursor()
    key = _make_case_key(enriched)
    row_hash = hashlib.sha1(json.dumps(enriched, sort_keys=True, default=str).encode()).hexdigest()
    now = dt.datetime.utcnow().isoformat()

    vals = {
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

    cur.execute(
        """
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
            last_seen_at=excluded.last_seen_at
        """,
        vals,
    )


def _find_crime_dataset() -> Optional[ChicagoDataset]:
    datasets = collect_datasets()
    ds = next((d for d in datasets if d.dataset_id == CRIME_DATASET_ID), None)
    if not ds:
        log.error("Crime dataset %s not found in catalog", CRIME_DATASET_ID)
    return ds


def run_forever(poll_interval: int = INTERVAL_SECONDS, commit_every: int = 1000) -> None:
    # local import avoids circular import at module import-time
    from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_filter import enrich_row

    cfg = FetchConfig(per_page=1000, pause_s=0.2, timeout_s=15, app_token=None)
    session = _make_session(cfg.app_token)
    ds = _find_crime_dataset()
    if not ds:
        return

    conn = get_conn(timeout=30)
    ensure_schema(conn)
    log.info("Started continuous pull → DB: %s (interval %ds)", DB_PATH, poll_interval)

    try:
        while True:
            count_inserted = 0
            count_seen = 0
            batch = 0

            try:
                for raw_row in iter_dataset_rows(ds, session=session, cfg=cfg):
                    count_seen += 1

                    try:
                        enriched = enrich_row(raw_row)
                    except Exception:
                        log.exception("Failed to enrich row; skipping")
                        continue

                    # only store violent rows
                    if not enriched.get("is_violent"):
                        continue

                    _upsert_enriched(conn, enriched)
                    count_inserted += 1
                    batch += 1

                    if batch >= commit_every:
                        conn.commit()
                        batch = 0

            except Exception:
                log.exception("Error while fetching/enriching rows; will continue on next cycle")

            if batch:
                conn.commit()

            try:
                total_rows = conn.execute("SELECT COUNT(*) FROM violent_crimes").fetchone()[0]
                log.info(
                    "Cycle complete: seen=%d violent_saved=%d total_rows=%d",
                    count_seen,
                    count_inserted,
                    total_rows,
                )
            except Exception:
                log.exception("Failed to count violent_crimes rows after cycle")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        log.info("Interrupted by user; shutting down")
    finally:
        conn.close()


if __name__ == "__main__":
    run_forever()

# watch for bad sqlite and bad dtc deprecation warnings gotta upddate stuff
