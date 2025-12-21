# -*- coding: utf-8 -*-
from __future__ import annotations

import logging
import sys
import time
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
from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_filter import enrich_row
from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_sqlite import (
    DB_PATH,
    ensure_schema,
    get_conn,
    _upsert_enriched,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

CRIME_DATASET_ID = "ijzp-q8t2"

def _find_dataset(dataset_id: str) -> Optional[ChicagoDataset]:
    # ijzp-q8t2 isn't guaranteed to show up in the first 500 results trying to fix that error
    for lim in (500, 2000, 5000, 10000):
        datasets = collect_datasets(limit=lim)
        ds = next((d for d in datasets if getattr(d, "dataset_id", None) == dataset_id), None)
        if ds:
            return ds

    log.error("Dataset %s not found in catalog (searched up to 10k results)", dataset_id)
    return None



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
    conn = get_conn(timeout=60)
    ensure_schema(conn)

    try:
        if resume:
            last_year = conn.execute("SELECT value FROM meta WHERE key=?", ("backfill_last_completed_year",)).fetchone()
            last_year = last_year[0] if last_year else None
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
                    _upsert_enriched(conn, enriched)
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
                conn.execute(
                    "INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    ("backfill_last_completed_year", str(year)),
                )
                conn.commit()

        try:
            total_rows = conn.execute("SELECT COUNT(*) FROM violent_crimes").fetchone()[0]
        except Exception:
            total_rows = None
            log.exception("Failed to count violent_crimes after backfill")

        log.info("Backfill complete: total_saved=%d DB=%s total_rows=%s", total_saved, DB_PATH, total_rows)

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


 
