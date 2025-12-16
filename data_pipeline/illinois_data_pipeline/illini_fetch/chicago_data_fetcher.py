# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
import logging
import sys
import time
import json

import requests


PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from data_pipeline.illinois_data_pipeline.illini_fetch.chicago_data_endpoint_catalog import (
    collect_datasets,
    ChicagoDataset,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FetchConfig:
    per_page: int = 1000
    max_pages: Optional[int] = None
    pause_s: float = 0.2
    timeout_s: int = 15
    app_token: Optional[str] = None  # Socrata app token (optional)


def _make_session(app_token: Optional[str]) -> requests.Session:
    session = requests.Session()
    if app_token:
        session.headers.update({"X-App-Token": app_token})
    session.headers.setdefault("User-Agent", "guardian-data-fetcher/0.1")
    return session


def iter_dataset_rows(
    dataset: ChicagoDataset,
    *,
    session: requests.Session,
    cfg: FetchConfig,
) -> Iterator[Dict[str, Any]]:
    """
    Page through a Socrata dataset using $limit/$offset and yield rows.
    """
    if not dataset.dataset_id:
        log.warning("Dataset missing dataset_id: title=%r", dataset.title)
        return

    offset = 0
    page = 0

    while True:
        url = dataset.api_url(fmt="json", **{"$limit": cfg.per_page, "$offset": offset})
        if not url:
            log.warning("No API URL for dataset_id=%s title=%r", dataset.dataset_id, dataset.title)
            return

        try:
            resp = session.get(url, timeout=cfg.timeout_s)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            log.error("Request failed dataset_id=%s url=%s err=%s", dataset.dataset_id, url, exc)
            return
        except ValueError as exc:
            log.error("JSON decode failed dataset_id=%s url=%s err=%s", dataset.dataset_id, url, exc)
            return

        if not isinstance(data, list):
            log.warning("Unexpected payload dataset_id=%s got=%s", dataset.dataset_id, type(data).__name__)
            return

        if not data:
            return

        for row in data:
            if isinstance(row, dict):
                yield row

        rows_fetched = len(data)
        offset += rows_fetched
        page += 1

        if cfg.max_pages is not None and page >= cfg.max_pages:
            return

        if rows_fetched < cfg.per_page:
            return

        time.sleep(cfg.pause_s)


def pull_data(
    *,
    text_filter: str = "public safety",
    dataset_limit: int = 10,
    cfg: Optional[FetchConfig] = None,
    output_dir: str = "data/raw",
    save_jsonl: bool = False,
    sanity_rows: int = 3,
) -> Dict[str, int]:
    """
    Discover datasets via the catalog module, then fetch rows from each dataset endpoint.
    Returns: dict of dataset_id -> rows_fetched_count
    """
    cfg = cfg or FetchConfig()
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    datasets = collect_datasets(text_filter=text_filter, limit=dataset_limit)
    if not datasets:
        log.info("No datasets found for filter=%r", text_filter)
        return {}

    session = _make_session(cfg.app_token)
    results: Dict[str, int] = {}

    for ds in datasets:
        ds_id = ds.dataset_id or "unknown"
        log.info("Fetching dataset_id=%s title=%r", ds_id, ds.title)

        count = 0
        preview: List[Dict[str, Any]] = []

        jsonl_path = out_dir / f"{ds_id}.jsonl" if (save_jsonl and ds.dataset_id) else None
        f = open(jsonl_path, "w", encoding="utf-8") if jsonl_path else None

        try:
            for row in iter_dataset_rows(ds, session=session, cfg=cfg):
                count += 1

                if sanity_rows > 0 and len(preview) < sanity_rows:
                    preview.append(row)

                if f:
                    f.write(json.dumps(row, ensure_ascii=False) + "\n")

        finally:
            if f:
                f.close()

        results[ds_id] = count

        if sanity_rows > 0:
            log.info("Preview dataset_id=%s (first %d rows):", ds_id, len(preview))
            for i, row in enumerate(preview, start=1):
                keys = list(row.keys())[:8]
                small = {k: row.get(k) for k in keys}
                log.info("  %d) keys=%s sample=%s", i, keys, small)

        log.info("Done dataset_id=%s rows=%d", ds_id, count)

    return results


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    cfg = FetchConfig(
        per_page=1000,
        max_pages=2,  # sanity: do not pull too much yet
        pause_s=0.2,
        timeout_s=15,
        app_token=None,
    )

    summary = pull_data(
        text_filter="public safety",
        dataset_limit=5,
        cfg=cfg,
        save_jsonl=False,
        sanity_rows=3,
    )

    print("\nSummary (dataset_id -> rows fetched):")
    for k, v in summary.items():
        print(k, "->", v)
