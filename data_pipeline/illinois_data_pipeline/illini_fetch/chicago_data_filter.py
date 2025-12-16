# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import datetime as dt
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
import sys


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



VIOLENT_TYPES = {
    "homicide",
    "robbery",
    "assault",
    "battery",
    "criminal sexual assault",
    "weapons violation",
    "arson",
}

SEVERITY_MAP = {
    "homicide": 1.0,
    "criminal sexual assault": 0.9,
    "robbery": 0.7,
    "weapons violation": 0.6,
    "assault": 0.5,
    "battery": 0.4,
}



def parse_dt(v: Any) -> Optional[dt.datetime]:
    if not v:
        return None
    try:
        return dt.datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def try_float(v: Any) -> Optional[float]:
    try:
        return float(v)
    except Exception:
        return None


def extract_lat_lon(row: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    for lat_k, lon_k in (("latitude", "longitude"), ("lat", "lon"), ("lat", "lng")):
        lat = try_float(row.get(lat_k))
        lon = try_float(row.get(lon_k))
        if lat is not None and lon is not None:
            return lat, lon

    loc = row.get("location")
    if isinstance(loc, dict):
        return try_float(loc.get("latitude")), try_float(loc.get("longitude"))

    return None, None


def grid_id(lat: Optional[float], lon: Optional[float], precision: int = 3) -> Optional[str]:
    if lat is None or lon is None:
        return None
    return f"{round(lat, precision)}:{round(lon, precision)}"



def enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
    lat, lon = extract_lat_lon(row)
    dttm = parse_dt(row.get("date"))

    raw_type = (
        row.get("primary_type")
        or row.get("offense")
        or row.get("category")
        or ""
    )

    t = str(raw_type).strip().lower()
    severity = SEVERITY_MAP.get(t, 0.2)

    enriched = dict(row)
    enriched.update(
        {
            # geo
            "lat": lat,
            "lon": lon,
            "grid_id": grid_id(lat, lon),

            # time
            "datetime": dttm.isoformat() if dttm else None,
            "hour": dttm.hour if dttm else None,
            "day_of_week": dttm.weekday() if dttm else None,
            "month": dttm.month if dttm else None,
            "year": dttm.year if dttm else None,
            "is_night": (dttm.hour >= 22 or dttm.hour <= 5) if dttm else None,

            # severity / type
            "primary_type_norm": t or None,
            "is_violent": t in VIOLENT_TYPES,
            "severity": severity,

            # domain tag
            "domain": "crime",
        }
    )
    return enriched

# ------------------
# RUNNER
# ------------------

def run_enrichment(
    dataset_id: str,
    *,
    out_dir: str = "data/enriched",
    cfg: Optional[FetchConfig] = None,
) -> Path:
    cfg = cfg or FetchConfig()

    ds = next(
        (d for d in collect_datasets(text_filter="public safety", limit=500)
         if d.dataset_id == dataset_id),
        None,
    )
    if not ds:
        raise RuntimeError(f"Dataset {dataset_id} not found")

    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    stamp = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_file = out_path / f"{dataset_id}_enriched_{stamp}.jsonl"

    session = _make_session(cfg.app_token)

    count = 0
    with out_file.open("w", encoding="utf-8") as fh:
        for row in iter_dataset_rows(ds, session=session, cfg=cfg):
            enriched = enrich_row(row)
            fh.write(json.dumps(enriched, ensure_ascii=False) + "\n")
            count += 1

    log.info("Wrote %d enriched rows → %s", count, out_file)
    return out_file


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    TARGET_DATASET_ID = "85ca-t3if"  # Traffic Crashes – Crashes

    cfg = FetchConfig(
        per_page=1000,
        max_pages=2,
        pause_s=0.2,
        timeout_s=15,
    )

    run_enrichment(TARGET_DATASET_ID, cfg=cfg)
