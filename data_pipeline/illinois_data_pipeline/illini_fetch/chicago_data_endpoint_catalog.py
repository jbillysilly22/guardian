from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlencode
import logging
import re
import requests

log = logging.getLogger(__name__)

CATALOG_URL = "https://data.cityofchicago.org/data.json"
SOCRATA_DOMAIN = "data.cityofchicago.org"




@dataclass(frozen=True)
class ChicagoDataset:
    title: str
    description: str
    identifier: Optional[str]
    landing_page: Optional[str]
    keywords: List[str]
    distributions: List[Dict[str, Any]]
    dataset_id: Optional[str]  # e.g. "ijzp-q8t2"

    def api_base(self) -> Optional[str]:
        if not self.dataset_id:
            return None
        return f"https://{SOCRATA_DOMAIN}/resource/{self.dataset_id}"

    def api_url(self, *, fmt: str = "json", **params: Any) -> Optional[str]:
        """
        Build a Socrata SODA URL like:
        https://data.cityofchicago.org/resource/ijzp-q8t2.json?$limit=100
        """
        base = self.api_base()
        if not base:
            return None
        qs = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True)
        return f"{base}.{fmt}" + (f"?{qs}" if qs else "")




def fetch_json(url: str, *, timeout: int = 15, headers: Optional[Dict[str, str]] = None) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.get(url, timeout=timeout, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            log.warning("Expected dict JSON from %s, got %s", url, type(data).__name__)
            return None
        return data
    except requests.RequestException as exc:
        log.error("Request failed: %s", exc)
        return None




def _as_lower_str_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v).strip().lower() for v in value if str(v).strip()]
    if isinstance(value, str):
        
        return [v.strip().lower() for v in value.split(",") if v.strip()]
    return [str(value).strip().lower()] if str(value).strip() else []


def extract_keywords(ds: Dict[str, Any]) -> List[str]:
    # Chicago catalog entries vary: keyword / keywords / theme / category
    keys: List[str] = []
    keys += _as_lower_str_list(ds.get("keyword"))
    keys += _as_lower_str_list(ds.get("keywords"))
    keys += _as_lower_str_list(ds.get("theme"))
    keys += _as_lower_str_list(ds.get("category"))
    # de-dupe but keep order
    seen = set()
    out: List[str] = []
    for k in keys:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return out


_DATASET_ID_RE = re.compile(r"\b([a-z0-9]{4}-[a-z0-9]{4})\b", re.IGNORECASE)

def extract_dataset_id(ds: Dict[str, Any]) -> Optional[str]:
    """
    Try to find the Socrata dataset ID like ijzp-q8t2 from common fields:
    - identifier
    - landingPage
    - distribution[].downloadURL / accessURL
    """
    candidates: List[str] = []

    for field in ("identifier", "id", "landingPage"):
        v = ds.get(field)
        if isinstance(v, str):
            candidates.append(v)

    dist = ds.get("distribution")
    if isinstance(dist, list):
        for d in dist:
            if isinstance(d, dict):
                for field in ("downloadURL", "accessURL", "url"):
                    v = d.get(field)
                    if isinstance(v, str):
                        candidates.append(v)

    for text in candidates:
        m = _DATASET_ID_RE.search(text)
        if m:
            return m.group(1).lower()

    return None


def dataset_to_model(ds: Dict[str, Any]) -> ChicagoDataset:
    title = (ds.get("title") or "").strip()
    description = (ds.get("description") or "") or ""
    identifier = ds.get("identifier") or ds.get("id")
    landing_page = ds.get("landingPage")
    distributions = ds.get("distribution", [])
    if not isinstance(distributions, list):
        distributions = []

    return ChicagoDataset(
        title=title,
        description=description,
        identifier=str(identifier) if identifier is not None else None,
        landing_page=str(landing_page) if landing_page is not None else None,
        keywords=extract_keywords(ds),
        distributions=distributions,
        dataset_id=extract_dataset_id(ds),
    )




def matches_text(d: ChicagoDataset, text: str) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return True
    haystack = " ".join(
        [
            d.title.lower(),
            str(d.description).lower(),
            " ".join(d.keywords),
        ]
    )
    return t in haystack


def collect_datasets(
    *,
    text_filter: Optional[str] = None,
    require_api: bool = True,
    limit: int = 50,
    timeout: int = 15,
) -> List[ChicagoDataset]:
  
    
    catalog = fetch_json(CATALOG_URL, timeout=timeout)
    if not catalog:
        return []

    raw = catalog.get("dataset", [])
    if not isinstance(raw, list):
        log.warning("Unexpected catalog format: 'dataset' is %s", type(raw).__name__)
        return []

    out: List[ChicagoDataset] = []
    for ds in raw:
        if not isinstance(ds, dict):
            continue
        model = dataset_to_model(ds)

        if require_api and not model.dataset_id:
            continue
        if text_filter and not matches_text(model, text_filter):
            continue

        out.append(model)
        if len(out) >= limit:
            break

    return out




def build_endpoints(
    datasets: Iterable[ChicagoDataset],
    *,
    fmt: str = "json",
    default_limit: int = 5000,
) -> List[Dict[str, Any]]:
   
    rows: List[Dict[str, Any]] = []
    for d in datasets:
        url = d.api_url(fmt=fmt, **{"$limit": default_limit})
        if not url:
            continue
        rows.append(
            {
                "title": d.title,
                "dataset_id": d.dataset_id,
                "url": url,
                "landing_page": d.landing_page,
            }
        )
    return rows




if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # "bad stuff" starter pack: crime + crashes + fires keywords
    hits = collect_datasets(text_filter="public safety", limit=25)
    for row in build_endpoints(hits, default_limit=1000)[:10]:
        print(row["dataset_id"], "-", row["title"])
        print("  ", row["url"])




