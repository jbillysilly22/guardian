import time
import requests

from typing import List, Dict, Any, Optional

class SimpleLimiter:
    def __init__(self, max_per_sec: int):
        self.interval = 1.0 / max_per_sec if max_per_sec > 0 else 0.0
        self._last_call = 0.0

    def try_acquire(self, _key: str):
        now = time.time()
        wait = self.interval - (now - self._last_call)
        if wait > 0:
            return False, wait
        self._last_call = now
        return True, 0.0


def fetch_ncvs_data(max_requests: int = 10, max_per_second: int = 5) -> List[Dict[str, Any]]:
    # NOTE: this is the base; most OJP/BJS endpoints require a specific dataset path
    url = "https://api.ojp.gov/bjsdataset/v1/"
    limiter = SimpleLimiter(max_per_second)

    results: List[Dict[str, Any]] = []
    for i in range(max_requests):
        allowed, wait_seconds = limiter.try_acquire("default")
        if not allowed:
            time.sleep(wait_seconds)

        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            results.append(resp.json())
            print(f"Request {i+1} ok.")
        except requests.RequestException as exc:
            print(f"Request {i+1} failed: {exc}")
            continue

    return results


def fetch_fbi_crime_data(
    api_key: str,
    endpoint: str,
    max_requests: int = 10,
    max_per_second: int = 5,
) -> List[Dict[str, Any]]:
    # we dont have the key for fbicrime yet so ensrure to pass it in when we do
    base = "https://api.usa.gov/crime/fbi/cde/"
    url = base + endpoint
    limiter = SimpleLimiter(max_per_second)

    results: List[Dict[str, Any]] = []
    for i in range(max_requests):
        allowed, wait_seconds = limiter.try_acquire("default")
        if not allowed:
            time.sleep(wait_seconds)

        try:
            resp = requests.get(url, params={"API_KEY": api_key}, timeout=10)
            resp.raise_for_status()
            results.append(resp.json())
            print(f"Request {i+1} ok.")
        except requests.RequestException as exc:
            print(f"Request {i+1} failed: {exc}")
            continue

    return results




def doj_news_data(max_requests: int = 10 , max_per_second: int = 5) -> List[Dict[str,Any]]:
    url = '/api/v1/press_releases/98baba74-8922-41de-95f1-73a82695a3d1.json?fields=date,title,url,uuid'
    limiter = SimpleLimiter(max_per_second)

    results = []
    for i in range(max_requests):
                allowed, wait_seconds = limiter.try_acquire("default")
    if not allowed:
            time.sleep(wait_seconds)
    try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            results.append(resp.json())
            print(f"Request {i+1} ok.")
    except requests.RequestException as exc:
            print(f"Request {i+1} failed: {exc}")
    doj_news_data = results
    return doj_news_data

#btw dev the apis for the doj and fbi are broken asf make sure to get them working. the logic works its alright but fix the apis.
   

