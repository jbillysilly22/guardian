import requests
import datetime
import time


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

        def is_rate_limited(self, _key: str):
            return (time.time() - self._last_call) < self.interval

limiter_result = SimpleLimiter





#national crime vitcim survey data fetcher kinda scared to use cuz its national but whatevs
def fetch_ncvm_data(max_requests: int = 20000, max_per_second: int = 5):
   
    ncvm_path = "https://api.ojp.gov/bjsdataset/v1/"

    results = []
    for i in range(max_requests):
        allowed, wait_seconds = SimpleLimiter.try_acquire("default")
        if not allowed:
            
            time.sleep(wait_seconds)

        try:
            response = requests.get(ncvm_path, timeout=10)
            response.raise_for_status()
            results.append(response.json())
            print(f"Request {i+1} allowed.")
        except requests.RequestException as exc:
           
            print(f"Request {i+1} failed: {exc}")
           
            continue

    return results


def fetch

