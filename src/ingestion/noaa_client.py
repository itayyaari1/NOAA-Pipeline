import requests
from typing import Any, Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential

class NOAAClient:
    def __init__(self, base_url: str, token: str, timeout_s: int = 30):
        self.base_url = base_url
        self.token = token
        self.timeout_s = timeout_s

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(min=1, max=10))
    def _get(self, params: Dict[str, Any]) -> Dict[str, Any]:
        headers = {"token": self.token}
        r = requests.get(self.base_url, headers=headers, params=params, timeout=self.timeout_s)
        r.raise_for_status()
        return r.json()

    def fetch_month(self, dataset_id: str, start_date: str, end_date: str, limit: int = 1000) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        offset = 1  # NOAA uses 1-based offsets

        while True:
            params = {
                "datasetid": dataset_id,
                "startdate": start_date,
                "enddate": end_date,
                "limit": limit,
                "offset": offset,
            }
            payload = self._get(params)
            results = payload.get("results", []) or []
            out.extend(results)

            if len(results) < limit:
                break
            offset += limit

        return out

