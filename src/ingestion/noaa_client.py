import requests
from typing import Any, Dict, List
from tenacity import retry, stop_after_attempt, wait_exponential
from ..utils.logging import get_logger

logger = get_logger(__name__)

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
        page_num = 1

        # Get total count from first request
        first_params = {
            "datasetid": dataset_id,
            "startdate": start_date,
            "enddate": end_date,
            "limit": limit,
            "offset": offset,
        }
        first_payload = self._get(first_params)
        total_count = first_payload.get("metadata", {}).get("resultset", {}).get("count", 0)
        estimated_pages = (total_count + limit - 1) // limit if total_count > 0 else 0
        
        results = first_payload.get("results", []) or []
        out.extend(results)
        logger.info(f"Fetching {total_count:,} records ({estimated_pages} pages)...")
        logger.info(f"  Page {page_num}/{estimated_pages}: {len(results)} records")
        
        if len(results) < limit:
            return out
        
        offset += limit
        page_num += 1

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
            logger.info(f"  Page {page_num}/{estimated_pages}: {len(results)} records (total: {len(out):,})")

            if len(results) < limit:
                break
            offset += limit
            page_num += 1

        return out

