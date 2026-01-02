from typing import Any, Dict, List, Optional
from dateutil import parser as dt_parser
import time

def now_ms() -> int:
    return int(time.time() * 1000)

class DataNormalizer:
    def __init__(self):
        pass

    def _split_to_array(self, s: Optional[str]) -> List[str]:
        if not s:
            return []
        # try comma first; if no comma, return single element
        parts = [p.strip() for p in s.split(",")]
        return [p for p in parts if p]

    def normalize(self, record: Dict[str, Any]) -> Dict[str, Any]:
        # Keep original
        source_record = record.copy()

        # 1) attributes string -> array
        attributes_arr = self._split_to_array(record.get("attributes"))

        # 2) date -> epoch ms
        date_str = record.get("date")
        event_time_ms = None
        if date_str:
            event_time_ms = int(dt_parser.isoparse(date_str).timestamp() * 1000)

        # 3) ingestion timestamp
        ingestion_ts = now_ms()

        return {
            "station_id": record.get("station"),
            "datatype": record.get("datatype"),
            "value": record.get("value"),
            "event_time_ms": event_time_ms,
            "attributes": attributes_arr,
            "source_record": source_record,
            "ingestion_ts": ingestion_ts,
        }

