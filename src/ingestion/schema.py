from pydantic import BaseModel, Field, ConfigDict
from typing import Any, Dict, List, Optional

class NormalizedPrecipRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    station_id: str = Field(min_length=1)
    datatype: str = Field(min_length=1)
    value: int
    event_time_ms: int
    attributes: List[str] = Field(default_factory=list)
    source_record: Dict[str, Any]
    ingestion_ts: int

