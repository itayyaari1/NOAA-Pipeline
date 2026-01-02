from typing import Any, Dict, Tuple
from .schema import NormalizedPrecipRecord
from pydantic import ValidationError

class DataValidator:
    def validate(self, record: Dict[str, Any]) -> Tuple[bool, str]:
        try:
            NormalizedPrecipRecord(**record)
            return True, ""
        except ValidationError as e:
            return False, str(e)

