from pydantic import BaseModel, Field, validator
from datetime import datetime
from typing import Dict, Any

class Event(BaseModel):
    topic: str = Field(..., min_length=1)
    event_id: str = Field(..., min_length=1)
    timestamp: datetime = Field(default_factory=datetime.now)
    source: str = Field(..., min_length=1)
    payload: Dict[str, Any]