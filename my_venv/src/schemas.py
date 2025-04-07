from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime

class Event(BaseModel):
    event_name: str
    client_id: str
    event_datetime: datetime
    payload: Dict[str, Any] # В общем виде

class EventBatch(BaseModel):
    events: List[Event]

class DedupeResult(BaseModel):
    status: str #
    duplicates: int
    unique_events: int
    duplicate_hashes: List[str]