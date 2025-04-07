from pydantic import BaseModel
from datetime import datetime
from typing import List, Dict, Literal

class Event(BaseModel):
    event_name: str
    event_datetime: datetime
    client_id: str = None
    user_id: str = None
    payload: Dict = {}

class EventBatch(BaseModel):
    events: List[Event]

class DedupeResponse(BaseModel):
    status: Literal['completed', 'failed']
    processed_events: int
    duplicates: int
    unique_events: int