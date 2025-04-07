from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional, Dict, List, Literal


class Event(BaseModel):
    event_name: str
    event_datetime: datetime
    client_id: Optional[str] = Field(None)
    user_id: Optional[str] = Field(None)
    payload: Dict = Field(default_factory=dict)

    # Все дополнительные поля будут попадать в payload
    class Config:
        extra = 'allow'


class EventBatch(BaseModel):
    events: List[Event]


class DedupeResponse(BaseModel):
    status: Literal['completed', 'failed']
    processed_events: int
    duplicates: int
    unique_events: int