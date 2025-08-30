from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
import re
from ingest.config import SCHEMA_VERSION, ALLOWED_PROTOCOLS

class Item(BaseModel):
    serial_number: str = Field(..., min_length=16, max_length=24)
    location: Optional[str] = None
    protocol_type: str = Field(..., pattern=r"^(?i)(rps|pms|css|dss)$")
    token: str = Field(..., min_length=1)
    token_created_at: datetime = Field(...)

    @field_validator("protocol_type")
    @classmethod
    def normalize_protocol(cls, v: str) -> str:
        return v.lower()

    @field_validator("token_created_at", mode="before")
    @classmethod
    def validate_token_created_at(cls, v: str) -> datetime:
        try:
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                raise ValueError("Timezone required")
            return dt
        except ValueError as e:
            raise ValueError(f"Invalid timezone-aware ISO8601: {e}")

class IngestRequest(BaseModel):
    schema_version: Literal[1] = Field(..., eq=SCHEMA_VERSION)
    sent_at: datetime = Field(...)
    client_request_id: Optional[str] = Field(None, max_length=128)
    items: List[Item] = Field(...)

    @field_validator("sent_at", mode="before")
    @classmethod
    def validate_sent_at(cls, v: str) -> datetime:
        try:
            dt = datetime.fromisoformat(v)
            if dt.tzinfo is None:
                raise ValueError("Timezone required")
            return dt
        except ValueError as e:
            raise ValueError(f"Invalid timezone-aware ISO8601: {e}")

    @field_validator("client_request_id")
    @classmethod
    def validate_client_request_id(cls, v: Optional[str]) -> Optional[str]:
        if v and not re.match(r"^[A-Za-z0-9._-]+$", v):
            raise ValueError("Invalid charset: only [A-Za-z0-9._-]")
        return v

    @model_validator(mode="after")
    def check_items_length(self):
        if not (1 <= len(self.items) <= 100):
            raise ValueError("items must have length 1..100")
        return self

class TestRequest(IngestRequest):
    # For /test, allow empty items or no items for ping mode
    items: Optional[List[Item]] = Field(None)

    @model_validator(mode="after")
    def check_items_length(self):
        if self.items is not None and len(self.items) > 100:
            raise ValueError("items must have length <=100")
        return self

class ErrorDetail(BaseModel):
    index: int
    code: str
    detail: str

class HealthResponse(BaseModel):
    status: str = "ok"
    ts: str = Field(...)
    version: str = Field(...)
    schema_version: int = SCHEMA_VERSION

class IngestResponse(BaseModel):
    status: str = "ok"
    schema_version: int = SCHEMA_VERSION
    request_id: str
    client_request_id: Optional[str] = None
    mb_ip: str
    received: int
    accepted: int
    rejected: int
    errors: List[ErrorDetail]

class TestResponse(IngestResponse):
    mode: str
    content_length: int
    content_encoding: str
    note: str = "dry-run; nothing enqueued or persisted"
