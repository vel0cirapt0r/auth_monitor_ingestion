from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from datetime import datetime
import re
from ingest.config import SCHEMA_VERSION, ALLOWED_PROTOCOLS

class Item(BaseModel):
    serial_number: str = Field(..., min_length=16, max_length=24)
    location: Optional[str] = None
    protocol_type: str = Field(...)
    token: str = Field(..., min_length=1)
    token_created_at: datetime = Field(...)

    @field_validator("protocol_type")
    @classmethod
    def validate_and_normalize_protocol(cls, v: str) -> str:
        lower_v = v.lower()
        if lower_v not in ALLOWED_PROTOCOLS:
            raise ValueError(f"protocol_type must be one of: {','.join(ALLOWED_PROTOCOLS)}")
        return lower_v

    @field_validator("token_created_at", mode="before")
    @classmethod
    def validate_token_created_at(cls, v: str) -> datetime:
        if isinstance(v, str):
            v = v.replace("Z", "+00:00")  # Handle Z as UTC
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
    items: List[dict] = Field(...)  # Raw dicts for per-item validation in endpoint

    @field_validator("sent_at", mode="before")
    @classmethod
    def validate_sent_at(cls, v: str) -> datetime:
        if isinstance(v, str):
            v = v.replace("Z", "+00:00")  # Handle Z as UTC
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

class TestRequest(BaseModel):
    schema_version: Optional[Literal[1]] = Field(None, eq=SCHEMA_VERSION)
    sent_at: Optional[datetime] = None
    client_request_id: Optional[str] = Field(None, max_length=128)
    items: Optional[List[dict]] = Field(None)  # Raw dicts for per-item validation

    @field_validator("sent_at", mode="before")
    @classmethod
    def validate_sent_at(cls, v: Optional[str]) -> Optional[datetime]:
        if v is None:
            return None
        v = v.replace("Z", "+00:00")
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
    def check_for_ping_or_validate(self):
        if self.items is not None:
            if self.schema_version is None or self.sent_at is None:
                raise ValueError("schema_version and sent_at required when items present")
            if len(self.items) > 100:
                raise ValueError("items must have length <=100")
        return self

class ErrorDetail(BaseModel):
    index: int
    code: str
    detail: str

class HealthResponse(BaseModel):
    status: str = "ok"
    time: str = Field(...)
    version: str = Field(...)

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
