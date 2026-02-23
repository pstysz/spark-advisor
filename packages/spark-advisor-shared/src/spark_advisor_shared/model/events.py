from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _new_uuid() -> str:
    return str(uuid4())


class MessageMetadata(BaseModel):
    model_config = ConfigDict(frozen=True)

    message_id: str = Field(default_factory=_new_uuid)
    source: str = ""
    timestamp: datetime = Field(default_factory=_utc_now)
    trace_id: str = ""
    span_id: str = ""


class KafkaEnvelope(BaseModel):
    model_config = ConfigDict(frozen=True)

    metadata: MessageMetadata
    payload: dict[str, Any]
