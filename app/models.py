"""Pydantic data models for Queuemaxxing Lite API."""

from __future__ import annotations

from typing import Any, Dict, Literal
from pydantic import BaseModel, Field, field_validator


class CreateQueueRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, description="Queue name (1-128 characters)")
    ordering: Literal["fifo", "lifo"] = Field(..., description="Ordering algorithm ('fifo' or 'lifo')")
    priority_enabled: bool = Field(default=False, description="Whether priority ordering is enabled")

    @field_validator("ordering", mode="before")
    @classmethod
    def normalize_ordering(cls, v: Any) -> Any:
        if isinstance(v, str):
            return v.lower()
        return v


class QueueResponse(BaseModel):
    id: str
    name: str
    ordering: str
    priority_enabled: bool
    created_at: str


class PushItemRequest(BaseModel):
    payload: Dict[str, Any] = Field(..., description="Arbitrary JSON payload object")
    priority: int = Field(default=0, description="Item priority")
    delay_seconds: int = Field(default=0, ge=0, description="Delay in seconds before item becomes available")


class PushItemResponse(BaseModel):
    id: str
    queue_id: str
    priority: int
    created_at: str
    available_at: str
    status: Literal["ready", "delayed"]


class ClaimItemResponse(BaseModel):
    id: str
    queue_id: str
    payload: Dict[str, Any]
    priority: int
    sequence: int
    created_at: str
    available_at: str
