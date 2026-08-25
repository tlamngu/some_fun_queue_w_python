""" Provide state management and queue operations for the queue service. """

from __future__ import annotations

import asyncio
from typing import Any, Dict, Optional, Tuple

#Core implemnetatoin of Queue Service using frankenstein_queue
import frankenstein_queue
from frankenstein_queue import (
    FrankensteinQueue,
    QueueItem,
    create_queue_instance,
    current_iso_utc,
)
from app.models import (
    ClaimItemResponse,
    CreateQueueRequest,
    PushItemRequest,
    PushItemResponse,
    QueueResponse,
)
from app.storage import Storage


class QueueNotFoundError(Exception):
    pass


class QueueItemNotFoundError(Exception):
    pass


class DuplicateQueueNameError(Exception):
    pass


class QueueService:
    """Core queue service orchestrating in-memory queues, concurrency lock, and persistence."""

    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage or Storage()
        self.lock = asyncio.Lock()
        self.queues: Dict[str, FrankensteinQueue] = {}
        self.load_from_storage()

    def _dump_state(self) -> Dict[str, Any]:
        """Serializes current queue and item state to dict for storage."""
        queues_dict = {}
        items_dict = {}
        for q_id, q in self.queues.items():
            queues_dict[q_id] = q.to_dict()
            for item_id, item in q.items.items():
                items_dict[item_id] = item.to_dict()
        return {"queues": queues_dict, "items": items_dict}

    def load_from_storage(self) -> None:
        """Loads queues and items from storage into memory."""
        data = self.storage.load_state()
        queues_data = data.get("queues", {})
        items_data = data.get("items", {})

        self.queues = {}
        for q_id, q_info in queues_data.items():
            q_instance = create_queue_instance(
                id=q_info.get("id", q_id),
                name=q_info.get("name", ""),
                ordering=q_info.get("ordering", "fifo"),
                priority_enabled=q_info.get("priority_enabled", False),
                sequence_counter=q_info.get("sequence_counter", 0),
                created_at=q_info.get("created_at"),
            )
            self.queues[q_id] = q_instance

        for item_id, item_info in items_data.items():
            q_id = item_info.get("queue_id")
            if q_id and q_id in self.queues:
                item = QueueItem(
                    payload=item_info.get("payload", {}),
                    priority=item_info.get("priority", 0),
                    id=item_info.get("id", item_id),
                    queue_id=q_id,
                    sequence=item_info.get("sequence", 0),
                    created_at=item_info.get("created_at"),
                    available_at=item_info.get("available_at"),
                )
                self.queues[q_id].items[item.id] = item

    async def create_queue(self, req: CreateQueueRequest) -> QueueResponse:
        """Creates a new queue atomically."""
        async with self.lock:
            # Check for duplicate queue name
            for existing in self.queues.values():
                if existing.name == req.name:
                    raise DuplicateQueueNameError(f"Queue with name '{req.name}' already exists.")

            queue = create_queue_instance(
                name=req.name,
                ordering=req.ordering,
                priority_enabled=req.priority_enabled,
            )
            self.queues[queue.id] = queue
            self.storage.save_state(self._dump_state())

            return QueueResponse(
                id=queue.id,
                name=queue.name,
                ordering=queue.ordering,
                priority_enabled=queue.priority_enabled,
                created_at=queue.created_at,
            )

    async def push_item(self, queue_id: str, req: PushItemRequest) -> PushItemResponse:
        """Pushes an item to a specified queue atomically."""
        async with self.lock:
            if queue_id not in self.queues:
                raise QueueNotFoundError(f"Queue '{queue_id}' not found.")

            queue = self.queues[queue_id]
            item = queue.push(
                payload=req.payload,
                priority=req.priority,
                delay_seconds=req.delay_seconds,
            )
            self.storage.save_state(self._dump_state())

            status = "ready" if req.delay_seconds == 0 else "delayed"
            return PushItemResponse(
                id=item.id,
                queue_id=queue.id,
                priority=item.priority,
                created_at=item.created_at,
                available_at=item.available_at,
                status=status,
            )

    async def claim_item(
        self, queue_id: str, current_time_iso: Optional[str] = None
    ) -> Optional[ClaimItemResponse]:
        """Atomically retrieves and removes the next eligible item from the queue."""
        async with self.lock:
            if queue_id not in self.queues:
                raise QueueNotFoundError(f"Queue '{queue_id}' not found.")

            queue = self.queues[queue_id]
            item = queue.claim(current_time_iso=current_time_iso)
            if item is None:
                return None

            self.storage.save_state(self._dump_state())
            return ClaimItemResponse(
                id=item.id,
                queue_id=queue.id,
                payload=item.payload,
                priority=item.priority,
                sequence=item.sequence,
                created_at=item.created_at,
                available_at=item.available_at,
            )

    async def delete_item(self, queue_id: str, item_id: str) -> None:
        """Atomically removes a specific item from a queue."""
        async with self.lock:
            if queue_id not in self.queues:
                raise QueueNotFoundError(f"Queue '{queue_id}' not found.")

            queue = self.queues[queue_id]
            if item_id not in queue.items:
                raise QueueItemNotFoundError(
                    f"Item '{item_id}' not found in queue '{queue_id}'."
                )

            queue.delete(item_id)
            self.storage.save_state(self._dump_state())
