"""Frankenstein Queue implementation for Queuemaxxing Lite."""

from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def current_iso_utc() -> str:
    """Return current UTC timestamp in ISO 8601 format ending with Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def iso_utc_from_timestamp(ts: float) -> str:
    """Convert a unix timestamp to ISO 8601 UTC string."""
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_iso_utc(iso_str: str) -> datetime:
    """Parse ISO 8601 UTC string into a timezone-aware datetime."""
    if iso_str.endswith("Z"):
        iso_str = iso_str[:-1] + "+00:00"
    return datetime.fromisoformat(iso_str)


class QueueItem:
    """Represents a single message item inside a queue."""

    def __init__(
        self,
        payload: Dict[str, Any],
        priority: int = 0,
        delay_seconds: int = 0,
        id: Optional[str] = None,
        queue_id: Optional[str] = None,
        sequence: int = 0,
        created_at: Optional[str] = None,
        available_at: Optional[str] = None,
        **kwargs: Any,
    ):
        self.id: str = id if id else str(uuid.uuid4())
        self.queue_id: Optional[str] = queue_id
        self.payload: Dict[str, Any] = payload
        self.priority: int = int(priority)
        self.sequence: int = int(sequence)

        if created_at is None:
            now_dt = datetime.now(timezone.utc)
            self.created_at: str = now_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            self.created_at = created_at
            now_dt = parse_iso_utc(self.created_at)

        if available_at is None:
            available_dt = datetime.fromtimestamp(
                now_dt.timestamp() + delay_seconds, tz=timezone.utc
            )
            self.available_at: str = available_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        else:
            self.available_at = available_at

    @property
    def delay(self) -> int:
        c_dt = parse_iso_utc(self.created_at)
        a_dt = parse_iso_utc(self.available_at)
        return max(0, int((a_dt - c_dt).total_seconds()))

    def is_eligible(self, current_time_iso: Optional[str] = None) -> bool:
        """Check if item is eligible to be claimed based on current time."""
        now_dt = (
            parse_iso_utc(current_time_iso)
            if current_time_iso
            else datetime.now(timezone.utc)
        )
        return parse_iso_utc(self.available_at) <= now_dt

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "queue_id": self.queue_id,
            "payload": self.payload,
            "priority": self.priority,
            "sequence": self.sequence,
            "created_at": self.created_at,
            "available_at": self.available_at,
        }

    def __repr__(self) -> str:
        return f"<QueueItem id={self.id} priority={self.priority} seq={self.sequence}>"


class FrankensteinQueue:
    """Base queue definition."""

    def __init__(
        self,
        id: Optional[str] = None,
        name: str = "",
        ordering: str = "fifo",
        priority_enabled: bool = False,
        sequence_counter: int = 0,
        created_at: Optional[str] = None,
    ):
        self.id: str = id if id else str(uuid.uuid4())
        self.name: str = name
        self.ordering: str = ordering.lower()
        self.priority_enabled: bool = priority_enabled
        self.sequence_counter: int = sequence_counter
        self.created_at: str = created_at if created_at else current_iso_utc()
        self.items: Dict[str, QueueItem] = {}

    def push(
        self, payload: Dict[str, Any], priority: int = 0, delay_seconds: int = 0
    ) -> QueueItem:
        self.sequence_counter += 1
        item = QueueItem(
            payload=payload,
            priority=priority,
            delay_seconds=delay_seconds,
            queue_id=self.id,
            sequence=self.sequence_counter,
        )
        self.items[item.id] = item
        return item

    def get_eligible_items(
        self, current_time_iso: Optional[str] = None
    ) -> List[QueueItem]:
        return [
            item for item in self.items.values() if item.is_eligible(current_time_iso)
        ]

    def _sort_eligible(
        self, eligible: List[QueueItem]
    ) -> List[QueueItem]:
        raise NotImplementedError("Must be implemented by subclass")

    def claim(self, current_time_iso: Optional[str] = None) -> Optional[QueueItem]:
        eligible = self.get_eligible_items(current_time_iso)
        if not eligible:
            return None
        sorted_items = self._sort_eligible(eligible)
        chosen = sorted_items[0]
        del self.items[chosen.id]
        return chosen

    def peek(self, current_time_iso: Optional[str] = None) -> Optional[QueueItem]:
        eligible = self.get_eligible_items(current_time_iso)
        if not eligible:
            return None
        sorted_items = self._sort_eligible(eligible)
        return sorted_items[0]

    def delete(self, item_id: str) -> bool:
        if item_id in self.items:
            del self.items[item_id]
            return True
        return False

    def is_empty(self) -> bool:
        return len(self.items) == 0

    def size(self) -> int:
        return len(self.items)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "ordering": self.ordering,
            "priority_enabled": self.priority_enabled,
            "sequence_counter": self.sequence_counter,
            "created_at": self.created_at,
        }


class FifoQueue(FrankensteinQueue):
    """FIFO queue implementation with optional priority support."""

    def __init__(
        self,
        id: Optional[str] = None,
        name: str = "",
        priority_enabled: bool = False,
        sequence_counter: int = 0,
        created_at: Optional[str] = None,
    ):
        super().__init__(
            id=id,
            name=name,
            ordering="fifo",
            priority_enabled=priority_enabled,
            sequence_counter=sequence_counter,
            created_at=created_at,
        )

    def _sort_eligible(self, eligible: List[QueueItem]) -> List[QueueItem]:
        if self.priority_enabled:
            # Sort priority descending, sequence ascending
            return sorted(eligible, key=lambda x: (-x.priority, x.sequence))
        else:
            # Sort sequence ascending
            return sorted(eligible, key=lambda x: x.sequence)


class LifoQueue(FrankensteinQueue):
    """LIFO queue implementation with optional priority support."""

    def __init__(
        self,
        id: Optional[str] = None,
        name: str = "",
        priority_enabled: bool = False,
        sequence_counter: int = 0,
        created_at: Optional[str] = None,
    ):
        super().__init__(
            id=id,
            name=name,
            ordering="lifo",
            priority_enabled=priority_enabled,
            sequence_counter=sequence_counter,
            created_at=created_at,
        )

    def _sort_eligible(self, eligible: List[QueueItem]) -> List[QueueItem]:
        if self.priority_enabled:
            # Sort priority descending, sequence descending
            return sorted(eligible, key=lambda x: (-x.priority, -x.sequence))
        else:
            # Sort sequence descending
            return sorted(eligible, key=lambda x: -x.sequence)


def create_queue_instance(
    id: Optional[str] = None,
    name: str = "",
    ordering: str = "fifo",
    priority_enabled: bool = False,
    sequence_counter: int = 0,
    created_at: Optional[str] = None,
) -> FrankensteinQueue:
    """Factory to create queue instance based on ordering."""
    if ordering.lower() == "fifo":
        return FifoQueue(
            id=id,
            name=name,
            priority_enabled=priority_enabled,
            sequence_counter=sequence_counter,
            created_at=created_at,
        )
    elif ordering.lower() == "lifo":
        return LifoQueue(
            id=id,
            name=name,
            priority_enabled=priority_enabled,
            sequence_counter=sequence_counter,
            created_at=created_at,
        )
    else:
        raise ValueError(f"Unsupported queue ordering: {ordering}")


# Maintain backward compatibility alias
frankensteinQueue = FrankensteinQueue


class TestFifoQueue(unittest.TestCase):
    def setUp(self):
        self.q = FifoQueue(name="test-fifo", priority_enabled=False)

    def test_push_and_size(self):
        self.q.push(payload={"data": "item1"})
        self.q.push(payload={"data": "item2"})
        self.q.push(payload={"data": "item3"})
        self.assertEqual(self.q.size(), 3)

    def test_claim_order_fifo_no_priority(self):
        self.q.push(payload={"data": "item1"}, priority=1)
        self.q.push(payload={"data": "item2"}, priority=10)
        self.q.push(payload={"data": "item3"}, priority=5)

        c1 = self.q.claim()
        self.assertEqual(c1.payload["data"], "item1")
        c2 = self.q.claim()
        self.assertEqual(c2.payload["data"], "item2")
        c3 = self.q.claim()
        self.assertEqual(c3.payload["data"], "item3")
        self.assertIsNone(self.q.claim())

    def test_claim_order_fifo_with_priority(self):
        q = FifoQueue(name="test-fifo-p", priority_enabled=True)
        q.push(payload={"data": "A"}, priority=1)
        q.push(payload={"data": "B"}, priority=10)
        q.push(payload={"data": "C"}, priority=10)
        q.push(payload={"data": "D"}, priority=5)

        # Expected: B, C, D, A
        order = [q.claim().payload["data"] for _ in range(4)]
        self.assertEqual(order, ["B", "C", "D", "A"])

    def test_delete(self):
        i1 = self.q.push(payload={"data": "item1"})
        i2 = self.q.push(payload={"data": "item2"})
        self.assertTrue(self.q.delete(i1.id))
        self.assertFalse(self.q.delete("non-existent-id"))
        self.assertEqual(self.q.size(), 1)
        claimed = self.q.claim()
        self.assertEqual(claimed.id, i2.id)

    def test_delay(self):
        self.q.push(payload={"data": "ready"}, delay_seconds=0)
        self.q.push(payload={"data": "delayed"}, delay_seconds=100)
        c1 = self.q.claim()
        self.assertEqual(c1.payload["data"], "ready")
        self.assertIsNone(self.q.claim())


class TestLifoQueue(unittest.TestCase):
    def setUp(self):
        self.q = LifoQueue(name="test-lifo", priority_enabled=False)

    def test_claim_order_lifo_no_priority(self):
        self.q.push(payload={"data": "item1"})
        self.q.push(payload={"data": "item2"})
        self.q.push(payload={"data": "item3"})

        c1 = self.q.claim()
        self.assertEqual(c1.payload["data"], "item3")
        c2 = self.q.claim()
        self.assertEqual(c2.payload["data"], "item2")
        c3 = self.q.claim()
        self.assertEqual(c3.payload["data"], "item1")
        self.assertIsNone(self.q.claim())

    def test_claim_order_lifo_with_priority(self):
        q = LifoQueue(name="test-lifo-p", priority_enabled=True)
        q.push(payload={"data": "A"}, priority=1)
        q.push(payload={"data": "B"}, priority=10)
        q.push(payload={"data": "C"}, priority=10)
        q.push(payload={"data": "D"}, priority=5)

        # Expected: C, B, D, A
        order = [q.claim().payload["data"] for _ in range(4)]
        self.assertEqual(order, ["C", "B", "D", "A"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
