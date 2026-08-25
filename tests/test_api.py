"""Comprehensive test suite for Queuemaxxing Lite API."""

import asyncio
import os
import tempfile
import pytest
import pytest_asyncio
import httpx
from datetime import datetime, timezone

from app.main import app, service
from app.storage import Storage
from app.queue_service import QueueService


@pytest_asyncio.fixture
async def client(tmp_path):
    """Fixture providing an isolated AsyncClient with temporary storage for each test."""
    temp_file = str(tmp_path / "test_queue_state.json")
    test_storage = Storage(filepath=temp_file)
    test_service = QueueService(storage=test_storage)

    # Monkeypatch the service used in app.main
    import app.main as app_main
    original_service = app_main.service
    app_main.service = test_service

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac, test_service, temp_file

    app_main.service = original_service


# 1. Queue Creation Tests
@pytest.mark.asyncio
async def test_queue_creation_success(client):
    ac, svc, _ = client
    response = await ac.post("/create_queue", json={
        "name": "email-jobs",
        "ordering": "fifo",
        "priority_enabled": True
    })
    assert response.status_code == 201
    data = response.json()
    assert "id" in data
    assert data["name"] == "email-jobs"
    assert data["ordering"] == "fifo"
    assert data["priority_enabled"] is True
    assert "created_at" in data


@pytest.mark.asyncio
async def test_duplicate_queue_name_conflict(client):
    ac, svc, _ = client
    res1 = await ac.post("/create_queue", json={
        "name": "unique-queue",
        "ordering": "fifo"
    })
    assert res1.status_code == 201

    res2 = await ac.post("/create_queue", json={
        "name": "unique-queue",
        "ordering": "lifo"
    })
    assert res2.status_code == 409
    assert "already exists" in res2.json()["detail"]


@pytest.mark.asyncio
async def test_invalid_ordering_validation_error(client):
    ac, svc, _ = client
    response = await ac.post("/create_queue", json={
        "name": "invalid-queue",
        "ordering": "random"
    })
    assert response.status_code == 422


# 2. FIFO and LIFO Ordering Tests
@pytest.mark.asyncio
async def test_fifo_ordering(client):
    ac, svc, _ = client
    # Create FIFO queue (no priority)
    q_res = await ac.post("/create_queue", json={
        "name": "fifo-queue",
        "ordering": "fifo",
        "priority_enabled": False
    })
    q_id = q_res.json()["id"]

    # Push A, B, C
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": "A"}})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": "B"}})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": "C"}})

    # Claims return A, B, C
    c1 = await ac.get(f"/queues/{q_id}/claim")
    assert c1.status_code == 200
    assert c1.json()["payload"]["val"] == "A"

    c2 = await ac.get(f"/queues/{q_id}/claim")
    assert c2.status_code == 200
    assert c2.json()["payload"]["val"] == "B"

    c3 = await ac.get(f"/queues/{q_id}/claim")
    assert c3.status_code == 200
    assert c3.json()["payload"]["val"] == "C"

    # Next claim returns 204 No Content
    c4 = await ac.get(f"/queues/{q_id}/claim")
    assert c4.status_code == 204


@pytest.mark.asyncio
async def test_lifo_ordering(client):
    ac, svc, _ = client
    # Create LIFO queue (no priority)
    q_res = await ac.post("/create_queue", json={
        "name": "lifo-queue",
        "ordering": "lifo",
        "priority_enabled": False
    })
    q_id = q_res.json()["id"]

    # Push A, B, C
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": "A"}})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": "B"}})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": "C"}})

    # Claims return C, B, A
    c1 = await ac.get(f"/queues/{q_id}/claim")
    assert c1.status_code == 200
    assert c1.json()["payload"]["val"] == "C"

    c2 = await ac.get(f"/queues/{q_id}/claim")
    assert c2.status_code == 200
    assert c2.json()["payload"]["val"] == "B"

    c3 = await ac.get(f"/queues/{q_id}/claim")
    assert c3.status_code == 200
    assert c3.json()["payload"]["val"] == "A"

    # Next claim returns 204 No Content
    c4 = await ac.get(f"/queues/{q_id}/claim")
    assert c4.status_code == 204


# 3. Priority Tests
@pytest.mark.asyncio
async def test_priority_enabled_fifo(client):
    ac, svc, _ = client
    q_res = await ac.post("/create_queue", json={
        "name": "priority-fifo-queue",
        "ordering": "fifo",
        "priority_enabled": True
    })
    q_id = q_res.json()["id"]

    # Push A (1), B (10), C (10), D (5)
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "A"}, "priority": 1})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "B"}, "priority": 10})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "C"}, "priority": 10})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "D"}, "priority": 5})

    # Expected order: B (10, seq 2), C (10, seq 3), D (5, seq 4), A (1, seq 1)
    order = []
    for _ in range(4):
        c = await ac.get(f"/queues/{q_id}/claim")
        assert c.status_code == 200
        order.append(c.json()["payload"]["name"])

    assert order == ["B", "C", "D", "A"]


@pytest.mark.asyncio
async def test_priority_enabled_lifo(client):
    ac, svc, _ = client
    q_res = await ac.post("/create_queue", json={
        "name": "priority-lifo-queue",
        "ordering": "lifo",
        "priority_enabled": True
    })
    q_id = q_res.json()["id"]

    # Push A (1), B (10), C (10), D (5)
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "A"}, "priority": 1})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "B"}, "priority": 10})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "C"}, "priority": 10})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"name": "D"}, "priority": 5})

    # Expected order: C (10, seq 3), B (10, seq 2), D (5, seq 4), A (1, seq 1)
    order = []
    for _ in range(4):
        c = await ac.get(f"/queues/{q_id}/claim")
        assert c.status_code == 200
        order.append(c.json()["payload"]["name"])

    assert order == ["C", "B", "D", "A"]


@pytest.mark.asyncio
async def test_priority_disabled_has_no_effect(client):
    ac, svc, _ = client
    q_res = await ac.post("/create_queue", json={
        "name": "priority-disabled-queue",
        "ordering": "fifo",
        "priority_enabled": False
    })
    q_id = q_res.json()["id"]

    # Push 1, 10, 5
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": 1}, "priority": 1})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": 10}, "priority": 10})
    await ac.post(f"/queues/{q_id}/push", json={"payload": {"val": 5}, "priority": 5})

    # FIFO ordering should prevail: 1, 10, 5
    order = []
    for _ in range(3):
        c = await ac.get(f"/queues/{q_id}/claim")
        assert c.status_code == 200
        order.append(c.json()["payload"]["val"])

    assert order == [1, 10, 5]


# 4. Delay Tests
@pytest.mark.asyncio
async def test_delayed_message_availability(client):
    ac, svc, _ = client
    q_res = await ac.post("/create_queue", json={
        "name": "delay-queue",
        "ordering": "fifo"
    })
    q_id = q_res.json()["id"]

    # Push delayed item (5 seconds)
    push_res = await ac.post(f"/queues/{q_id}/push", json={
        "payload": {"task": "delayed_job"},
        "delay_seconds": 5
    })
    assert push_res.status_code == 201
    assert push_res.json()["status"] == "delayed"

    # Immediate claim returns 204 No Content
    claim1 = await ac.get(f"/queues/{q_id}/claim")
    assert claim1.status_code == 204

    # Simulate clock passing using service method with future timestamp
    future_time_iso = "2099-01-01T00:00:00Z"
    claimed = await svc.claim_item(queue_id=q_id, current_time_iso=future_time_iso)
    assert claimed is not None
    assert claimed.payload["task"] == "delayed_job"


# 5. Persistence Tests
@pytest.mark.asyncio
async def test_persistence_across_restart(client):
    ac, svc, state_file = client
    # 1. Create queue
    q_res = await ac.post("/create_queue", json={
        "name": "persist-queue",
        "ordering": "fifo",
        "priority_enabled": True
    })
    q_id = q_res.json()["id"]

    # 2. Push item
    p_res = await ac.post(f"/queues/{q_id}/push", json={
        "payload": {"persisted": "data"},
        "priority": 42
    })
    item_id = p_res.json()["id"]

    # 3. Simulate application restart: instantiate a new QueueService using the same file
    new_storage = Storage(filepath=state_file)
    new_service = QueueService(storage=new_storage)

    # 4. Claim from the new service instance
    claimed = await new_service.claim_item(queue_id=q_id)
    assert claimed is not None
    assert claimed.id == item_id
    assert claimed.payload["persisted"] == "data"
    assert claimed.priority == 42


# 6. Concurrency Tests
@pytest.mark.asyncio
async def test_concurrent_claims(client):
    ac, svc, _ = client
    q_res = await ac.post("/create_queue", json={
        "name": "concurrency-queue",
        "ordering": "fifo"
    })
    q_id = q_res.json()["id"]

    num_items = 100
    # Push 100 items
    for i in range(num_items):
        await ac.post(f"/queues/{q_id}/push", json={"payload": {"idx": i}})

    # Concurrently issue 120 claim requests
    async def make_claim():
        return await ac.get(f"/queues/{q_id}/claim")

    tasks = [make_claim() for _ in range(120)]
    responses = await asyncio.gather(*tasks)

    claimed_indices = []
    no_content_count = 0

    for res in responses:
        if res.status_code == 200:
            claimed_indices.append(res.json()["payload"]["idx"])
        elif res.status_code == 204:
            no_content_count += 1

    # Exactly 100 items claimed, each unique
    assert len(claimed_indices) == num_items
    assert len(set(claimed_indices)) == num_items
    assert no_content_count == 20


# 7. Delete Item Tests
@pytest.mark.asyncio
async def test_delete_item(client):
    ac, svc, _ = client
    q_res = await ac.post("/create_queue", json={
        "name": "delete-queue",
        "ordering": "fifo"
    })
    q_id = q_res.json()["id"]

    p1 = await ac.post(f"/queues/{q_id}/push", json={"payload": {"item": 1}})
    p2 = await ac.post(f"/queues/{q_id}/push", json={"payload": {"item": 2}})

    item1_id = p1.json()["id"]
    item2_id = p2.json()["id"]

    # Delete item 1
    del_res = await ac.delete(f"/queues/{q_id}/{item1_id}")
    assert del_res.status_code == 204

    # Item 1 deleted, claiming should return item 2
    claim_res = await ac.get(f"/queues/{q_id}/claim")
    assert claim_res.status_code == 200
    assert claim_res.json()["id"] == item2_id

    # Trying to delete already deleted item returns 404
    del_again = await ac.delete(f"/queues/{q_id}/{item1_id}")
    assert del_again.status_code == 404
