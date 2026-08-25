# Queuemaxxing Lite

A high-correctness, persistent, concurrency-safe HTTP queue service built with **Python** and **FastAPI**.

---

## Features

- **Configurable Ordering**: Supports FIFO and LIFO queue modes with optional priority ordering.
- **Delayed Availability**: Support for `delay_seconds` where items only become eligible once their availability timestamp has passed.
- **Crash-Resilient Persistence**: Atomic file writes (`flush` + `fsync` + `os.replace`) to local filesystem JSON storage.
- **Concurrency-Safe**: Coordinated with asynchronous locks (`asyncio.Lock`) guaranteeing atomic claim operations (at most once delivery per claim).
- **Zero External Database**: Pure standard library + FastAPI persistence.

---

## Project Structure

```
.
├── app/
│   ├── __init__.py
│   ├── main.py              # FastAPI application & endpoint routes
│   ├── models.py            # Pydantic schemas for requests and responses
│   ├── queue_service.py     # State management, concurrency locks & orchestration
│   └── storage.py           # Atomic JSON filesystem storage manager
├── data/
│   ├── .gitkeep
│   └── queue_state.json     # Persisted queues and items state
├── tests/
│   ├── __init__.py
│   └── test_api.py          # Comprehensive test suite (FIFO, LIFO, Priority, Delay, Persistence, Concurrency)
├── frankenstein_queue.py    # Core Queue and Item data structures & standalone tests
├── queue_server.py          # Server application entrypoint
├── requirements.txt         # Project dependencies
└── README.md                # Documentation & usage guide
```

---

## Installation and Startup Instructions

### 1. Prerequisites
- Python 3.11+
- Virtual environment (recommended)

### 2. Setup Virtual Environment
```bash
python -m venv .venv

# On Linux / macOS:
source .venv/bin/activate

# On Windows (PowerShell):
.venv\Scripts\Activate.ps1
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the Service
Run with Uvicorn:
```bash
uvicorn app.main:app --reload
```
Or alternatively via `queue_server.py`:
```bash
uvicorn queue_server:app --reload
# or
python queue_server.py
```

The service will start at `http://127.0.0.1:8000`.
Interactive Swagger API documentation is available at:
- **Swagger UI**: `http://127.0.0.1:8000/docs`
- **ReDoc**: `http://127.0.0.1:8000/redoc`

### 5. Running Tests
```bash
pytest -v
```
Or run the unit tests in `frankenstein_queue.py`:
```bash
python frankenstein_queue.py
```

---

## HTTP API & Example curl Commands

### 1. Create Queue
**`POST /create_queue`** (Status: `201 Created` / `409 Conflict` / `422 Unprocessable Entity`)

Creates a new queue with FIFO or LIFO ordering and optional priority support.

```bash
curl -X POST "http://127.0.0.1:8000/create_queue" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "email-jobs",
    "ordering": "fifo",
    "priority_enabled": true
  }'
```

*Response `201 Created`:*
```json
{
  "id": "0f9e1d95-46f3-4857-a5a0-c3b437d73f1b",
  "name": "email-jobs",
  "ordering": "fifo",
  "priority_enabled": true,
  "created_at": "2026-08-21T01:16:00Z"
}
```

---

### 2. Push Item
**`POST /queues/{id}/push`** (Status: `201 Created` / `404 Not Found`)

Adds a new item to the queue. `delay_seconds` makes the item invisible to claims until `created_at + delay_seconds`.

```bash
curl -X POST "http://127.0.0.1:8000/queues/0f9e1d95-46f3-4857-a5a0-c3b437d73f1b/push" \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {
      "task": "send_email",
      "recipient": "user@example.com"
    },
    "priority": 10,
    "delay_seconds": 5
  }'
```

*Response `201 Created`:*
```json
{
  "id": "9bc6704d-d452-4ec2-8c65-5146b12f0095",
  "queue_id": "0f9e1d95-46f3-4857-a5a0-c3b437d73f1b",
  "priority": 10,
  "created_at": "2026-08-21T01:16:00Z",
  "available_at": "2026-08-21T01:16:05Z",
  "status": "delayed"
}
```

---

### 3. Claim Item
**`GET /queues/{id}/claim`** (Status: `200 OK` / `204 No Content` / `404 Not Found`)

Atomically retrieves and removes the next eligible item according to the queue's ordering rules.

```bash
curl -X GET "http://127.0.0.1:8000/queues/0f9e1d95-46f3-4857-a5a0-c3b437d73f1b/claim"
```

*Response `200 OK` (when item available):*
```json
{
  "id": "9bc6704d-d452-4ec2-8c65-5146b12f0095",
  "queue_id": "0f9e1d95-46f3-4857-a5a0-c3b437d73f1b",
  "payload": {
    "task": "send_email",
    "recipient": "user@example.com"
  },
  "priority": 10,
  "sequence": 1,
  "created_at": "2026-08-21T01:16:00Z",
  "available_at": "2026-08-21T01:16:05Z"
}
```

*Response `204 No Content` (when no eligible items ready):*
No response body.

---

### 4. Delete Item
**`DELETE /queues/{id}/{queue_item_id}`** (Status: `204 No Content` / `404 Not Found`)

Removes a specific item from a queue regardless of whether it is ready or delayed.

```bash
curl -X DELETE "http://127.0.0.1:8000/queues/0f9e1d95-46f3-4857-a5a0-c3b437d73f1b/9bc6704d-d452-4ec2-8c65-5146b12f0095"
```

*Response `204 No Content`:*
No response body.

---

## Queue Ordering Semantics

Each queue maintains an internal sequence counter that monotonically increments upon every item push.

1. **Eligibility Filter**:
   Only items where `available_at <= current_time` are eligible for claiming.

2. **Ordering Policies**:
   - **FIFO (No Priority)**: Claims order by lowest `sequence` first (`sequence` ascending).
   - **LIFO (No Priority)**: Claims order by highest `sequence` first (`sequence` descending).
   - **Priority FIFO (`priority_enabled: true`)**: Claims order by `priority` descending, then `sequence` ascending.
   - **Priority LIFO (`priority_enabled: true`)**: Claims order by `priority` descending, then `sequence` descending.

**Example**:
Suppose items are pushed in order:
- Item `A`: `priority = 1`, `sequence = 1`
- Item `B`: `priority = 10`, `sequence = 2`
- Item `C`: `priority = 10`, `sequence = 3`
- Item `D`: `priority = 5`, `sequence = 4`

Claim order:
- **Priority FIFO**: `B` (10, seq 2) $\rightarrow$ `C` (10, seq 3) $\rightarrow$ `D` (5, seq 4) $\rightarrow$ `A` (1, seq 1)
- **Priority LIFO**: `C` (10, seq 3) $\rightarrow$ `B` (10, seq 2) $\rightarrow$ `D` (5, seq 4) $\rightarrow$ `A` (1, seq 1)

---

## Persistence Strategy

The system persists all queue definitions, sequence counters, and items in a single JSON state file (`data/queue_state.json`).

### Atomic Write Process:
To guarantee that a crash or power failure mid-write never corrupts the state:
1. State mutation lock is acquired.
2. In-memory queue state is updated.
3. State is serialized to a unique temporary file (`tempfile.mkstemp`) located within the same directory.
4. The temporary file buffer is flushed (`f.flush()`) and synced to physical storage (`os.fsync(f.fileno())`).
5. The temporary file is atomically renamed over the state file using `os.replace()`.
6. Lock is released and HTTP response is sent.

On server startup, the state file is parsed and in-memory structures are reconstructed.

---

## Concurrency Strategy

The service ensures linearizability and race-condition freedom across concurrent HTTP requests using `asyncio.Lock()`:

- **State Lock**: A shared `asyncio.Lock` protects critical sections for `/create_queue`, `/queues/{id}/push`, `/queues/{id}/claim`, and `/queues/{id}/{queue_item_id}`.
- **Atomic Claims**: When multiple clients issue concurrent `GET /claim` requests:
  - The request acquires the lock.
  - Eligible items are evaluated and the winning item is selected according to the queue policy.
  - The item is removed from the queue and state is persisted to disk.
  - Lock is released.
  - **Result**: Exactly one consumer receives any given item; no duplicates are ever served.

---

## Delivery Semantics (At-Most-Once)

Because the API does not include acknowledgment (`ack`/`nack`), lease visibility timeouts, or message dead-letter queues:
- An item is deleted from state as soon as `GET /claim` selects it and responds.
- If a consumer crashes while processing the claimed item or network failure occurs during response transmission, the item is not retried.
- This represents **at-most-once** delivery semantics by design.

---

## Known Limitations

- **Single Process / Worker**: Because in-memory locking and local filesystem replacement are used, horizontal scaling across multiple independent nodes or worker processes requires an external coordination mechanism or distributed lock.
- **In-Memory Scale**: All active queues and items are stored in memory and serialized to a single JSON file; suitable for small-to-medium queue volumes rather than millions of long-lived items.
- **No Message Replay / Visibility Timeouts**: Items cannot be released back to the queue if processing fails.


Co: Kora Agent @ Versys Research [backbone: Gemma4-e2b-q4
]