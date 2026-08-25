"""FastAPI application entrypoint for Queuemaxxing Lite."""

from __future__ import annotations

import os
import sys
from contextlib import asynccontextmanager
from typing import Optional, Union

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from fastapi import FastAPI, HTTPException, Response, status
from app.models import (
    ClaimItemResponse,
    CreateQueueRequest,
    PushItemRequest,
    PushItemResponse,
    QueueResponse,
)
from app.queue_service import (
    DuplicateQueueNameError,
    QueueItemNotFoundError,
    QueueNotFoundError,
    QueueService,
)

service = QueueService()

# Load the queue state from storage when the application starts
@asynccontextmanager
async def lifespan(app: FastAPI):
    service.load_from_storage()
    yield

app = FastAPI(
    title="Queuemaxxing Lite",
    description="HTTP-based queue service prioritizing correctness, persistence, and concurrency safety.",
    version="1.0.0",
    lifespan=lifespan,
)

#POST API
# <endpoint>/create_queue
# create a new queue with the given name and type (FIFO or LIFO)
@app.post(
    "/create_queue",
    response_model=QueueResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new queue",
)
async def create_queue(req: CreateQueueRequest) -> QueueResponse:
    try:
        return await service.create_queue(req)
    except DuplicateQueueNameError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(e),
        )


#POST API
# <endpoint>/queues/{id}/push
# Add new item to queue with id {id}
@app.post(
    "/queues/{id}/push",
    response_model=PushItemResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add an item to a queue",
)
async def push_item(id: str, req: PushItemRequest) -> PushItemResponse:
    try:
        return await service.push_item(queue_id=id, req=req)
    except QueueNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

#GET API
# <endpoint>/queues/{id}/claim
# Claim new item from queue with id {id}, removes the item from the queue and returns it to the caller
@app.get(
    "/queues/{id}/claim",
    response_model=ClaimItemResponse,
    responses={
        200: {"description": "Item claimed successfully", "model": ClaimItemResponse},
        204: {"description": "No eligible item available in queue"},
        404: {"description": "Queue not found"},
    },
    summary="Atomically retrieve and remove the next eligible item",
)
async def claim_item(id: str) -> Union[ClaimItemResponse, Response]:
    try:
        claimed = await service.claim_item(queue_id=id)
        if claimed is None:
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        return claimed
    except QueueNotFoundError as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )

#DELETE API
# <endpoint>/queues/{id}/{queue_item_id}
# Delete item with id {queue_item_id} from queue with id {id}
@app.delete(
    "/queues/{id}/{queue_item_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    responses={
        204: {"description": "Item successfully deleted"},
        404: {"description": "Queue or item not found"},
    },
    summary="Remove a specific item from a queue",
)
async def delete_item(id: str, queue_item_id: str) -> Response:
    try:
        await service.delete_item(queue_id=id, item_id=queue_item_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except (QueueNotFoundError, QueueItemNotFoundError) as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e),
        )
