"""Queue Server FastAPI Application for Queuemaxxing Lite.

Exposes the HTTP API matching queuemaxxing_lite_assignment.pdf using frankenstein_queue.
"""

from __future__ import annotations

import os
import sys

# Ensure root dir is in sys.path
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from app.main import app, service

__all__ = ["app", "service"]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("queue_server:app", host="127.0.0.1", port=8000, reload=True)
