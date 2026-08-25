"""Storage manager for queue state persistence."""

from __future__ import annotations

import json
import os
import tempfile
from typing import Any, Dict, Optional


DEFAULT_DATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "queue_state.json"
)


class Storage:
    """Handles atomic persistent storage for queues and items using local JSON file."""

    def __init__(self, filepath: Optional[str] = None):
        self.filepath = filepath or os.environ.get("QUEUE_STORAGE_PATH", DEFAULT_DATA_PATH)

    def load_state(self) -> Dict[str, Any]:
        """Loads state from the filesystem. Returns empty structure if file does not exist."""
        if not os.path.exists(self.filepath):
            return {"queues": {}, "items": {}}
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
                return {
                    "queues": data.get("queues", {}),
                    "items": data.get("items", {}),
                }
        except (json.JSONDecodeError, OSError):
            return {"queues": {}, "items": {}}

    def save_state(self, state: Dict[str, Any]) -> None:
        """
        Atomically saves state to the state file:
        1. Serialize state to temporary file in the same directory
        2. Flush and fsync the file
        3. Close and atomically replace target state file via os.replace
        """
        dir_name = os.path.dirname(self.filepath)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        temp_fd, temp_path = tempfile.mkstemp(dir=dir_name, prefix="queue_state_", suffix=".tmp")
        try:
            with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
                json.dump(state, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(temp_path, self.filepath)
        except Exception:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            raise
