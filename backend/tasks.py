"""In-memory background-task registry.

Surfaces ongoing work to the frontend so the user can see what the server
is doing — comparison fetches, semantic-model loads, image hydration,
re-scoring, individual benchmark runs.

Tasks have a stable id, a kind (rescore | compare | hydrate | semantic |
run | other), a human label, an integer progress / total when known,
status (pending | running | completed | failed | cancelled), an optional
error message, and timestamps. They live for ~5 minutes after completion
so the UI has time to show the final state, then are evicted.

Cancellation is cooperative: the registered work checks
``is_cancelled(task_id)`` periodically and bails out cleanly. The runner
already has its own RUN_CONTROLS dict — runs are surfaced to /api/tasks
read-only (no double cancel path).
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Optional

# task_id -> dict
_TASKS: dict[str, dict] = {}
_LOCK = threading.Lock()
_TTL_AFTER_DONE = 300.0  # seconds — keep finished tasks visible briefly


def _evict_expired() -> None:
    now = time.time()
    with _LOCK:
        dead = [
            tid for tid, t in _TASKS.items()
            if t.get("status") in ("completed", "failed", "cancelled")
            and t.get("finished_at") and (now - t["finished_at"]) > _TTL_AFTER_DONE
        ]
        for tid in dead:
            _TASKS.pop(tid, None)


def register(kind: str, label: str, total: Optional[int] = None,
             meta: Optional[dict] = None) -> str:
    """Create a new task. Returns the task id (caller passes to update/complete)."""
    tid = uuid.uuid4().hex[:12]
    with _LOCK:
        _TASKS[tid] = {
            "id": tid,
            "kind": kind,
            "label": label,
            "status": "running",
            "progress": 0,
            "total": total,
            "started_at": time.time(),
            "finished_at": None,
            "error": None,
            "cancel_requested": False,
            "meta": meta or {},
        }
    return tid


def update(task_id: str, progress: Optional[int] = None,
           label: Optional[str] = None, total: Optional[int] = None,
           meta: Optional[dict] = None) -> None:
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t:
            return
        if progress is not None: t["progress"] = progress
        if label is not None:    t["label"] = label
        if total is not None:    t["total"] = total
        if meta:                 t["meta"].update(meta)


def complete(task_id: str, label: Optional[str] = None) -> None:
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t: return
        t["status"] = "completed"
        t["finished_at"] = time.time()
        if t.get("total") is not None:
            t["progress"] = t["total"]
        if label is not None:
            t["label"] = label


def fail(task_id: str, error: str) -> None:
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t: return
        t["status"] = "failed"
        t["finished_at"] = time.time()
        t["error"] = str(error)


def cancel(task_id: str) -> bool:
    """Request cancellation. Returns True if the task existed."""
    with _LOCK:
        t = _TASKS.get(task_id)
        if not t: return False
        if t["status"] in ("completed", "failed", "cancelled"):
            return False
        t["cancel_requested"] = True
        # Mark as cancelled too — long-running work will exit on next check.
        t["status"] = "cancelled"
        t["finished_at"] = time.time()
        return True


def is_cancelled(task_id: str) -> bool:
    with _LOCK:
        t = _TASKS.get(task_id)
        return bool(t and t.get("cancel_requested"))


def list_active() -> list[dict]:
    """All tasks (active + recently finished). Newest first."""
    _evict_expired()
    with _LOCK:
        items = list(_TASKS.values())
    items.sort(key=lambda t: t.get("started_at", 0), reverse=True)
    return items
