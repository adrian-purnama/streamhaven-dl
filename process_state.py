"""
Process state for the downloader. Single source of truth for job phase and progress.
Use from main.py or elsewhere: get_state(), set_state(), clear_state(), schedule_revert_to_idle().
Backend can GET /progress to read the same state.
"""
import os
import time
from threading import Lock, Thread

# -----------------------------------------------------------------------------
# State
# -----------------------------------------------------------------------------

_lock = Lock()
_state: dict = {
    "phase": "idle",  # idle | searching | downloading | uploading | done | failed
    "jobId": None,
    "explanation": None,  # progress explanation (changes as phases progress)
    "download": {},
    "upload": {},
    "updatedAt": None,
    "snifferResult": None,  # last run_sniffer result (used by sniffer_server)
}

# Default seconds to wait after done/failed before reverting to idle (caller can override via delay_sec)
REVERT_TO_IDLE_DELAY_SEC = int(os.environ.get("IDLE_REVERT_DELAY_SEC", "60"))


def get_state() -> dict:
    """Thread-safe read of current process state. Safe to call from any thread or another module."""
    with _lock:
        return {
            "jobId": _state.get("jobId"),
            "phase": _state.get("phase", "idle"),
            "explanation": _state.get("explanation"),
            "download": dict(_state.get("download") or {}),
            "upload": dict(_state.get("upload") or {}),
            "updatedAt": _state.get("updatedAt"),
            "snifferResult": _state.get("snifferResult"),
        }


def set_state(**kwargs: object) -> None:
    """Update process state. Merges dicts for 'download' and 'upload'; sets others as-is."""
    with _lock:
        _state["updatedAt"] = time.time()
        for k, v in kwargs.items():
            if v is None and k not in ("explanation",):
                continue
            if k == "download" and isinstance(v, dict):
                _state.setdefault("download", {}).update(v)
            elif k == "upload" and isinstance(v, dict):
                _state.setdefault("upload", {}).update(v)
            else:
                _state[k] = v


def clear_state() -> None:
    """Reset state to idle. Call at start of a new job."""
    with _lock:
        _state.clear()
        _state["phase"] = "idle"
        _state["explanation"] = None
        _state["snifferResult"] = None
        _state["updatedAt"] = time.time()


def schedule_revert_to_idle(delay_sec: int | None = None, log_fn=None) -> None:
    """After delay_sec, if phase is still done/failed, set phase to idle. Runs in a background thread."""
    delay = delay_sec if delay_sec is not None else REVERT_TO_IDLE_DELAY_SEC

    def _revert() -> None:
        time.sleep(delay)
        with _lock:
            if _state.get("phase") in ("done", "failed"):
                _state.clear()
                _state["phase"] = "idle"
                _state["updatedAt"] = time.time()
                if log_fn:
                    log_fn("Reverted process state to idle.")

    Thread(target=_revert, daemon=True).start()
