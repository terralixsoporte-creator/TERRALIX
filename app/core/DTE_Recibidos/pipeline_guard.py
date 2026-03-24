# Script Control:
# - Role: Shared lock to avoid concurrent DTE pipelines.
# - Track file: docs/SCRIPT_CONTROL.md
import threading

_PIPELINE_LOCK = threading.Lock()


def acquire_pipeline_lock(blocking: bool = True, timeout: float = -1) -> bool:
    """Try to acquire the shared DTE pipeline lock."""
    if timeout is None or timeout < 0:
        return _PIPELINE_LOCK.acquire(blocking=blocking)
    return _PIPELINE_LOCK.acquire(blocking=blocking, timeout=timeout)


def release_pipeline_lock() -> None:
    """Release shared lock when owned by current flow."""
    if _PIPELINE_LOCK.locked():
        _PIPELINE_LOCK.release()


def pipeline_is_busy() -> bool:
    return _PIPELINE_LOCK.locked()
