# Script Control:
# - Role: Weekly background checker for new DTE downloads from SII.
# - Track file: docs/SCRIPT_CONTROL.md
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv

from app.core.DTE_Recibidos.pipeline_guard import acquire_pipeline_lock, release_pipeline_lock

BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data"
ENV_PATH = DATA_DIR / "config.env"
STATE_PATH = DATA_DIR / "auto_weekly_dte_check_state.json"
LOG_PATH = DATA_DIR / "logs" / "auto_weekly_dte_check.log"

_STARTED = False
_START_LOCK = threading.Lock()


def _now_local() -> datetime:
    return datetime.now().astimezone()


def _to_iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds")


def _from_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _load_env() -> None:
    load_dotenv(str(ENV_PATH), override=False)


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, min_value: int, max_value: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except Exception:
        return default
    return max(min_value, min(max_value, value))


def _append_log(message: str) -> None:
    try:
        LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"[{_to_iso(_now_local())}] {message}\n")
    except Exception:
        pass


def _load_state() -> dict:
    if not STATE_PATH.exists():
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _resolve_pdf_dir() -> str | None:
    raw = (os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "").strip().strip('"').strip("'")
    if not raw:
        return None
    p = Path(raw)
    if not p.is_absolute():
        p = (BASE_DIR / p).resolve()
    if not p.exists() or not p.is_dir():
        return None
    return str(p)


def _next_due(last_success: datetime | None, interval_days: int) -> datetime:
    if not last_success:
        return _now_local()
    return last_success + timedelta(days=interval_days)


def _should_run_now(
    state: dict,
    interval_days: int,
    retry_hours: int,
) -> bool:
    now = _now_local()
    last_success = _from_iso(state.get("last_success_at"))
    if last_success and now < _next_due(last_success, interval_days):
        return False

    last_attempt = _from_iso(state.get("last_attempt_at"))
    if last_attempt and now < (last_attempt + timedelta(hours=retry_hours)):
        return False
    return True


def _run_weekly_check_once() -> None:
    state = _load_state()
    state["last_attempt_at"] = _to_iso(_now_local())
    state["last_status"] = "running"
    state["last_error"] = ""
    _save_state(state)

    if not acquire_pipeline_lock(blocking=False):
        _append_log("Chequeo semanal omitido: hay otro proceso DTE en ejecucion.")
        return

    try:
        _load_env()
        pdf_dir = _resolve_pdf_dir()
        if not pdf_dir:
            state = _load_state()
            state["last_status"] = "skipped"
            state["last_error"] = "Ruta RUTA_PDF_DTE_RECIBIDOS no configurada o inexistente."
            _save_state(state)
            _append_log("Chequeo semanal omitido: ruta de PDFs no disponible.")
            return

        from app.core.DTE_Recibidos.Scrap import scrapear

        _append_log("Iniciando chequeo semanal automatico de DTE en SII.")
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(f"\n--- Inicio scrap semanal [{_to_iso(_now_local())}] ---\n")
            with redirect_stdout(f), redirect_stderr(f):
                incompletas = scrapear(pdf_dir, progress_cb=None) or []
            f.write(f"--- Fin scrap semanal [{_to_iso(_now_local())}] ---\n")

        state = _load_state()
        state["last_status"] = "ok" if not incompletas else "partial"
        state["last_error"] = ""
        state["last_success_at"] = _to_iso(_now_local())
        state["incomplete_downloads"] = int(len(incompletas))
        _save_state(state)
        _append_log(
            f"Chequeo semanal finalizado. incompletas={len(incompletas)} status={state['last_status']}"
        )
    except Exception as e:
        state = _load_state()
        state["last_status"] = "error"
        state["last_error"] = str(e)
        _save_state(state)
        _append_log(f"Chequeo semanal con error: {e}")
    finally:
        release_pipeline_lock()


def _scheduler_loop() -> None:
    _append_log("Scheduler semanal DTE iniciado.")
    while True:
        try:
            _load_env()
            enabled = _env_bool("AUTO_WEEKLY_DTE_CHECK_ENABLED", True)
            interval_days = _env_int("AUTO_WEEKLY_DTE_CHECK_INTERVAL_DAYS", 7, 1, 30)
            retry_hours = _env_int("AUTO_WEEKLY_DTE_CHECK_RETRY_HOURS", 6, 1, 48)
            poll_minutes = _env_int("AUTO_WEEKLY_DTE_CHECK_POLL_MINUTES", 30, 5, 1440)

            if enabled:
                state = _load_state()
                if _should_run_now(state, interval_days=interval_days, retry_hours=retry_hours):
                    _run_weekly_check_once()
        except Exception as e:
            _append_log(f"Error en scheduler semanal DTE: {e}")
            poll_minutes = _env_int("AUTO_WEEKLY_DTE_CHECK_POLL_MINUTES", 30, 5, 1440)

        time.sleep(max(60, poll_minutes * 60))


def start_weekly_checker() -> bool:
    """
    Starts one daemon thread for weekly checks.
    Returns True only when started in this call.
    """
    global _STARTED
    with _START_LOCK:
        if _STARTED:
            return False
        _STARTED = True

    t = threading.Thread(
        target=_scheduler_loop,
        name="weekly-dte-checker",
        daemon=True,
    )
    t.start()
    return True
