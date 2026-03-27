# Script Control:
# - Role: Local SQLite outbox sync to Supabase + optional PDF mirror to Google Drive folder.

from __future__ import annotations

import json
import os
import re
import shutil
import sqlite3
import threading
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

from app.core.paths import get_env_path

load_dotenv(str(get_env_path()), override=False)

_SYNC_LOCK = threading.Lock()
_DEFAULT_SYNC_TABLES = (
    "catalogo_costos",
    "documentos",
    "detalle",
    "inventario_catalogo",
    "inventario_movimientos",
    "aplicaciones_campo",
    "aplicaciones_campo_productos",
    "mantenedor_categoria_proveedor",
    "mantenedor_keyword_categoria",
)


def _clean_path(value: str) -> str:
    return (value or "").strip().strip('"').strip("'")


def _to_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    raw = str(value).strip().lower()
    if raw in {"1", "true", "yes", "y", "si", "on"}:
        return True
    if raw in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _to_int(value: Any, default: int, min_value: Optional[int] = None, max_value: Optional[int] = None) -> int:
    try:
        out = int(str(value).strip())
    except Exception:
        out = default
    if min_value is not None:
        out = max(min_value, out)
    if max_value is not None:
        out = min(max_value, out)
    return out


def _is_sync_enabled() -> bool:
    return _to_bool(os.getenv("SUPABASE_SYNC_ENABLED", "true"), default=True)


def _sync_tables() -> List[str]:
    raw = (os.getenv("SUPABASE_SYNC_TABLES", "") or "").strip()
    if not raw:
        return list(_DEFAULT_SYNC_TABLES)
    items = [x.strip() for x in raw.split(",")]
    return [x for x in items if x]


def _normalize_table_scope(tables: Optional[Iterable[str]]) -> List[str]:
    if not tables:
        return []
    out: List[str] = []
    seen = set()
    for raw in tables:
        name = str(raw or "").strip()
        if not name or name in seen:
            continue
        seen.add(name)
        out.append(name)
    return out


def _resolve_supabase_auth_key() -> str:
    # Prefer service role when available to avoid RLS issues during sync.
    service_role = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if service_role:
        return service_role
    return (os.getenv("SUPABASE_ANON_KEY") or "").strip()


def _supabase_config() -> Tuple[str, str]:
    url = (os.getenv("SUPABASE_URL") or "").strip().rstrip("/")
    key = _resolve_supabase_auth_key()
    if not url or not key:
        raise RuntimeError("SUPABASE_URL/SUPABASE_KEY no configurados para sincronizacion.")
    return url, key


def _headers(key: str) -> Dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates,return=minimal",
    }


def resolve_db_path(db_path: Optional[str] = None) -> str:
    if db_path:
        resolved = _clean_path(db_path)
        if resolved:
            return resolved
    resolved = _clean_path(os.getenv("DB_PATH_DTE_RECIBIDOS", ""))
    if not resolved:
        raise ValueError("No se encontro DB_PATH_DTE_RECIBIDOS para sincronizacion.")
    return resolved


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    con.execute("PRAGMA busy_timeout=60000;")
    return con


def _q(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _pk_columns(con: sqlite3.Connection, table: str) -> List[str]:
    rows = con.execute(f"PRAGMA table_info({_q(table)});").fetchall()
    pk_rows = [r for r in rows if int(r["pk"] or 0) > 0]
    pk_rows.sort(key=lambda r: int(r["pk"]))
    return [str(r["name"]) for r in pk_rows]


def _trigger_safe_name(table: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in table.lower())


def _json_obj_expr(alias: str, pk_cols: Iterable[str]) -> str:
    parts: List[str] = []
    for col in pk_cols:
        parts.append(f"'{col}'")
        parts.append(f'{alias}.{_q(col)}')
    return f"json_object({', '.join(parts)})"


def _ensure_queue_schema(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS supabase_sync_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT NOT NULL,
            op TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
            pk_json TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            processed_at TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            UNIQUE(table_name, pk_json)
        )
        """
    )
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_supasync_pending
        ON supabase_sync_queue(processed_at, created_at, id)
        """
    )


def _ensure_table_triggers(con: sqlite3.Connection, table: str, pk_cols: List[str]) -> None:
    if not pk_cols:
        return

    table_q = _q(table)
    safe_table = _trigger_safe_name(table)
    new_pk_json = _json_obj_expr("NEW", pk_cols)
    old_pk_json = _json_obj_expr("OLD", pk_cols)

    trigger_specs = (
        ("ai", "AFTER INSERT", "upsert", new_pk_json),
        ("au", "AFTER UPDATE", "upsert", new_pk_json),
        ("ad", "AFTER DELETE", "delete", old_pk_json),
    )

    for suffix, timing, op, pk_expr in trigger_specs:
        trigger_name = _q(f"trg_supasync_{safe_table}_{suffix}")
        con.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS {trigger_name}
            {timing} ON {table_q}
            BEGIN
                INSERT INTO supabase_sync_queue (table_name, op, pk_json, created_at, processed_at, attempts, last_error)
                VALUES ('{table}', '{op}', {pk_expr}, datetime('now'), NULL, 0, NULL)
                ON CONFLICT(table_name, pk_json) DO UPDATE SET
                    op = excluded.op,
                    created_at = datetime('now'),
                    processed_at = NULL,
                    attempts = 0,
                    last_error = NULL;
            END
            """
        )


def initialize_supabase_sync(
    db_path: Optional[str] = None,
    *,
    log=print,
) -> Dict[str, Any]:
    if not _is_sync_enabled():
        return {"ok": True, "enabled": False, "reason": "SUPABASE_SYNC_ENABLED=false"}

    try:
        resolved_db = resolve_db_path(db_path)
    except Exception as e:
        return {"ok": False, "enabled": True, "error": str(e)}

    created_for_tables = 0
    skipped_tables: List[str] = []
    no_pk_tables: List[str] = []

    with _SYNC_LOCK:
        try:
            con = _connect(resolved_db)
            try:
                _ensure_queue_schema(con)

                for table in _sync_tables():
                    if not _table_exists(con, table):
                        skipped_tables.append(table)
                        continue
                    pk_cols = _pk_columns(con, table)
                    if not pk_cols:
                        no_pk_tables.append(table)
                        continue
                    _ensure_table_triggers(con, table, pk_cols)
                    created_for_tables += 1

                con.commit()
            finally:
                con.close()
        except Exception as e:
            return {"ok": False, "enabled": True, "error": f"init_sync_failed: {e}"}

    if no_pk_tables:
        log(f"[SYNC][WARN] Tablas sin PK omitidas: {', '.join(no_pk_tables)}")
    return {
        "ok": True,
        "enabled": True,
        "db_path": resolved_db,
        "tables_with_triggers": created_for_tables,
        "tables_missing": skipped_tables,
        "tables_without_pk": no_pk_tables,
    }


def _pending_rows(
    con: sqlite3.Connection,
    max_items: int,
    *,
    tables: Optional[List[str]] = None,
) -> List[sqlite3.Row]:
    scoped_tables = _normalize_table_scope(tables)
    if not scoped_tables:
        return con.execute(
            """
            SELECT id, table_name, op, pk_json, attempts, created_at
            FROM supabase_sync_queue
            WHERE processed_at IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            (max_items,),
        ).fetchall()

    placeholders = ",".join("?" for _ in scoped_tables)
    params = tuple(scoped_tables) + (max_items,)
    return con.execute(
        f"""
        SELECT id, table_name, op, pk_json, attempts, created_at
        FROM supabase_sync_queue
        WHERE processed_at IS NULL
          AND table_name IN ({placeholders})
        ORDER BY id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()


def _pending_count(
    con: sqlite3.Connection,
    *,
    tables: Optional[List[str]] = None,
) -> int:
    scoped_tables = _normalize_table_scope(tables)
    if not scoped_tables:
        row = con.execute(
            "SELECT COUNT(*) AS n FROM supabase_sync_queue WHERE processed_at IS NULL"
        ).fetchone()
        return int(row["n"] or 0) if row else 0

    placeholders = ",".join("?" for _ in scoped_tables)
    row = con.execute(
        f"SELECT COUNT(*) AS n FROM supabase_sync_queue WHERE processed_at IS NULL AND table_name IN ({placeholders})",
        tuple(scoped_tables),
    ).fetchone()
    return int(row["n"] or 0) if row else 0


def _mark_row_success(con: sqlite3.Connection, row_id: int, created_at: str, attempts: int) -> None:
    con.execute(
        """
        UPDATE supabase_sync_queue
        SET processed_at = datetime('now'),
            attempts = ?,
            last_error = NULL
        WHERE id = ? AND created_at = ?
        """,
        (attempts, row_id, created_at),
    )


def _mark_row_failure(
    con: sqlite3.Connection,
    row_id: int,
    created_at: str,
    attempts: int,
    error_text: str,
    *,
    drop: bool,
) -> None:
    processed_at = "datetime('now')" if drop else "NULL"
    con.execute(
        f"""
        UPDATE supabase_sync_queue
        SET processed_at = {processed_at},
            attempts = ?,
            last_error = ?
        WHERE id = ? AND created_at = ?
        """,
        (attempts, (error_text or "")[:1000], row_id, created_at),
    )


def _json_load(value: str) -> Dict[str, Any]:
    out = json.loads(value or "{}")
    if not isinstance(out, dict):
        return {}
    return out


def _pk_filter_values(pk_map: Dict[str, Any], pk_cols: Iterable[str]) -> List[Any]:
    return [pk_map.get(col) for col in pk_cols]


def _fetch_local_row(
    con: sqlite3.Connection,
    table: str,
    pk_cols: List[str],
    pk_map: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    where = " AND ".join([f"{_q(col)} = ?" for col in pk_cols])
    sql = f"SELECT * FROM {_q(table)} WHERE {where} LIMIT 1"
    vals = _pk_filter_values(pk_map, pk_cols)
    row = con.execute(sql, vals).fetchone()
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def _request_timeout() -> int:
    return _to_int(os.getenv("SUPABASE_SYNC_TIMEOUT_SECONDS", "25"), default=25, min_value=5, max_value=120)


def _serialize_value(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except Exception:
            return value.hex()
    return str(value)


def _looks_like_url(value: str) -> bool:
    v = (value or "").strip().lower()
    return v.startswith("http://") or v.startswith("https://")


def _looks_like_local_path(value: str) -> bool:
    v = (value or "").strip()
    if not v:
        return False
    if v.startswith("\\\\") or v.startswith("/") or v.startswith("./") or v.startswith("../"):
        return True
    return bool(re.match(r"^[A-Za-z]:[\\/]", v))


def _extract_google_drive_folder_id(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    # URL tipo: https://drive.google.com/drive/folders/<id>?...
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", raw)
    if m:
        return m.group(1)
    # URL tipo: ...?id=<id>
    m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", raw)
    if m:
        return m.group(1)
    # Acepta ID directo
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", raw):
        return raw
    return ""


def _google_drive_timeout() -> int:
    return _to_int(os.getenv("GOOGLE_DRIVE_TIMEOUT_SECONDS", "60"), default=60, min_value=10, max_value=300)


def _google_drive_access_token() -> str:
    token = (os.getenv("GOOGLE_DRIVE_ACCESS_TOKEN") or "").strip()
    if token:
        return token

    client_id = (os.getenv("GOOGLE_DRIVE_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("GOOGLE_DRIVE_CLIENT_SECRET") or "").strip()
    refresh_token = (os.getenv("GOOGLE_DRIVE_REFRESH_TOKEN") or "").strip()
    if not (client_id and client_secret and refresh_token):
        raise RuntimeError(
            "Faltan credenciales de Google Drive. "
            "Define GOOGLE_DRIVE_ACCESS_TOKEN o CLIENT_ID/CLIENT_SECRET/REFRESH_TOKEN."
        )

    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        },
        timeout=_google_drive_timeout(),
    )
    if resp.status_code >= 400:
        txt = (resp.text or "").strip()
        raise RuntimeError(f"No se pudo refrescar token de Drive (HTTP {resp.status_code}): {txt[:260]}")
    data = resp.json() if resp.content else {}
    token = str(data.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Respuesta de OAuth sin access_token.")
    return token


def _drive_file_web_link(file_id: str) -> str:
    fid = (file_id or "").strip()
    if not fid:
        return ""
    return f"https://drive.google.com/file/d/{fid}/view"


def _google_drive_find_existing_file_id(
    session: requests.Session,
    *,
    access_token: str,
    folder_id: str,
    filename: str,
) -> str:
    safe_name = (filename or "").replace("\\", "\\\\").replace("'", "\\'")
    q = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
    resp = session.get(
        "https://www.googleapis.com/drive/v3/files",
        headers={"Authorization": f"Bearer {access_token}"},
        params={
            "q": q,
            "fields": "files(id,name)",
            "pageSize": 1,
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        },
        timeout=_google_drive_timeout(),
    )
    if resp.status_code >= 400:
        txt = (resp.text or "").strip()
        raise RuntimeError(f"Error buscando archivo en Drive (HTTP {resp.status_code}): {txt[:260]}")
    data = resp.json() if resp.content else {}
    files = data.get("files") or []
    if not files:
        return ""
    return str((files[0] or {}).get("id") or "")


def _google_drive_upload_pdf(
    session: requests.Session,
    *,
    access_token: str,
    folder_id: str,
    src: Path,
    replace_file_id: str = "",
) -> Dict[str, Any]:
    is_update = bool(replace_file_id)
    method = "PATCH" if is_update else "POST"
    url = (
        f"https://www.googleapis.com/upload/drive/v3/files/{replace_file_id}"
        if is_update
        else "https://www.googleapis.com/upload/drive/v3/files"
    )
    metadata: Dict[str, Any] = {"name": src.name}
    if not is_update:
        metadata["parents"] = [folder_id]

    with src.open("rb") as f:
        resp = session.request(
            method=method,
            url=url,
            headers={"Authorization": f"Bearer {access_token}"},
            params={
                "uploadType": "multipart",
                "supportsAllDrives": "true",
                "fields": "id,name,webViewLink",
            },
            files={
                "metadata": (
                    "metadata",
                    json.dumps(metadata, ensure_ascii=False),
                    "application/json; charset=UTF-8",
                ),
                "file": (src.name, f, "application/pdf"),
            },
            timeout=_google_drive_timeout(),
        )

    if resp.status_code >= 400:
        txt = (resp.text or "").strip()
        op = "actualizar" if is_update else "subir"
        raise RuntimeError(f"No se pudo {op} PDF en Drive (HTTP {resp.status_code}): {txt[:260]}")
    return resp.json() if resp.content else {}


def _upload_pdf_to_google_drive_url(target_url: str, src: Path) -> Dict[str, Any]:
    folder_id = _extract_google_drive_folder_id(target_url)
    if not folder_id:
        return {"ok": False, "enabled": True, "error": "No se pudo extraer folderId de la URL de Drive."}

    upsert_by_name = _to_bool(os.getenv("GOOGLE_DRIVE_UPSERT_BY_NAME", "true"), default=True)
    session = requests.Session()
    try:
        token = _google_drive_access_token()
        existing_id = ""
        if upsert_by_name:
            existing_id = _google_drive_find_existing_file_id(
                session,
                access_token=token,
                folder_id=folder_id,
                filename=src.name,
            )

        payload = _google_drive_upload_pdf(
            session,
            access_token=token,
            folder_id=folder_id,
            src=src,
            replace_file_id=existing_id,
        )
        file_id = str(payload.get("id") or existing_id or "").strip()
        web_link = str(payload.get("webViewLink") or "").strip() or _drive_file_web_link(file_id)
        return {
            "ok": True,
            "enabled": True,
            "copied": True,
            "uploaded": True,
            "replaced": bool(existing_id),
            "file_id": file_id,
            "dest": web_link,
            "folder_id": folder_id,
        }
    except Exception as e:
        return {"ok": False, "enabled": True, "error": str(e), "folder_id": folder_id}
    finally:
        session.close()


def _transform_row_for_supabase(table: str, row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Alinea columnas SQLite desktop al esquema Supabase de terralix-web.
    """
    out = dict(row)

    if table == "documentos":
        if "IVA" in out:
            out["iva"] = out.pop("IVA")
        if "DTE_referencia" in out:
            out["dte_referencia"] = out.pop("DTE_referencia")

        # Solo existen en SQLite desktop.
        out.pop("saldo", None)
        out.pop("categoria_doc", None)

        # Evita sobreescribir ruta_pdf en Supabase con path local Windows.
        ruta_mode = (os.getenv("SUPABASE_SYNC_RUTA_PDF_MODE", "preserve_remote") or "").strip().lower()
        ruta_pdf = str(out.get("ruta_pdf") or "").strip()
        if ruta_mode == "raw":
            pass
        elif ruta_mode == "basename":
            out["ruta_pdf"] = Path(ruta_pdf).name if ruta_pdf else ""
        elif ruta_mode == "id_doc_filename":
            doc_id = str(out.get("id_doc") or "").strip()
            out["ruta_pdf"] = f"{doc_id}.pdf" if doc_id else (Path(ruta_pdf).name if ruta_pdf else "")
        else:
            # preserve_remote (default): solo envía si es URL o storage path no-local.
            if not ruta_pdf:
                out.pop("ruta_pdf", None)
            elif _looks_like_url(ruta_pdf):
                out["ruta_pdf"] = ruta_pdf
            elif _looks_like_local_path(ruta_pdf):
                out.pop("ruta_pdf", None)

    elif table == "detalle":
        if "Descuento" in out:
            out["descuento"] = out.pop("Descuento")
        if "fecha_emision" in out and "fecha_emision_denorm" not in out:
            out["fecha_emision_denorm"] = out.pop("fecha_emision")
        out.pop("confianza_ia", None)

    return out


def _to_upsert_payload(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _serialize_value(v) for k, v in row.items()}


def _delete_filter_value(value: Any) -> str:
    if value is None:
        return "is.null"
    if isinstance(value, bool):
        return f"eq.{str(value).lower()}"
    return f"eq.{value}"


def _supabase_upsert(
    session: requests.Session,
    *,
    base_url: str,
    key: str,
    table: str,
    pk_cols: List[str],
    row_payload: Dict[str, Any],
) -> None:
    on_conflict = ",".join(pk_cols)
    resp = session.post(
        f"{base_url}/rest/v1/{table}",
        headers=_headers(key),
        params={"on_conflict": on_conflict},
        json=[row_payload],
        timeout=_request_timeout(),
    )
    if resp.status_code >= 400:
        text = (resp.text or "").strip()
        raise RuntimeError(f"upsert {table} HTTP {resp.status_code}: {text[:260]}")


def _supabase_delete(
    session: requests.Session,
    *,
    base_url: str,
    key: str,
    table: str,
    pk_map: Dict[str, Any],
    pk_cols: List[str],
) -> None:
    params: List[Tuple[str, str]] = []
    for col in pk_cols:
        params.append((col, _delete_filter_value(pk_map.get(col))))
    resp = session.delete(
        f"{base_url}/rest/v1/{table}",
        headers=_headers(key),
        params=params,
        timeout=_request_timeout(),
    )
    if resp.status_code >= 400:
        text = (resp.text or "").strip()
        raise RuntimeError(f"delete {table} HTTP {resp.status_code}: {text[:260]}")


def sync_pending_changes(
    db_path: Optional[str] = None,
    *,
    max_items: Optional[int] = None,
    tables: Optional[List[str]] = None,
    log=print,
) -> Dict[str, Any]:
    if not _is_sync_enabled():
        return {"ok": True, "enabled": False, "reason": "SUPABASE_SYNC_ENABLED=false"}

    init = initialize_supabase_sync(db_path=db_path, log=log)
    if not init.get("ok"):
        return {"ok": False, "enabled": True, "error": init.get("error", "sync_init_failed")}
    resolved_db = init.get("db_path")
    if not resolved_db:
        return {"ok": False, "enabled": True, "error": "db_path_not_resolved"}

    try:
        base_url, key = _supabase_config()
    except Exception as e:
        return {"ok": False, "enabled": True, "error": str(e)}

    batch_size = max_items
    if batch_size is None:
        batch_size = _to_int(os.getenv("SUPABASE_SYNC_BATCH_SIZE", "250"), default=250, min_value=1, max_value=5000)

    max_retries = _to_int(os.getenv("SUPABASE_SYNC_MAX_RETRIES", "25"), default=25, min_value=1, max_value=500)
    scope_tables = _normalize_table_scope(tables)

    processed = 0
    failed = 0
    dropped = 0

    with _SYNC_LOCK:
        try:
            con = _connect(str(resolved_db))
            session = requests.Session()
            try:
                rows = _pending_rows(con, int(batch_size), tables=scope_tables)
                if not rows:
                    return {
                        "ok": True,
                        "enabled": True,
                        "processed": 0,
                        "failed": 0,
                        "dropped": 0,
                        "pending": 0,
                        "scope_tables": scope_tables,
                    }

                for item in rows:
                    row_id = int(item["id"])
                    table = str(item["table_name"])
                    op = str(item["op"])
                    created_at = str(item["created_at"] or "")
                    attempts = int(item["attempts"] or 0) + 1

                    try:
                        if not _table_exists(con, table):
                            raise RuntimeError(f"tabla_local_inexistente:{table}")

                        pk_cols = _pk_columns(con, table)
                        if not pk_cols:
                            raise RuntimeError(f"tabla_sin_pk:{table}")

                        pk_map = _json_load(str(item["pk_json"] or "{}"))
                        if not pk_map:
                            raise RuntimeError(f"pk_json_invalido:{table}")

                        if op == "delete":
                            _supabase_delete(
                                session,
                                base_url=base_url,
                                key=key,
                                table=table,
                                pk_map=pk_map,
                                pk_cols=pk_cols,
                            )
                        else:
                            row_data = _fetch_local_row(con, table, pk_cols, pk_map)
                            if row_data is None:
                                # Si la fila ya no existe localmente, la reflejamos como delete remoto.
                                _supabase_delete(
                                    session,
                                    base_url=base_url,
                                    key=key,
                                    table=table,
                                    pk_map=pk_map,
                                    pk_cols=pk_cols,
                                )
                            else:
                                _supabase_upsert(
                                    session,
                                    base_url=base_url,
                                    key=key,
                                    table=table,
                                    pk_cols=pk_cols,
                                    row_payload=_to_upsert_payload(_transform_row_for_supabase(table, row_data)),
                                )

                        _mark_row_success(con, row_id, created_at, attempts)
                        processed += 1
                    except Exception as e:
                        error_text = str(e)
                        drop = attempts >= max_retries
                        _mark_row_failure(con, row_id, created_at, attempts, error_text, drop=drop)
                        if drop:
                            dropped += 1
                        else:
                            failed += 1

                con.commit()
                pending = _pending_count(con, tables=scope_tables)
                return {
                    "ok": True,
                    "enabled": True,
                    "processed": processed,
                    "failed": failed,
                    "dropped": dropped,
                    "pending": pending,
                    "scope_tables": scope_tables,
                }
            finally:
                session.close()
                con.close()
        except Exception as e:
            return {"ok": False, "enabled": True, "error": f"sync_failed:{e}"}


def enqueue_full_sync(
    db_path: Optional[str] = None,
    *,
    tables: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Encola un upsert de todas las filas locales para resincronizacion completa.
    """
    init = initialize_supabase_sync(db_path=db_path, log=lambda _m: None)
    if not init.get("ok"):
        return {"ok": False, "error": init.get("error", "init_failed")}
    if not init.get("enabled", True):
        return {"ok": True, "enabled": False, "queued": 0}

    resolved_db = str(init.get("db_path") or "")
    if not resolved_db:
        return {"ok": False, "error": "db_path_not_resolved"}

    target_tables = tables[:] if tables else _sync_tables()
    total_queued = 0

    with _SYNC_LOCK:
        con = _connect(resolved_db)
        try:
            _ensure_queue_schema(con)

            for table in target_tables:
                if not _table_exists(con, table):
                    continue
                pk_cols = _pk_columns(con, table)
                if not pk_cols:
                    continue

                select_cols = ", ".join([_q(c) for c in pk_cols])
                rows = con.execute(f"SELECT {select_cols} FROM {_q(table)}").fetchall()
                for row in rows:
                    pk_obj = {c: row[c] for c in pk_cols}
                    pk_json = json.dumps(pk_obj, ensure_ascii=False, separators=(",", ":"))
                    con.execute(
                        """
                        INSERT INTO supabase_sync_queue (table_name, op, pk_json, created_at, processed_at, attempts, last_error)
                        VALUES (?, 'upsert', ?, datetime('now'), NULL, 0, NULL)
                        ON CONFLICT(table_name, pk_json) DO UPDATE SET
                            op = 'upsert',
                            created_at = datetime('now'),
                            processed_at = NULL,
                            attempts = 0,
                            last_error = NULL
                        """,
                        (table, pk_json),
                    )
                    total_queued += 1

            con.commit()
        finally:
            con.close()

    return {"ok": True, "enabled": True, "queued": total_queued}


def mirror_pdf_to_google_drive(
    pdf_path: str,
    *,
    log=print,
) -> Dict[str, Any]:
    """
    Replica un PDF local a Google Drive:
    - Si GOOGLE_DRIVE_PDF_DIR es ruta local -> copia a carpeta de Drive Desktop.
    - Si GOOGLE_DRIVE_PDF_DIR es URL/ID de folder -> sube por Google Drive API.
    """
    if not _to_bool(os.getenv("GOOGLE_DRIVE_COPY_ENABLED", "true"), default=True):
        return {"ok": True, "enabled": False, "reason": "GOOGLE_DRIVE_COPY_ENABLED=false"}

    target_dir_raw = _clean_path(os.getenv("GOOGLE_DRIVE_PDF_DIR", ""))
    if not target_dir_raw:
        return {"ok": True, "enabled": False, "reason": "GOOGLE_DRIVE_PDF_DIR_vacio"}

    src = Path(str(pdf_path or "")).expanduser()
    if not src.exists() or not src.is_file():
        return {"ok": False, "error": f"pdf_no_existe:{src}"}

    if _looks_like_url(target_dir_raw):
        result = _upload_pdf_to_google_drive_url(target_dir_raw, src)
        if not result.get("ok", False):
            log(f"[DRIVE][WARN] No se pudo subir PDF a Drive URL: {result.get('error', 'error_desconocido')}")
        return result

    target_dir = Path(target_dir_raw).expanduser()
    target_dir.mkdir(parents=True, exist_ok=True)

    dst = target_dir / src.name
    try:
        if src.resolve() == dst.resolve():
            return {"ok": True, "enabled": True, "copied": False, "reason": "origen_igual_destino", "dest": str(dst)}
    except Exception:
        pass

    if dst.exists():
        try:
            if dst.stat().st_size == src.stat().st_size:
                return {"ok": True, "enabled": True, "copied": False, "reason": "archivo_ya_actualizado", "dest": str(dst)}
        except Exception:
            pass

    try:
        shutil.copy2(str(src), str(dst))
        return {"ok": True, "enabled": True, "copied": True, "dest": str(dst)}
    except Exception as e:
        log(f"[DRIVE][WARN] No se pudo copiar PDF a Google Drive: {e}")
        return {"ok": False, "enabled": True, "error": str(e)}
