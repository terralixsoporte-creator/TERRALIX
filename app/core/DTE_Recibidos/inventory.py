"""
Modulo de inventario teorico para Terralix.

Objetivo:
  - Mantener un catalogo maestro por codigo de producto.
  - Registrar movimientos de inventario (entradas desde DTE y salidas manuales por uso).
  - Calcular stock teorico actual por codigo.
"""

from __future__ import annotations

import os
import re
import sqlite3
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

_IGNORED_CODES = {"-"}
_NON_FIELD_CATEGORIES = {"EPP", "MATERIAL", "HERRAMIENTA", "CONTABLE", "RIEGO"}
_NON_FIELD_TYPES = {
    "PROTECCION PERSONAL",
    "INSUMO GENERAL",
    "EQUIPO",
    "NO INVENTARIABLE",
    "INFRAESTRUCTURA",
}


def _clean_path(path_value: str) -> str:
    return (path_value or "").strip().strip('"').strip("'")


def resolve_db_path(db_path: Optional[str] = None) -> str:
    resolved = _clean_path(db_path or os.getenv("DB_PATH_DTE_RECIBIDOS", ""))
    if not resolved:
        raise ValueError("No se encontro DB_PATH_DTE_RECIBIDOS en variables de entorno.")
    return resolved


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_code(value: Any) -> str:
    return _normalize_text(value).upper()


def _is_valid_inventory_code(value: Any) -> bool:
    code = _normalize_code(value)
    return bool(code) and code not in _IGNORED_CODES


def _is_field_product(categoria: Any, tipo: Any) -> bool:
    cat = _normalize_text(categoria).upper()
    tpo = _normalize_text(tipo).upper()
    if cat in _NON_FIELD_CATEGORIES:
        return False
    if tpo in _NON_FIELD_TYPES:
        return False
    return True


def _normalize_header(value: Any) -> str:
    s = _normalize_text(value)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)

    s = _normalize_text(value)
    if not s:
        return 0.0

    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", s):
        s = s.replace(".", "")

    try:
        return float(s)
    except Exception:
        return 0.0


def _parse_float_strict(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    s = _normalize_text(value)
    if not s:
        return None

    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".")
    elif re.fullmatch(r"\d{1,3}(?:\.\d{3})+", s):
        s = s.replace(".", "")

    try:
        return float(s)
    except Exception:
        return None


def _to_int(value: Any) -> int:
    try:
        return int(round(_to_float(value)))
    except Exception:
        return 0


def _coerce_date_iso(value: Any) -> str:
    if value is None:
        return date.today().isoformat()
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    raw = _normalize_text(value)
    if not raw:
        return date.today().isoformat()

    # Casos comunes: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS
    raw10 = raw[:10]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw10, fmt).date().isoformat()
        except Exception:
            pass

    # Fallback: hoy
    return date.today().isoformat()


def _parse_date_iso_strict(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()

    raw = _normalize_text(value)
    if not raw:
        return None

    raw10 = raw[:10]
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(raw10, fmt).date().isoformat()
        except Exception:
            pass
    return None


def _connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    return con


def _add_column_if_missing(con: sqlite3.Connection, table: str, column: str, coltype: str) -> None:
    cols = {r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()}
    if column not in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {coltype}")


def _purge_ignored_codes_in_connection(con: sqlite3.Connection) -> Dict[str, int]:
    n_mov = con.execute(
        "DELETE FROM inventario_movimientos WHERE UPPER(TRIM(COALESCE(codigo, ''))) IN ('', '-')"
    ).rowcount
    n_cat = con.execute(
        "DELETE FROM inventario_catalogo WHERE UPPER(TRIM(COALESCE(codigo, ''))) IN ('', '-')"
    ).rowcount
    return {"catalog_deleted": int(n_cat or 0), "movements_deleted": int(n_mov or 0)}


def ensure_inventory_schema(db_path: str) -> None:
    con = _connect(db_path)
    try:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS inventario_catalogo (
                codigo TEXT PRIMARY KEY,
                descripcion_estandar TEXT,
                unidad_base TEXT,
                categoria TEXT,
                tipo TEXT,
                ocurrencias INTEGER DEFAULT 0,
                variaciones TEXT DEFAULT 'NO',
                activo INTEGER DEFAULT 1,
                fuente TEXT,
                updated_at TEXT DEFAULT (datetime('now'))
            )
            """
        )
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS inventario_movimientos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                codigo TEXT NOT NULL,
                fecha TEXT NOT NULL,
                tipo_mov TEXT NOT NULL,
                cantidad REAL NOT NULL,
                unidad TEXT,
                signo INTEGER NOT NULL,
                fuente TEXT NOT NULL,
                referencia TEXT,
                observacion TEXT,
                source_hash TEXT UNIQUE,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (codigo) REFERENCES inventario_catalogo(codigo)
            )
            """
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_inv_mov_codigo_fecha ON inventario_movimientos(codigo, fecha)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_inv_mov_fuente ON inventario_movimientos(fuente)"
        )
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_inv_catalogo_categoria ON inventario_catalogo(categoria, tipo)"
        )
        _add_column_if_missing(con, "inventario_catalogo", "ultima_fecha_override", "TEXT")
        _add_column_if_missing(con, "inventario_catalogo", "ultima_tipo_override", "TEXT")
        _purge_ignored_codes_in_connection(con)
        con.commit()
    finally:
        con.close()


def _header_aliases() -> Dict[str, set[str]]:
    return {
        "codigo": {"CODIGO", "COD", "COD.", "SKU", "ITEM"},
        "descripcion_estandar": {
            "DESCRIPCION ESTANDAR",
            "DESCRIPCION",
            "PRODUCTO",
            "INSUMO",
        },
        "unidad_base": {"UNIDAD", "UND", "UM"},
        "categoria": {"CATEGORIA"},
        "tipo": {"TIPO"},
        "ocurrencias": {"OCURRENCIAS"},
        "variaciones": {"VARIACIONES"},
    }


def import_catalog_from_excel(
    excel_path: str,
    db_path: str,
    source_name: str = "EXCEL_MAESTRO",
) -> Dict[str, Any]:
    try:
        from openpyxl import load_workbook
    except Exception:
        return {"ok": False, "error": "Falta la dependencia openpyxl para leer Excel."}

    ensure_inventory_schema(db_path)

    wb = load_workbook(excel_path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    try:
        headers = [c.value for c in next(ws.iter_rows(min_row=1, max_row=1))]
    except StopIteration:
        wb.close()
        return {"ok": False, "error": "El Excel de inventario esta vacio."}

    alias = _header_aliases()
    col_map: Dict[str, int] = {}
    for idx, h in enumerate(headers):
        norm = _normalize_header(h)
        for target, options in alias.items():
            if norm in options and target not in col_map:
                col_map[target] = idx

    if "codigo" not in col_map:
        wb.close()
        return {"ok": False, "error": "No se encontro la columna 'Codigo' en el Excel."}

    con = _connect(db_path)
    inserted = 0
    updated = 0
    skipped = 0

    try:
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row:
                continue

            codigo = _normalize_code(row[col_map["codigo"]])
            if not _is_valid_inventory_code(codigo):
                skipped += 1
                continue

            desc = _normalize_text(row[col_map["descripcion_estandar"]]) if "descripcion_estandar" in col_map else ""
            unidad = _normalize_text(row[col_map["unidad_base"]]).upper() if "unidad_base" in col_map else ""
            categoria = _normalize_text(row[col_map["categoria"]]).upper() if "categoria" in col_map else ""
            tipo = _normalize_text(row[col_map["tipo"]]).upper() if "tipo" in col_map else ""
            ocurrencias = _to_int(row[col_map["ocurrencias"]]) if "ocurrencias" in col_map else 0
            variaciones = _normalize_text(row[col_map["variaciones"]]).upper() if "variaciones" in col_map else "NO"

            old = con.execute(
                """
                SELECT codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones
                FROM inventario_catalogo
                WHERE codigo = ?
                """,
                (codigo,),
            ).fetchone()

            if old is None:
                con.execute(
                    """
                    INSERT INTO inventario_catalogo
                    (codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones, activo, fuente, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, datetime('now'))
                    """,
                    (
                        codigo,
                        desc,
                        unidad,
                        categoria,
                        tipo,
                        ocurrencias,
                        variaciones or "NO",
                        source_name,
                    ),
                )
                inserted += 1
                continue

            new_desc = desc or (old["descripcion_estandar"] or "")
            new_unidad = unidad or (old["unidad_base"] or "")
            new_categoria = categoria or (old["categoria"] or "")
            new_tipo = tipo or (old["tipo"] or "")
            new_ocurr = ocurrencias if ocurrencias > 0 else int(old["ocurrencias"] or 0)
            new_var = variaciones or (old["variaciones"] or "NO")

            changed = (
                new_desc != (old["descripcion_estandar"] or "")
                or new_unidad != (old["unidad_base"] or "")
                or new_categoria != (old["categoria"] or "")
                or new_tipo != (old["tipo"] or "")
                or new_ocurr != int(old["ocurrencias"] or 0)
                or new_var != (old["variaciones"] or "NO")
            )

            if changed:
                con.execute(
                    """
                    UPDATE inventario_catalogo
                    SET descripcion_estandar = ?,
                        unidad_base = ?,
                        categoria = ?,
                        tipo = ?,
                        ocurrencias = ?,
                        variaciones = ?,
                        activo = 1,
                        fuente = ?,
                        updated_at = datetime('now')
                    WHERE codigo = ?
                    """,
                    (
                        new_desc,
                        new_unidad,
                        new_categoria,
                        new_tipo,
                        new_ocurr,
                        new_var,
                        source_name,
                        codigo,
                    ),
                )
                updated += 1
            else:
                skipped += 1

        con.commit()
    finally:
        wb.close()
        con.close()

    return {
        "ok": True,
        "inserted": inserted,
        "updated": updated,
        "skipped": skipped,
        "total_processed": inserted + updated + skipped,
    }


def _table_exists(con: sqlite3.Connection, table_name: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def _refresh_catalog_stats_from_detalle(
    con: sqlite3.Connection,
    only_categoria: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Recalcula ocurrencias/variaciones por codigo usando detalle.
    Respeta la exclusion de codigos no inventariables ('-').
    """
    if not _table_exists(con, "detalle"):
        return {"ok": False, "error": "No existe la tabla detalle"}

    detalle_cols = {r[1] for r in con.execute("PRAGMA table_info(detalle);").fetchall()}
    if "codigo" not in detalle_cols:
        return {"ok": False, "error": "La tabla detalle no tiene columna codigo"}

    select_parts = [
        "UPPER(TRIM(COALESCE(d.codigo, ''))) AS codigo",
        "UPPER(TRIM(COALESCE(d.descripcion, ''))) AS descripcion_norm" if "descripcion" in detalle_cols else "'' AS descripcion_norm",
        "UPPER(TRIM(COALESCE(d.unidad, ''))) AS unidad_norm" if "unidad" in detalle_cols else "'' AS unidad_norm",
        "UPPER(TRIM(COALESCE(d.categoria, ''))) AS categoria_norm" if "categoria" in detalle_cols else "'' AS categoria_norm",
        "COUNT(*) AS ocurrencias_total",
        "COUNT(DISTINCT UPPER(TRIM(COALESCE(d.descripcion, '')))) AS n_desc" if "descripcion" in detalle_cols else "1 AS n_desc",
        "COUNT(DISTINCT UPPER(TRIM(COALESCE(d.unidad, '')))) AS n_uni" if "unidad" in detalle_cols else "1 AS n_uni",
    ]
    where_parts = [
        "TRIM(COALESCE(d.codigo, '')) <> ''",
        "UPPER(TRIM(COALESCE(d.codigo, ''))) <> '-'",
    ]
    params: List[Any] = []
    if only_categoria and "categoria" in detalle_cols:
        where_parts.append("UPPER(TRIM(COALESCE(d.categoria, ''))) = ?")
        params.append(only_categoria.strip().upper())

    # Se toma descripcion/unidad/categoria con mayor frecuencia por codigo.
    grouped = con.execute(
        f"""
        WITH base AS (
            SELECT
                UPPER(TRIM(COALESCE(d.codigo, ''))) AS codigo,
                UPPER(TRIM(COALESCE(d.descripcion, ''))) AS descripcion_norm,
                UPPER(TRIM(COALESCE(d.unidad, ''))) AS unidad_norm,
                UPPER(TRIM(COALESCE(d.categoria, ''))) AS categoria_norm
            FROM detalle d
            WHERE {' AND '.join(where_parts)}
        ),
        desc_mode AS (
            SELECT codigo, descripcion_norm
            FROM (
                SELECT
                    codigo,
                    descripcion_norm,
                    COUNT(*) AS c,
                    ROW_NUMBER() OVER (
                        PARTITION BY codigo
                        ORDER BY COUNT(*) DESC, descripcion_norm
                    ) AS rn
                FROM base
                WHERE descripcion_norm <> ''
                GROUP BY codigo, descripcion_norm
            ) x
            WHERE rn = 1
        ),
        uni_mode AS (
            SELECT codigo, unidad_norm
            FROM (
                SELECT
                    codigo,
                    unidad_norm,
                    COUNT(*) AS c,
                    ROW_NUMBER() OVER (
                        PARTITION BY codigo
                        ORDER BY COUNT(*) DESC, unidad_norm
                    ) AS rn
                FROM base
                WHERE unidad_norm <> ''
                GROUP BY codigo, unidad_norm
            ) x
            WHERE rn = 1
        ),
        cat_mode AS (
            SELECT codigo, categoria_norm
            FROM (
                SELECT
                    codigo,
                    categoria_norm,
                    COUNT(*) AS c,
                    ROW_NUMBER() OVER (
                        PARTITION BY codigo
                        ORDER BY COUNT(*) DESC, categoria_norm
                    ) AS rn
                FROM base
                WHERE categoria_norm <> ''
                GROUP BY codigo, categoria_norm
            ) x
            WHERE rn = 1
        ),
        agg AS (
            SELECT
                codigo,
                COUNT(*) AS ocurrencias_total,
                COUNT(DISTINCT CASE WHEN descripcion_norm <> '' THEN descripcion_norm END) AS n_desc,
                COUNT(DISTINCT CASE WHEN unidad_norm <> '' THEN unidad_norm END) AS n_uni
            FROM base
            GROUP BY codigo
        )
        SELECT
            a.codigo AS codigo,
            COALESCE(dm.descripcion_norm, '') AS descripcion_mode,
            COALESCE(um.unidad_norm, '') AS unidad_mode,
            COALESCE(cm.categoria_norm, '') AS categoria_mode,
            a.ocurrencias_total AS ocurrencias_total,
            a.n_desc AS n_desc,
            a.n_uni AS n_uni
        FROM agg a
        LEFT JOIN desc_mode dm ON dm.codigo = a.codigo
        LEFT JOIN uni_mode um ON um.codigo = a.codigo
        LEFT JOIN cat_mode cm ON cm.codigo = a.codigo
        """,
        params,
    ).fetchall()

    updated = 0
    inserted = 0
    variaciones_si = 0
    for r in grouped:
        codigo = _normalize_code(r["codigo"])
        if not _is_valid_inventory_code(codigo):
            continue

        desc_mode = _normalize_text(r["descripcion_mode"]).upper()
        unidad_mode = _normalize_text(r["unidad_mode"]).upper()
        categoria_mode = _normalize_text(r["categoria_mode"]).upper()
        ocurrencias = _to_int(r["ocurrencias_total"])
        n_desc = _to_int(r["n_desc"])
        n_uni = _to_int(r["n_uni"])
        variaciones = "SI" if n_desc > 1 or n_uni > 1 else "NO"
        if variaciones == "SI":
            variaciones_si += 1

        old = con.execute(
            """
            SELECT codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones
            FROM inventario_catalogo
            WHERE codigo = ?
            """,
            (codigo,),
        ).fetchone()

        if old is None:
            con.execute(
                """
                INSERT INTO inventario_catalogo
                (codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones, activo, fuente, updated_at)
                VALUES (?, ?, ?, ?, '', ?, ?, 1, 'AUTO_DETALLE', datetime('now'))
                """,
                (codigo, desc_mode, unidad_mode, categoria_mode, ocurrencias, variaciones),
            )
            inserted += 1
            continue

        new_desc = old["descripcion_estandar"] or desc_mode
        new_unidad = old["unidad_base"] or unidad_mode
        new_categoria = old["categoria"] or categoria_mode
        new_tipo = old["tipo"] or ""
        new_ocurr = ocurrencias
        new_var = variaciones

        if (
            new_desc != (old["descripcion_estandar"] or "")
            or new_unidad != (old["unidad_base"] or "")
            or new_categoria != (old["categoria"] or "")
            or new_tipo != (old["tipo"] or "")
            or new_ocurr != int(old["ocurrencias"] or 0)
            or new_var != (old["variaciones"] or "NO")
        ):
            con.execute(
                """
                UPDATE inventario_catalogo
                SET descripcion_estandar = ?,
                    unidad_base = ?,
                    categoria = ?,
                    tipo = ?,
                    ocurrencias = ?,
                    variaciones = ?,
                    activo = 1,
                    fuente = CASE
                        WHEN COALESCE(fuente, '') = '' THEN 'AUTO_DETALLE'
                        ELSE fuente
                    END,
                    updated_at = datetime('now')
                WHERE codigo = ?
                """,
                (
                    new_desc,
                    new_unidad,
                    new_categoria,
                    new_tipo,
                    new_ocurr,
                    new_var,
                    codigo,
                ),
            )
            updated += 1

    return {
        "ok": True,
        "codes_seen": len(grouped),
        "catalog_inserted_stats": inserted,
        "catalog_updated_stats": updated,
        "codes_variaciones_si": variaciones_si,
    }


def sync_entries_from_detalle(
    db_path: str,
    only_categoria: Optional[str] = None,
) -> Dict[str, Any]:
    ensure_inventory_schema(db_path)
    con = _connect(db_path)
    try:
        if not _table_exists(con, "detalle"):
            return {"ok": False, "error": "No existe la tabla 'detalle' en la base de datos."}

        detalle_cols = {r[1] for r in con.execute("PRAGMA table_info(detalle);").fetchall()}
        required = {"id_det", "codigo", "descripcion", "cantidad"}
        missing = sorted(required - detalle_cols)
        if missing:
            return {"ok": False, "error": f"Faltan columnas en detalle: {', '.join(missing)}"}

        select_parts = [
            "d.id_det",
            "COALESCE(d.codigo, '') AS codigo",
            "COALESCE(d.descripcion, '') AS descripcion",
            "COALESCE(d.cantidad, 0) AS cantidad",
            "COALESCE(d.unidad, '') AS unidad" if "unidad" in detalle_cols else "'' AS unidad",
            "COALESCE(d.categoria, '') AS categoria" if "categoria" in detalle_cols else "'' AS categoria",
            "COALESCE(d.fecha_emision, '') AS fecha_emision" if "fecha_emision" in detalle_cols else "'' AS fecha_emision",
        ]

        query = f"""
            SELECT {", ".join(select_parts)}
            FROM detalle d
            WHERE TRIM(COALESCE(d.codigo, '')) <> ''
              AND UPPER(TRIM(COALESCE(d.codigo, ''))) <> '-'
              AND COALESCE(d.cantidad, 0) > 0
        """
        params: List[Any] = []
        if only_categoria and "categoria" in detalle_cols:
            query += " AND UPPER(TRIM(COALESCE(d.categoria, ''))) = ?"
            params.append(only_categoria.strip().upper())

        rows = con.execute(query, params).fetchall()

        mov_inserted = 0
        mov_updated = 0
        cat_inserted = 0
        cat_updated = 0

        for r in rows:
            codigo = _normalize_code(r["codigo"])
            if not _is_valid_inventory_code(codigo):
                continue

            descripcion = _normalize_text(r["descripcion"])
            unidad = _normalize_text(r["unidad"]).upper()
            categoria = _normalize_text(r["categoria"]).upper()
            cantidad = _to_float(r["cantidad"])
            fecha = _coerce_date_iso(r["fecha_emision"])

            old_cat = con.execute(
                "SELECT codigo, descripcion_estandar, unidad_base, categoria FROM inventario_catalogo WHERE codigo = ?",
                (codigo,),
            ).fetchone()

            if old_cat is None:
                con.execute(
                    """
                    INSERT INTO inventario_catalogo
                    (codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones, activo, fuente, updated_at)
                    VALUES (?, ?, ?, ?, '', 0, 'NO', 1, 'SYNC_DTE', datetime('now'))
                    """,
                    (codigo, descripcion, unidad, categoria),
                )
                cat_inserted += 1
            else:
                new_desc = old_cat["descripcion_estandar"] or descripcion
                new_unidad = old_cat["unidad_base"] or unidad
                new_categoria = old_cat["categoria"] or categoria
                if (
                    new_desc != (old_cat["descripcion_estandar"] or "")
                    or new_unidad != (old_cat["unidad_base"] or "")
                    or new_categoria != (old_cat["categoria"] or "")
                ):
                    con.execute(
                        """
                        UPDATE inventario_catalogo
                        SET descripcion_estandar = ?,
                            unidad_base = ?,
                            categoria = ?,
                            updated_at = datetime('now')
                        WHERE codigo = ?
                        """,
                        (new_desc, new_unidad, new_categoria, codigo),
                    )
                    cat_updated += 1

            source_hash = f"DTE:{_normalize_text(r['id_det'])}"
            old_mov = con.execute(
                "SELECT id FROM inventario_movimientos WHERE source_hash = ?",
                (source_hash,),
            ).fetchone()

            if old_mov is None:
                con.execute(
                    """
                    INSERT INTO inventario_movimientos
                    (codigo, fecha, tipo_mov, cantidad, unidad, signo, fuente, referencia, observacion, source_hash)
                    VALUES (?, ?, 'ENTRADA_COMPRA', ?, ?, 1, 'DTE_DETALLE', ?, ?, ?)
                    """,
                    (
                        codigo,
                        fecha,
                        cantidad,
                        unidad,
                        _normalize_text(r["id_det"]),
                        descripcion,
                        source_hash,
                    ),
                )
                mov_inserted += 1
            else:
                con.execute(
                    """
                    UPDATE inventario_movimientos
                    SET codigo = ?,
                        fecha = ?,
                        tipo_mov = 'ENTRADA_COMPRA',
                        cantidad = ?,
                        unidad = ?,
                        signo = 1,
                        fuente = 'DTE_DETALLE',
                        referencia = ?,
                        observacion = ?
                    WHERE source_hash = ?
                    """,
                    (
                        codigo,
                        fecha,
                        cantidad,
                        unidad,
                        _normalize_text(r["id_det"]),
                        descripcion,
                        source_hash,
                    ),
                )
                mov_updated += 1

        stats = _refresh_catalog_stats_from_detalle(
            con=con,
            only_categoria=only_categoria,
        )
        purge = _purge_ignored_codes_in_connection(con)
        con.commit()
        return {
            "ok": True,
            "rows_scanned": len(rows),
            "movements_inserted": mov_inserted,
            "movements_updated": mov_updated,
            "catalog_inserted": cat_inserted,
            "catalog_updated": cat_updated,
            "catalog_stats": stats,
            "purge_ignored_codes": purge,
            "categoria_filtro": only_categoria or "(todas)",
        }
    finally:
        con.close()


def list_catalog_products(db_path: str) -> List[Dict[str, Any]]:
    ensure_inventory_schema(db_path)
    con = _connect(db_path)
    try:
        rows = con.execute(
            """
            SELECT codigo, descripcion_estandar, unidad_base, categoria, tipo, activo
            FROM inventario_catalogo
            WHERE COALESCE(activo, 1) = 1
              AND UPPER(TRIM(COALESCE(codigo, ''))) NOT IN ('', '-')
              AND UPPER(TRIM(COALESCE(categoria, ''))) NOT IN ('EPP', 'MATERIAL', 'HERRAMIENTA', 'CONTABLE', 'RIEGO')
              AND UPPER(TRIM(COALESCE(tipo, ''))) NOT IN (
                    'PROTECCION PERSONAL',
                    'INSUMO GENERAL',
                    'EQUIPO',
                    'NO INVENTARIABLE',
                    'INFRAESTRUCTURA'
              )
            ORDER BY categoria, tipo, codigo
            """
        ).fetchall()
        return [dict(r) for r in rows if _is_field_product(r["categoria"], r["tipo"])]
    finally:
        con.close()


def _stock_for_code(con: sqlite3.Connection, codigo: str) -> float:
    row = con.execute(
        """
        SELECT COALESCE(SUM(signo * cantidad), 0) AS stock
        FROM inventario_movimientos
        WHERE codigo = ?
        """,
        (codigo,),
    ).fetchone()
    return float(row["stock"] if row else 0.0)


def register_usage(
    db_path: str,
    codigo: str,
    cantidad: float,
    fecha_uso: Any,
    observacion: str = "",
    unidad: str = "",
    allow_negative: bool = False,
) -> Dict[str, Any]:
    ensure_inventory_schema(db_path)
    codigo_norm = _normalize_code(codigo)
    if not _is_valid_inventory_code(codigo_norm):
        return {"ok": False, "error": "Codigo invalido para inventario (vacio o '-')."}

    qty = _to_float(cantidad)
    if qty <= 0:
        return {"ok": False, "error": "La cantidad debe ser mayor a 0."}

    fecha = _coerce_date_iso(fecha_uso)
    obs = _normalize_text(observacion)
    um = _normalize_text(unidad).upper()

    con = _connect(db_path)
    try:
        cat = con.execute(
            "SELECT codigo, unidad_base FROM inventario_catalogo WHERE codigo = ?",
            (codigo_norm,),
        ).fetchone()
        if cat is None:
            con.execute(
                """
                INSERT INTO inventario_catalogo
                (codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones, activo, fuente, updated_at)
                VALUES (?, '', ?, '', '', 0, 'NO', 1, 'MANUAL_USO', datetime('now'))
                """,
                (codigo_norm, um),
            )
            unidad_final = um
        else:
            unidad_final = um or _normalize_text(cat["unidad_base"]).upper()

        stock_before = _stock_for_code(con, codigo_norm)
        stock_after = stock_before - qty
        if stock_after < 0 and not allow_negative:
            return {
                "ok": False,
                "error": "El movimiento deja stock negativo.",
                "stock_before": stock_before,
                "stock_after": stock_after,
            }

        cur = con.execute(
            """
            INSERT INTO inventario_movimientos
            (codigo, fecha, tipo_mov, cantidad, unidad, signo, fuente, referencia, observacion, source_hash)
            VALUES (?, ?, 'SALIDA_USO', ?, ?, -1, 'MANUAL_USO', NULL, ?, NULL)
            """,
            (codigo_norm, fecha, qty, unidad_final, obs),
        )
        con.commit()

        return {
            "ok": True,
            "movement_id": cur.lastrowid,
            "codigo": codigo_norm,
            "stock_before": stock_before,
            "stock_after": stock_after,
        }
    finally:
        con.close()


def update_stock_cell(
    db_path: str,
    codigo: str,
    column_name: str,
    new_value: Any,
    observacion: str = "EDICION_CELDA",
) -> Dict[str, Any]:
    """
    Edita una celda del modulo de stock (excepto codigo).

    Columnas soportadas:
      - descripcion_estandar: actualiza catalogo
      - unidad_base: actualiza catalogo
      - stock_actual: crea movimiento de ajuste por delta
      - ultima_fecha: edita la fecha del ultimo movimiento
      - ultima_modificacion_tipo: ajusta signo/tipo del ultimo movimiento
    """
    ensure_inventory_schema(db_path)

    codigo_norm = _normalize_code(codigo)
    if not _is_valid_inventory_code(codigo_norm):
        return {"ok": False, "error": "Codigo invalido para inventario."}

    col = _normalize_text(column_name)
    if col == "codigo":
        return {"ok": False, "error": "La columna 'codigo' no es editable."}

    editable_cols = {
        "descripcion_estandar",
        "unidad_base",
        "stock_actual",
        "ultima_fecha",
        "ultima_modificacion_tipo",
    }
    if col not in editable_cols:
        return {"ok": False, "error": f"Columna no editable: {col}"}

    con = _connect(db_path)
    try:
        # 1) Columnas maestras del catalogo
        if col in {"descripcion_estandar", "unidad_base"}:
            val = _normalize_text(new_value)
            if col == "unidad_base":
                val = val.upper()

            old_cat = con.execute(
                """
                SELECT codigo, descripcion_estandar, unidad_base
                FROM inventario_catalogo
                WHERE codigo = ?
                """,
                (codigo_norm,),
            ).fetchone()

            if old_cat is None:
                desc = val if col == "descripcion_estandar" else ""
                unidad = val if col == "unidad_base" else ""
                con.execute(
                    """
                    INSERT INTO inventario_catalogo
                    (codigo, descripcion_estandar, unidad_base, categoria, tipo, ocurrencias, variaciones, activo, fuente, updated_at)
                    VALUES (?, ?, ?, '', '', 0, 'NO', 1, 'MANUAL_EDICION_CELDA', datetime('now'))
                    """,
                    (codigo_norm, desc, unidad),
                )
                before = ""
            else:
                before = _normalize_text(old_cat[col])
                con.execute(
                    f"""
                    UPDATE inventario_catalogo
                    SET {col} = ?,
                        updated_at = datetime('now')
                    WHERE codigo = ?
                    """,
                    (val, codigo_norm),
                )

            con.commit()
            return {
                "ok": True,
                "action": "catalog_update",
                "codigo": codigo_norm,
                "column": col,
                "before": before,
                "after": val,
            }

        # 2) Stock actual -> ajuste por delta
        if col == "stock_actual":
            parsed = _parse_float_strict(new_value)
            if parsed is None:
                return {"ok": False, "error": "Stock invalido. Ingresa un numero valido."}
            target = float(parsed)
            stock_before = _stock_for_code(con, codigo_norm)
            delta = round(target - stock_before, 6)

            if abs(delta) < 1e-9:
                return {
                    "ok": True,
                    "action": "stock_adjust",
                    "codigo": codigo_norm,
                    "stock_before": stock_before,
                    "stock_after": stock_before,
                    "delta": 0.0,
                    "no_change": True,
                }

            cat = con.execute(
                "SELECT unidad_base FROM inventario_catalogo WHERE codigo = ?",
                (codigo_norm,),
            ).fetchone()
            unidad = _normalize_text(cat["unidad_base"]) if cat else ""
            sign = 1 if delta > 0 else -1
            qty = abs(delta)
            tipo_mov = "AJUSTE_ENTRADA_MANUAL" if sign > 0 else "AJUSTE_SALIDA_MANUAL"

            cur = con.execute(
                """
                INSERT INTO inventario_movimientos
                (codigo, fecha, tipo_mov, cantidad, unidad, signo, fuente, referencia, observacion, source_hash)
                VALUES (?, ?, ?, ?, ?, ?, 'MANUAL_EDICION_CELDA', NULL, ?, NULL)
                """,
                (
                    codigo_norm,
                    date.today().isoformat(),
                    tipo_mov,
                    qty,
                    unidad,
                    sign,
                    f"{observacion}:stock_actual",
                ),
            )
            con.commit()
            stock_after = _stock_for_code(con, codigo_norm)
            return {
                "ok": True,
                "action": "stock_adjust",
                "movement_id": cur.lastrowid,
                "codigo": codigo_norm,
                "stock_before": stock_before,
                "stock_after": stock_after,
                "delta": delta,
            }

        # 3) Fecha de la ultima modificacion
        if col == "ultima_fecha":
            new_fecha = _parse_date_iso_strict(new_value)
            if not new_fecha:
                return {
                    "ok": False,
                    "error": "Fecha invalida. Usa formato YYYY-MM-DD (o DD/MM/YYYY).",
                }

            last_mov = con.execute(
                """
                SELECT id, fecha
                FROM inventario_movimientos
                WHERE codigo = ?
                ORDER BY fecha DESC, id DESC
                LIMIT 1
                """,
                (codigo_norm,),
            ).fetchone()
            if last_mov is None:
                return {"ok": False, "error": "No hay movimientos para este codigo."}

            before = _normalize_text(last_mov["fecha"])
            con.execute(
                "UPDATE inventario_movimientos SET fecha = ? WHERE id = ?",
                (new_fecha, int(last_mov["id"])),
            )
            con.commit()
            return {
                "ok": True,
                "action": "last_date_update",
                "codigo": codigo_norm,
                "movement_id": int(last_mov["id"]),
                "before": before,
                "after": new_fecha,
            }

        # 4) Tipo de ultima modificacion (ENTRADA/SALIDA)
        if col == "ultima_modificacion_tipo":
            normalized = _normalize_text(new_value).upper()
            sign_map = {
                "ENTRADA": 1,
                "E": 1,
                "+": 1,
                "SALIDA": -1,
                "S": -1,
                "-": -1,
            }
            if normalized not in sign_map:
                return {"ok": False, "error": "Tipo invalido. Usa ENTRADA o SALIDA."}

            new_sign = sign_map[normalized]
            last_mov = con.execute(
                """
                SELECT id, signo, tipo_mov, cantidad
                FROM inventario_movimientos
                WHERE codigo = ?
                ORDER BY fecha DESC, id DESC
                LIMIT 1
                """,
                (codigo_norm,),
            ).fetchone()
            if last_mov is None:
                return {"ok": False, "error": "No hay movimientos para este codigo."}

            old_sign = int(last_mov["signo"] or 0)
            old_tipo = _normalize_text(last_mov["tipo_mov"])
            if old_sign == new_sign:
                return {
                    "ok": True,
                    "action": "last_type_update",
                    "codigo": codigo_norm,
                    "movement_id": int(last_mov["id"]),
                    "before": old_tipo,
                    "after": old_tipo,
                    "no_change": True,
                }

            stock_before = _stock_for_code(con, codigo_norm)
            new_tipo = "ENTRADA_MANUAL" if new_sign > 0 else "SALIDA_MANUAL"
            con.execute(
                """
                UPDATE inventario_movimientos
                SET signo = ?, tipo_mov = ?
                WHERE id = ?
                """,
                (new_sign, new_tipo, int(last_mov["id"])),
            )
            con.commit()
            stock_after = _stock_for_code(con, codigo_norm)
            return {
                "ok": True,
                "action": "last_type_update",
                "codigo": codigo_norm,
                "movement_id": int(last_mov["id"]),
                "before": old_tipo,
                "after": new_tipo,
                "stock_before": stock_before,
                "stock_after": stock_after,
            }

        return {"ok": False, "error": "Operacion no soportada."}
    finally:
        con.close()


def get_stock_summary(
    db_path: str,
    search_text: str = "",
    limit: int = 2000,
) -> List[Dict[str, Any]]:
    ensure_inventory_schema(db_path)
    con = _connect(db_path)
    try:
        text_value = _normalize_text(search_text)
        like_value = f"%{text_value}%"
        rows = con.execute(
            """
            WITH base AS (
                SELECT codigo FROM inventario_catalogo
                UNION
                SELECT DISTINCT codigo FROM inventario_movimientos
            ),
            last_mov AS (
                SELECT
                    m.codigo,
                    m.fecha,
                    m.tipo_mov,
                    m.signo,
                    ROW_NUMBER() OVER (
                        PARTITION BY m.codigo
                        ORDER BY m.fecha DESC, m.id DESC
                    ) AS rn
                FROM inventario_movimientos m
            )
            SELECT
                b.codigo AS codigo,
                COALESCE(c.descripcion_estandar, '') AS descripcion_estandar,
                COALESCE(c.unidad_base, '') AS unidad_base,
                COALESCE(c.categoria, '') AS categoria,
                COALESCE(c.tipo, '') AS tipo,
                COALESCE(SUM(CASE WHEN m.signo = 1 THEN m.cantidad ELSE 0 END), 0) AS entradas,
                COALESCE(SUM(CASE WHEN m.signo = -1 THEN m.cantidad ELSE 0 END), 0) AS salidas,
                COALESCE(SUM(m.signo * m.cantidad), 0) AS stock_actual,
                COALESCE(lm.fecha, '') AS ultima_fecha,
                CASE
                    WHEN lm.signo = 1 THEN 'ENTRADA'
                    WHEN lm.signo = -1 THEN 'SALIDA'
                    ELSE COALESCE(lm.tipo_mov, '')
                END AS ultima_modificacion_tipo
            FROM base b
            LEFT JOIN inventario_catalogo c
              ON c.codigo = b.codigo
            LEFT JOIN inventario_movimientos m
              ON m.codigo = b.codigo
            LEFT JOIN last_mov lm
              ON lm.codigo = b.codigo
             AND lm.rn = 1
            WHERE UPPER(TRIM(COALESCE(b.codigo, ''))) NOT IN ('', '-')
              AND UPPER(TRIM(COALESCE(c.categoria, ''))) NOT IN ('EPP', 'MATERIAL', 'HERRAMIENTA', 'CONTABLE', 'RIEGO')
              AND UPPER(TRIM(COALESCE(c.tipo, ''))) NOT IN (
                    'PROTECCION PERSONAL',
                    'INSUMO GENERAL',
                    'EQUIPO',
                    'NO INVENTARIABLE',
                    'INFRAESTRUCTURA'
              )
              AND (
                    (? = '')
                 OR b.codigo LIKE ?
                 OR COALESCE(c.descripcion_estandar, '') LIKE ?
              )
            GROUP BY
                b.codigo, c.descripcion_estandar, c.unidad_base, c.categoria, c.tipo,
                lm.fecha, lm.signo, lm.tipo_mov
            ORDER BY c.categoria, c.tipo, b.codigo
            LIMIT ?
            """,
            (text_value, like_value, like_value, max(1, int(limit))),
        ).fetchall()

        out: List[Dict[str, Any]] = []
        for r in rows:
            if not _is_field_product(r["categoria"], r["tipo"]):
                continue
            out.append(
                {
                    "codigo": r["codigo"],
                    "descripcion_estandar": r["descripcion_estandar"],
                    "unidad_base": r["unidad_base"],
                    "categoria": r["categoria"],
                    "tipo": r["tipo"],
                    "entradas": float(r["entradas"] or 0),
                    "salidas": float(r["salidas"] or 0),
                    "stock_actual": float(r["stock_actual"] or 0),
                    "ultima_fecha": r["ultima_fecha"] or "",
                    "ultima_modificacion_tipo": r["ultima_modificacion_tipo"] or "",
                }
            )
        return out
    finally:
        con.close()


def get_stock_for_code(db_path: str, codigo: str) -> float:
    ensure_inventory_schema(db_path)
    con = _connect(db_path)
    try:
        codigo_norm = _normalize_code(codigo)
        if not _is_valid_inventory_code(codigo_norm):
            return 0.0
        return _stock_for_code(con, codigo_norm)
    finally:
        con.close()


def export_stock_to_excel(db_path: str, output_path: str) -> Dict[str, Any]:
    try:
        from openpyxl import Workbook
    except Exception:
        return {"ok": False, "error": "Falta la dependencia openpyxl para exportar Excel."}

    rows = get_stock_summary(db_path=db_path, search_text="", limit=200000)

    wb = Workbook()
    ws = wb.active
    ws.title = "stock"

    headers = [
        "codigo",
        "descripcion_estandar",
        "unidad_base",
        "categoria",
        "tipo",
        "entradas",
        "salidas",
        "stock_actual",
        "ultima_fecha",
        "ultima_modificacion_tipo",
    ]
    ws.append(headers)

    for r in rows:
        ws.append(
            [
                r["codigo"],
                r["descripcion_estandar"],
                r["unidad_base"],
                r["categoria"],
                r["tipo"],
                r["entradas"],
                r["salidas"],
                r["stock_actual"],
                r["ultima_fecha"],
                r["ultima_modificacion_tipo"],
            ]
        )

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(out))

    return {"ok": True, "path": str(out), "n_rows": len(rows)}
