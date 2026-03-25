"""
Sincronización bidireccional SQLite <-> Excel para revisión manual.

Flujo:
  1. export_to_excel()  → Genera Excel con detalle + catálogo + dropdowns
  2. Usuario revisa/corrige en Excel
  3. import_from_excel() → Detecta cambios, actualiza DB, marca MANUAL
  4. retrain_if_changed() → Reentrena modelo ML con correcciones

El Excel tiene:
  - Hoja "detalle": filas editables con dropdowns de categoría
  - Hoja "catalogo": referencia de combinaciones válidas
  - Hoja "documentos": datos de documentos (solo lectura)
  - Las filas SIN_CLASIFICAR y needs_review=1 se resaltan en amarillo/naranja
"""

from __future__ import annotations

import os
import sqlite3
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from openpyxl.worksheet.datavalidation import DataValidation

# ─── Colores ───
_HEADER_FILL   = PatternFill("solid", fgColor="2B4C7E")
_HEADER_FONT   = Font(bold=True, color="FFFFFF", size=11)
_REVIEW_FILL   = PatternFill("solid", fgColor="FFF3CD")   # amarillo: needs_review
_SIN_CLAS_FILL = PatternFill("solid", fgColor="F8D7DA")   # rojo claro: SIN_CLASIFICAR
_MANUAL_FILL   = PatternFill("solid", fgColor="D4EDDA")   # verde: corregido manual
_LOCKED_FONT   = Font(color="888888", italic=True)
_THIN_BORDER   = Border(
    left=Side(style="thin"), right=Side(style="thin"),
    top=Side(style="thin"), bottom=Side(style="thin"),
)

# Columnas de detalle que se exportan (orden importa)
_DETALLE_COLS = [
    "id_det", "id_doc", "linea", "descripcion", "cantidad",
    "precio_unitario", "monto_item",
    "razon_social", "giro", "fecha_emision",
    "categoria", "subcategoria", "tipo_gasto",
    "catalogo_costo_id", "confianza_categoria", "needs_review",
    "origen_clasificacion", "motivo_clasificacion",
]

# Columnas editables por el usuario (el resto es solo lectura visual)
_EDITABLE_COLS = {"categoria", "subcategoria", "tipo_gasto"}

# Columna oculta para detectar cambios
_HASH_COL = "hash_original"


def _row_hash(cat: str, sub: str, tipo: str) -> str:
    """Hash MD5 corto de la clasificación original para detectar cambios."""
    raw = f"{(cat or '').strip().upper()}|{(sub or '').strip().upper()}|{(tipo or '').strip().upper()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


# =============================================================================
# EXPORTAR
# =============================================================================

def export_to_excel(
    db_path: str,
    output_path: str,
    only_pending: bool = False,
) -> Dict[str, Any]:
    """
    Exporta la DB a Excel con formato para revisión manual.

    Args:
        db_path: ruta a DteRecibidos_db.db
        output_path: ruta del Excel de salida
        only_pending: si True, solo exporta needs_review=1 y SIN_CLASIFICAR

    Returns:
        {"ok": True, "n_rows": int, "path": str}
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # --- Catálogo ---
    catalogo = con.execute(
        "SELECT id, categoria_costo, subcategoria_costo, tipo_gasto "
        "FROM catalogo_costos ORDER BY categoria_costo, subcategoria_costo, tipo_gasto"
    ).fetchall()
    categorias_unicas = sorted({r["categoria_costo"] for r in catalogo if r["categoria_costo"]})

    # --- Detalle ---
    where = ""
    if only_pending:
        where = "WHERE d.needs_review = 1 OR d.categoria = 'SIN_CLASIFICAR' OR d.categoria IS NULL"

    detalle_rows = con.execute(f"""
        SELECT
            d.id_det, d.id_doc, d.linea, d.descripcion, d.cantidad,
            d.precio_unitario, d.monto_item,
            COALESCE(d.razon_social, doc.razon_social, '') AS razon_social,
            COALESCE(d.giro, doc.giro, '') AS giro,
            COALESCE(d.fecha_emision, doc.fecha_emision, '') AS fecha_emision,
            d.categoria, d.subcategoria, d.tipo_gasto,
            d.catalogo_costo_id, d.confianza_categoria, d.needs_review,
            d.origen_clasificacion, d.motivo_clasificacion
        FROM detalle d
        LEFT JOIN documentos doc ON doc.id_doc = d.id_doc
        {where}
        ORDER BY d.fecha_emision DESC, d.id_doc, d.linea
    """).fetchall()

    # --- Documentos ---
    doc_rows = con.execute("""
        SELECT id_doc, tipo_doc, folio, fecha_emision, rut_emisor,
               razon_social, giro, monto_total
        FROM documentos ORDER BY fecha_emision DESC
    """).fetchall()
    con.close()

    # === Crear Excel ===
    wb = Workbook()

    # ── Hoja DETALLE ──
    ws_det = wb.active
    ws_det.title = "detalle"
    headers = _DETALLE_COLS + [_HASH_COL]

    # Headers
    for col_idx, header in enumerate(headers, 1):
        cell = ws_det.cell(row=1, column=col_idx, value=header)
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.alignment = Alignment(horizontal="center")
        cell.border = _THIN_BORDER

    # Data
    for row_idx, r in enumerate(detalle_rows, 2):
        cat  = (r["categoria"]    or "").strip()
        sub  = (r["subcategoria"] or "").strip()
        tipo = (r["tipo_gasto"]   or "").strip()
        h    = _row_hash(cat, sub, tipo)

        for col_idx, col_name in enumerate(headers, 1):
            if col_name == _HASH_COL:
                value = h
            else:
                value = r[col_name] if col_name in r.keys() else ""
            cell = ws_det.cell(row=row_idx, column=col_idx, value=value)
            cell.border = _THIN_BORDER

            # Colorear según estado
            nr = r["needs_review"]
            if cat.upper() == "SIN_CLASIFICAR" or not cat:
                cell.fill = _SIN_CLAS_FILL
            elif nr and int(nr) == 1:
                cell.fill = _REVIEW_FILL

            # Columnas no editables en gris
            if col_name not in _EDITABLE_COLS:
                cell.font = _LOCKED_FONT

    # Dropdown de categoría
    cat_col_idx = headers.index("categoria") + 1
    cat_formula = '"' + ",".join(categorias_unicas) + '"'
    dv_cat = DataValidation(type="list", formula1=cat_formula, allow_blank=True)
    dv_cat.error = "Selecciona una categoría válida del catálogo"
    dv_cat.errorTitle = "Categoría inválida"
    dv_cat.prompt = "Selecciona la categoría"
    dv_cat.promptTitle = "Categoría"
    ws_det.add_data_validation(dv_cat)
    if len(detalle_rows) > 0:
        col_letter = get_column_letter(cat_col_idx)
        dv_cat.add(f"{col_letter}2:{col_letter}{len(detalle_rows) + 1}")

    # Ocultar columna hash
    hash_col_letter = get_column_letter(len(headers))
    ws_det.column_dimensions[hash_col_letter].hidden = True

    # Auto-ajustar anchos
    for col_idx, header in enumerate(headers, 1):
        max_len = len(str(header))
        for row_idx in range(2, min(len(detalle_rows) + 2, 50)):
            cell_val = ws_det.cell(row=row_idx, column=col_idx).value
            if cell_val:
                max_len = max(max_len, min(len(str(cell_val)), 40))
        ws_det.column_dimensions[get_column_letter(col_idx)].width = max_len + 2

    # Filtros
    ws_det.auto_filter.ref = f"A1:{get_column_letter(len(headers))}{len(detalle_rows) + 1}"
    # Congelar primera fila
    ws_det.freeze_panes = "A2"

    # ── Hoja CATALOGO ──
    ws_cat = wb.create_sheet("catalogo")
    cat_headers = ["id", "categoria_costo", "subcategoria_costo", "tipo_gasto"]
    for col_idx, header in enumerate(cat_headers, 1):
        cell = ws_cat.cell(row=1, column=col_idx, value=header)
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.border = _THIN_BORDER
    for row_idx, r in enumerate(catalogo, 2):
        for col_idx, col_name in enumerate(cat_headers, 1):
            cell = ws_cat.cell(row=row_idx, column=col_idx, value=r[col_name])
            cell.border = _THIN_BORDER
    ws_cat.freeze_panes = "A2"

    # ── Hoja DOCUMENTOS ──
    ws_doc = wb.create_sheet("documentos")
    doc_headers = ["id_doc", "tipo_doc", "folio", "fecha_emision",
                   "rut_emisor", "razon_social", "giro", "monto_total"]
    for col_idx, header in enumerate(doc_headers, 1):
        cell = ws_doc.cell(row=1, column=col_idx, value=header)
        cell.fill = _HEADER_FILL
        cell.font = _HEADER_FONT
        cell.border = _THIN_BORDER
    for row_idx, r in enumerate(doc_rows, 2):
        for col_idx, col_name in enumerate(doc_headers, 1):
            val = r[col_name] if col_name in r.keys() else ""
            cell = ws_doc.cell(row=row_idx, column=col_idx, value=val)
            cell.border = _THIN_BORDER
    ws_doc.freeze_panes = "A2"

    # Guardar
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    wb.save(output_path)

    return {
        "ok": True,
        "n_rows": len(detalle_rows),
        "n_catalogo": len(catalogo),
        "n_documentos": len(doc_rows),
        "path": output_path,
    }


# =============================================================================
# IMPORTAR
# =============================================================================

def import_from_excel(
    excel_path: str,
    db_path: str,
) -> Dict[str, Any]:
    """
    Lee el Excel, detecta cambios en categoría/subcategoría/tipo_gasto,
    y actualiza la DB marcando origen='MANUAL'.

    Returns:
        {"ok": True, "n_changed": int, "changes": [...]}
    """
    wb = load_workbook(excel_path, read_only=True, data_only=True)
    if "detalle" not in wb.sheetnames:
        return {"ok": False, "error": "No se encontró la hoja 'detalle'"}

    ws = wb["detalle"]

    # Leer headers
    headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    if "id_det" not in headers:
        return {"ok": False, "error": "Columna 'id_det' no encontrada"}

    col_map = {h: i for i, h in enumerate(headers) if h}

    # Validar columnas necesarias
    for needed in ("id_det", "categoria", "subcategoria", "tipo_gasto", _HASH_COL):
        if needed not in col_map:
            return {"ok": False, "error": f"Columna '{needed}' no encontrada"}

    # Leer catálogo válido de la DB
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    valid_combos = set()
    catalogo_lookup: Dict[str, int] = {}
    for r in con.execute("SELECT id, categoria_costo, subcategoria_costo, tipo_gasto FROM catalogo_costos"):
        key = f"{r['categoria_costo']}|{r['subcategoria_costo']}|{r['tipo_gasto']}"
        valid_combos.add(key)
        catalogo_lookup[key] = int(r["id"])

    # Detectar cambios
    changes: List[Dict[str, Any]] = []
    warnings: List[str] = []

    for row in ws.iter_rows(min_row=2, values_only=True):
        if not row or not row[col_map["id_det"]]:
            continue

        id_det   = str(row[col_map["id_det"]])
        new_cat  = (str(row[col_map["categoria"]]    or "")).strip().upper()
        new_sub  = (str(row[col_map["subcategoria"]] or "")).strip().upper()
        new_tipo = (str(row[col_map["tipo_gasto"]]   or "")).strip().upper()
        old_hash = str(row[col_map[_HASH_COL]] or "")

        new_hash = _row_hash(new_cat, new_sub, new_tipo)
        if new_hash == old_hash:
            continue  # Sin cambios

        # Validar combinación
        combo_key = f"{new_cat}|{new_sub}|{new_tipo}"
        catalogo_id = catalogo_lookup.get(combo_key)
        if catalogo_id is None:
            # Intentar con tipo_gasto=OTRO
            fallback_key = f"{new_cat}|{new_sub}|OTRO"
            catalogo_id = catalogo_lookup.get(fallback_key)
            if catalogo_id is None:
                fallback_key2 = f"{new_cat}|OTRO|OTRO"
                catalogo_id = catalogo_lookup.get(fallback_key2)
            if catalogo_id is not None:
                # Corregir al valor válido más cercano
                matched = fallback_key if f"{new_cat}|{new_sub}|OTRO" in catalogo_lookup else fallback_key2
                parts = matched.split("|")
                new_sub  = parts[1]
                new_tipo = parts[2]
                warnings.append(f"{id_det}: ajustado a {matched}")
            else:
                warnings.append(
                    f"{id_det}: combinación inválida {combo_key}, ignorando"
                )
                continue

        changes.append({
            "id_det":          id_det,
            "categoria":       new_cat,
            "subcategoria":    new_sub,
            "tipo_gasto":      new_tipo,
            "catalogo_costo_id": catalogo_id,
        })

    wb.close()

    # Aplicar cambios a la DB
    if changes:
        for ch in changes:
            con.execute("""
                UPDATE detalle
                SET categoria = ?,
                    subcategoria = ?,
                    tipo_gasto = ?,
                    catalogo_costo_id = ?,
                    needs_review = 0,
                    origen_clasificacion = 'MANUAL',
                    motivo_clasificacion = ?,
                    confianza_categoria = 100,
                    confianza_subcategoria = 100
                WHERE id_det = ?
            """, (
                ch["categoria"],
                ch["subcategoria"],
                ch["tipo_gasto"],
                ch["catalogo_costo_id"],
                f"correccion_manual:{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                ch["id_det"],
            ))
        con.commit()

    con.close()

    return {
        "ok":        True,
        "n_changed": len(changes),
        "changes":   changes,
        "warnings":  warnings,
    }


# =============================================================================
# SYNC COMPLETO: importar + reentrenar
# =============================================================================

def sync_and_retrain(
    excel_path: str,
    db_path: str,
    model_path: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Importa cambios del Excel y reentrena el modelo ML si hubo cambios.
    """
    # 1. Importar cambios
    import_result = import_from_excel(excel_path, db_path)
    if not import_result.get("ok"):
        return import_result

    n_changed = import_result["n_changed"]
    print(f"[SYNC] {n_changed} filas modificadas desde Excel")

    if n_changed == 0:
        return {
            "ok":        True,
            "n_changed": 0,
            "retrained": False,
            "message":   "Sin cambios detectados",
        }

    # 2. Reentrenar modelo
    try:
        from app.core.DTE_Recibidos.local_classifier import get_classifier
    except ImportError:
        from local_classifier import get_classifier

    clf = get_classifier(db_path, model_path)
    train_result = clf.retrain()

    return {
        "ok":           True,
        "n_changed":    n_changed,
        "changes":      import_result["changes"],
        "warnings":     import_result.get("warnings", []),
        "retrained":    train_result.get("ok", False),
        "train_result": train_result,
    }


# =============================================================================
# RELLENAR columnas desnormalizadas para filas existentes
# =============================================================================

def backfill_denormalized_columns(db_path: str) -> int:
    """
    Rellena razon_social, giro, fecha_emision en detalle
    desde la tabla documentos, para filas que aún no las tienen.
    Retorna cantidad de filas actualizadas.
    """
    con = sqlite3.connect(db_path)
    cur = con.execute("""
        UPDATE detalle
        SET razon_social = (
                SELECT COALESCE(doc.razon_social, '')
                FROM documentos doc WHERE doc.id_doc = detalle.id_doc
            ),
            giro = (
                SELECT COALESCE(doc.giro, '')
                FROM documentos doc WHERE doc.id_doc = detalle.id_doc
            ),
            fecha_emision = (
                SELECT COALESCE(doc.fecha_emision, '')
                FROM documentos doc WHERE doc.id_doc = detalle.id_doc
            )
        WHERE razon_social IS NULL OR razon_social = ''
    """)
    n = cur.rowcount
    con.commit()
    con.close()
    return n


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sincronización Excel <-> SQLite")
    parser.add_argument("action", choices=["export", "import", "sync", "backfill"],
                        help="export: DB→Excel | import: Excel→DB | sync: import+retrain | backfill: llenar columnas")
    parser.add_argument("--db", default=None, help="Ruta a DteRecibidos_db.db")
    parser.add_argument("--excel", default=None, help="Ruta del Excel")
    parser.add_argument("--pending", action="store_true",
                        help="Solo exportar filas pendientes de revisión")
    args = parser.parse_args()

    # Resolver DB desde config.env
    if not args.db:
        base = Path(__file__).resolve().parents[3]
        env_path = base / "data" / "config.env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("DB_PATH_DTE_RECIBIDOS"):
                    args.db = line.split("=", 1)[1].strip().strip("'\"")
                    break

    if not args.db:
        print("[ERROR] Especifica --db o configura DB_PATH_DTE_RECIBIDOS")
        exit(1)

    default_excel = str(Path(args.db).parent / "DteRecibidos_revision.xlsx")
    excel = args.excel or default_excel

    if args.action == "export":
        result = export_to_excel(args.db, excel, only_pending=args.pending)
        print(f"[EXPORT] {result}")

    elif args.action == "import":
        result = import_from_excel(excel, args.db)
        print(f"[IMPORT] {result}")

    elif args.action == "sync":
        result = sync_and_retrain(excel, args.db)
        print(f"[SYNC] {result}")

    elif args.action == "backfill":
        n = backfill_denormalized_columns(args.db)
        print(f"[BACKFILL] {n} filas actualizadas")
