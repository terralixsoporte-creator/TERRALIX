#!/usr/bin/env python3
"""
Relee PDFs de DTE y repara el detalle en DB.

Flujo:
1) selecciona documentos por --ids o desde --from-report
2) (apply) elimina detalle actual de cada id_doc
3) relee el PDF con ai_reader.read_one_pdf_with_ai()
4) deja reporte con resultado por documento

Ejemplos:
  Dry run:
    python tools/releer_y_reparar_dte.py --db "C:/.../DteRecibidos_db.db" --from-report "C:/.../reporte_coherencia_xxx.txt"

  Aplicar fix:
    python tools/releer_y_reparar_dte.py --db "C:/.../DteRecibidos_db.db" --from-report "C:/.../reporte_coherencia_xxx.txt" --apply

  Lista manual:
    python tools/releer_y_reparar_dte.py --db "C:/.../DteRecibidos_db.db" --ids Factura_Electronica_10031658-7_4430 Factura_Electronica_76754205-4_5348 --apply
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sqlite3
import sys
import unicodedata
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None  # type: ignore[assignment]


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "reportes"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
ENV_PATH = PROJECT_ROOT / "data" / "config.env"
if load_dotenv and ENV_PATH.is_file():
    try:
        load_dotenv(str(ENV_PATH), override=False)
    except Exception:
        pass


@dataclass
class TargetDoc:
    id_doc: str
    ruta_pdf: str
    local_pdf: str
    reason: str = ""


@dataclass
class ResultRow:
    id_doc: str
    status: str
    detail_before: int
    detail_after: int
    local_pdf: str
    note: str = ""


@dataclass
class CleanupStats:
    missing_local_pdf_count: int = 0
    deleted_missing_local_pdf_count: int = 0


def resolve_db_path(cli_db: str | None) -> Path:
    def _normalize_raw_path(raw: str) -> Path:
        s = (raw or "").strip().strip("\"'")
        s = os.path.expandvars(os.path.expanduser(s))
        # Soporta rutas mezcladas C:/.../Base\archivo.db
        s = os.path.normpath(s)
        return Path(s)

    if cli_db:
        return _normalize_raw_path(cli_db).resolve()
    env_db = (os.getenv("DB_PATH_DTE_RECIBIDOS") or "").strip().strip("\"'")
    if env_db:
        p = _normalize_raw_path(env_db)
        if p.is_absolute():
            return p.resolve()
        return (PROJECT_ROOT / p).resolve()
    return (PROJECT_ROOT / "data" / "dte_recibidos.db").resolve()


def count_detalle(con: sqlite3.Connection, id_doc: str) -> int:
    return int(con.execute("SELECT COUNT(*) FROM detalle WHERE id_doc = ?", (id_doc,)).fetchone()[0])


def get_table_columns(con: sqlite3.Connection, table: str) -> list[str]:
    rows = con.execute(f"PRAGMA table_info({table});").fetchall()
    return [r[1] for r in rows]


def backup_detalle_rows(con: sqlite3.Connection, id_doc: str) -> tuple[list[str], list[dict]]:
    cols = get_table_columns(con, "detalle")
    if not cols:
        return [], []
    sql = f"SELECT {', '.join(cols)} FROM detalle WHERE id_doc = ? ORDER BY linea, id_det"
    rows = con.execute(sql, (id_doc,)).fetchall()
    out = [{c: r[idx] for idx, c in enumerate(cols)} for r in rows]
    return cols, out


def restore_detalle_rows(con: sqlite3.Connection, id_doc: str, cols: list[str], rows: list[dict]) -> None:
    with con:
        con.execute("DELETE FROM detalle WHERE id_doc = ?", (id_doc,))
        if not rows or not cols:
            return
        placeholders = ", ".join(["?"] * len(cols))
        sql = f"INSERT INTO detalle ({', '.join(cols)}) VALUES ({placeholders})"
        values = [tuple(r.get(c) for c in cols) for r in rows]
        con.executemany(sql, values)


def make_backup(db_path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = db_path.with_suffix(db_path.suffix + f".bak_releer_{stamp}")
    shutil.copy2(db_path, out)
    return out


def parse_ids_from_report(report_path: Path, issue_type: str) -> list[str]:
    ids: list[str] = []
    pattern = re.compile(rf"^\[[^\]]+\]\s+{re.escape(issue_type)}\s+\|\s+id_doc=([^|]+)\s+\|")
    with report_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = pattern.match(line.strip())
            if m:
                ids.append(m.group(1).strip())
    # unique conservando orden
    out: list[str] = []
    seen = set()
    for x in ids:
        if x not in seen:
            out.append(x)
            seen.add(x)
    return out


def parse_ids_from_report_multi(report_path: Path, issue_types: list[str]) -> list[str]:
    out: list[str] = []
    seen = set()
    for it in issue_types:
        for x in parse_ids_from_report(report_path, it):
            if x not in seen:
                out.append(x)
                seen.add(x)
    return out


def resolve_local_pdf(ruta_pdf: str) -> str:
    if not ruta_pdf:
        return ""
    if ruta_pdf.startswith("http://") or ruta_pdf.startswith("https://"):
        return ""
    p = Path(ruta_pdf.replace("\\", "/"))
    if p.is_file():
        return str(p)
    base_name = p.name
    pdf_dir = (os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "").strip().strip("\"'")
    if pdf_dir:
        alt = Path(pdf_dir) / base_name
        if alt.is_file():
            return str(alt)
    return ""


def collect_targets(
    con: sqlite3.Connection,
    ids: Iterable[str],
    reason: str,
) -> list[TargetDoc]:
    out: list[TargetDoc] = []
    for id_doc in ids:
        row = con.execute(
            "SELECT id_doc, COALESCE(ruta_pdf, '') FROM documentos WHERE id_doc = ?",
            (id_doc,),
        ).fetchone()
        if not row:
            out.append(TargetDoc(id_doc=id_doc, ruta_pdf="", local_pdf="", reason=f"{reason}:id_doc_no_existe"))
            continue
        ruta_pdf = row[1] or ""
        local_pdf = resolve_local_pdf(ruta_pdf)
        out.append(TargetDoc(id_doc=id_doc, ruta_pdf=ruta_pdf, local_pdf=local_pdf, reason=reason))
    return out


def collect_all_doc_ids(con: sqlite3.Connection) -> list[str]:
    rows = con.execute("SELECT id_doc FROM documentos ORDER BY id_doc").fetchall()
    return [r[0] for r in rows if r and r[0]]


def collect_docs_with_missing_local_pdf(con: sqlite3.Connection) -> list[tuple[str, str]]:
    rows = con.execute("SELECT id_doc, COALESCE(ruta_pdf, '') FROM documentos ORDER BY id_doc").fetchall()
    out: list[tuple[str, str]] = []
    for id_doc, ruta_pdf in rows:
        ruta_pdf = (ruta_pdf or "").strip()
        if not id_doc:
            continue
        local = resolve_local_pdf(ruta_pdf)
        if not local:
            out.append((id_doc, ruta_pdf))
    return out


def normalize_text(s: str) -> str:
    txt = str(s or "").strip().lower()
    txt = "".join(c for c in unicodedata.normalize("NFD", txt) if unicodedata.category(c) != "Mn")
    txt = re.sub(r"\s+", " ", txt)
    return txt


def to_float_or_none(value) -> float | None:
    if value is None:
        return None
    try:
        s = str(value).strip()
        if s == "":
            return None
        # tolera formato chileno simple
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", ".")
        return float(s)
    except Exception:
        return None


def build_match_keys(row: dict) -> list[tuple]:
    desc = normalize_text(row.get("descripcion", ""))
    if not desc:
        return []
    monto = to_float_or_none(row.get("monto_item"))
    cantidad = to_float_or_none(row.get("cantidad"))
    precio = to_float_or_none(row.get("precio_unitario"))
    keys: list[tuple] = []
    if (monto is not None) and (cantidad is not None) and (precio is not None):
        keys.append((desc, round(monto, 2), round(cantidad, 6), round(precio, 2)))
    if monto is not None:
        keys.append((desc, round(monto, 2)))
    keys.append((desc,))
    return keys


def recycle_categorization_for_doc(
    con: sqlite3.Connection,
    id_doc: str,
    old_rows: list[dict],
) -> tuple[int, int]:
    """
    Reutiliza clasificación antigua en líneas nuevas cuando coinciden por descripción
    (y monto/cantidad/precio cuando están disponibles).
    """
    if not old_rows:
        return (0, 0)

    table_cols = set(get_table_columns(con, "detalle"))
    recyclable_cols_pref = [
        "catalogo_costo_id",
        "needs_review",
        "categoria",
        "subcategoria",
        "tipo_gasto",
        "confianza_categoria",
        "confianza_subcategoria",
        "origen_clasificacion",
        "motivo_clasificacion",
        "confianza_ia",
    ]
    recyclable_cols = [c for c in recyclable_cols_pref if c in table_cols]
    if not recyclable_cols:
        return (0, 0)

    def has_classification_data(r: dict) -> bool:
        for c in recyclable_cols:
            v = r.get(c)
            if v is None:
                continue
            if isinstance(v, str) and v.strip() == "":
                continue
            return True
        return False

    indexed_old: list[dict] = [r for r in old_rows if has_classification_data(r)]
    if not indexed_old:
        return (0, 0)

    # indice flexible de coincidencia
    key_map: dict[tuple, list[int]] = {}
    for idx, r in enumerate(indexed_old):
        for k in build_match_keys(r):
            key_map.setdefault(k, []).append(idx)

    new_raw = con.execute(
        """
        SELECT id_det, linea, descripcion, cantidad, precio_unitario, monto_item
        FROM detalle
        WHERE id_doc = ?
        ORDER BY linea, id_det
        """,
        (id_doc,),
    ).fetchall()
    new_rows = [
        {
            "id_det": r[0],
            "linea": r[1],
            "descripcion": r[2],
            "cantidad": r[3],
            "precio_unitario": r[4],
            "monto_item": r[5],
        }
        for r in new_raw
    ]
    if not new_rows:
        return (0, 0)

    used_old: set[int] = set()
    reused = 0
    with con:
        for nr in new_rows:
            picked_idx = None
            for k in build_match_keys(nr):
                cand = key_map.get(k, [])
                for old_idx in cand:
                    if old_idx not in used_old:
                        picked_idx = old_idx
                        break
                if picked_idx is not None:
                    break
            if picked_idx is None:
                continue

            old = indexed_old[picked_idx]
            used_old.add(picked_idx)
            set_sql = ", ".join([f"{c} = ?" for c in recyclable_cols])
            vals = [old.get(c) for c in recyclable_cols] + [nr["id_det"]]
            con.execute(f"UPDATE detalle SET {set_sql} WHERE id_det = ?", vals)
            reused += 1

    return (reused, len(new_rows))


def write_report(
    path: Path,
    db_path: Path,
    rows: list[ResultRow],
    apply_mode: bool,
    backup: Path | None,
    cleanup: CleanupStats | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ok = len([r for r in rows if r.status == "OK"])
    err = len([r for r in rows if r.status != "OK"])
    with path.open("w", encoding="utf-8") as f:
        f.write("REPORTE RELECTURA/FIX DTE\n")
        f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"DB: {db_path}\n")
        f.write(f"apply: {apply_mode}\n")
        if backup:
            f.write(f"backup: {backup}\n")
        f.write("=" * 100 + "\n\n")
        f.write(f"OK: {ok}\n")
        f.write(f"ERROR: {err}\n\n")
        if cleanup is not None:
            f.write("LIMPIEZA\n")
            f.write(f"- docs_sin_pdf_local_detectados: {cleanup.missing_local_pdf_count}\n")
            f.write(f"- docs_sin_pdf_local_eliminados: {cleanup.deleted_missing_local_pdf_count}\n\n")
        for r in rows:
            f.write(
                f"[{r.status}] id_doc={r.id_doc} | detalle_before={r.detail_before} | "
                f"detalle_after={r.detail_after} | pdf={r.local_pdf or '-'}"
            )
            if r.note:
                f.write(f" | note={r.note}")
            f.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Relee DTE y repara detalle en DB.")
    parser.add_argument("--db", help="Ruta DB SQLite.")
    parser.add_argument("--ids", nargs="*", default=[], help="Lista de id_doc a releer.")
    parser.add_argument("--all-docs", action="store_true", help="Relee todos los documentos de la DB.")
    parser.add_argument("--from-report", help="Reporte de auditoria para extraer id_doc.")
    parser.add_argument(
        "--issue-type",
        default="detalle_no_coincide_con_pdf",
        help=(
            "Tipo(s) de hallazgo a extraer del reporte. "
            "Puedes pasar uno o varios separados por coma. "
            "Ej: detalle_no_coincide_con_pdf,documento_sin_detalle"
        ),
    )
    parser.add_argument("--limit", type=int, default=0, help="Limitar cantidad de documentos.")
    parser.add_argument(
        "--delete-missing-local-pdf",
        action="store_true",
        help="Elimina de la DB documentos cuyo PDF local no existe (ruta no http/https).",
    )
    parser.add_argument("--debug", action="store_true", help="Debug de lectura IA.")
    parser.add_argument("--apply", action="store_true", help="Aplica cambios. Sin esto solo simula.")
    parser.add_argument("--report", help="Ruta de salida del reporte.")
    args = parser.parse_args()

    db_path = resolve_db_path(args.db)
    if not db_path.is_file():
        raise SystemExit(f"DB no encontrada: {db_path}")
    # Obliga a que ai_reader/dte_loader use esta misma DB en runtime
    os.environ["DB_PATH_DTE_RECIBIDOS"] = str(db_path)

    report_path = (
        Path(args.report).expanduser().resolve()
        if args.report
        else (DEFAULT_REPORT_DIR / f"relectura_fix_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt").resolve()
    )

    ids: list[str] = list(args.ids or [])
    if args.from_report:
        rp = Path(args.from_report).expanduser().resolve()
        if not rp.is_file():
            raise SystemExit(f"Reporte no encontrado: {rp}")
        issue_types = [x.strip() for x in str(args.issue_type or "").split(",") if x.strip()]
        if not issue_types:
            issue_types = ["detalle_no_coincide_con_pdf"]
        ids.extend(parse_ids_from_report_multi(rp, issue_types))

    # unique manteniendo orden
    uniq: list[str] = []
    seen = set()
    for x in ids:
        if x and x not in seen:
            uniq.append(x)
            seen.add(x)

    # Import tardio para no cargar OpenAI al pedir ayuda
    from app.core.DTE_Recibidos import ai_reader as AIR

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON;")
    backup_path: Path | None = None
    results: list[ResultRow] = []
    cleanup = CleanupStats()
    try:
        # cleanup opcional: documentos sin PDF local
        if args.delete_missing_local_pdf:
            missing_docs = collect_docs_with_missing_local_pdf(con)
            cleanup.missing_local_pdf_count = len(missing_docs)
            if args.apply and missing_docs:
                if backup_path is None:
                    backup_path = make_backup(db_path)
                with con:
                    for id_doc, _ruta in missing_docs:
                        con.execute("DELETE FROM documentos WHERE id_doc = ?", (id_doc,))
                cleanup.deleted_missing_local_pdf_count = len(missing_docs)

        # Construccion de lista objetivo
        if args.all_docs:
            ids_source = collect_all_doc_ids(con)
            # si tambien llega --ids / --from-report se unen
            merged = ids_source + uniq
            uniq = []
            seen2 = set()
            for x in merged:
                if x and x not in seen2:
                    uniq.append(x)
                    seen2.add(x)

        if args.limit and args.limit > 0:
            uniq = uniq[: args.limit]
        if not uniq:
            # permite correr solo limpieza y salir con reporte
            write_report(report_path, db_path, results, args.apply, backup_path, cleanup)
            print(f"Reporte: {report_path}")
            print("OK: 0 | ERROR: 0")
            if backup_path:
                print(f"Backup: {backup_path}")
            return

        targets = collect_targets(con, uniq, reason=args.issue_type)

        if args.apply and backup_path is None:
            backup_path = make_backup(db_path)

        for t in targets:
            before = count_detalle(con, t.id_doc) if t.id_doc else 0
            detail_cols: list[str] = []
            detail_rows_old: list[dict] = []
            if not t.id_doc:
                results.append(ResultRow(id_doc=t.id_doc, status="ERROR", detail_before=before, detail_after=before, local_pdf=t.local_pdf, note="id_doc_vacio"))
                continue
            if not t.local_pdf or not os.path.isfile(t.local_pdf):
                results.append(
                    ResultRow(
                        id_doc=t.id_doc,
                        status="ERROR",
                        detail_before=before,
                        detail_after=before,
                        local_pdf=t.local_pdf,
                        note="pdf_local_no_encontrado",
                    )
                )
                continue

            if not args.apply:
                results.append(
                    ResultRow(
                        id_doc=t.id_doc,
                        status="OK",
                        detail_before=before,
                        detail_after=before,
                        local_pdf=t.local_pdf,
                        note="dry_run",
                    )
                )
                continue

            try:
                detail_cols, detail_rows_old = backup_detalle_rows(con, t.id_doc)
                with con:
                    con.execute("DELETE FROM detalle WHERE id_doc = ?", (t.id_doc,))

                res = AIR.read_one_pdf_with_ai(t.local_pdf, debug=args.debug)
                if not res.get("ok"):
                    restore_detalle_rows(con, t.id_doc, detail_cols, detail_rows_old)
                    # deja rastro: al menos no perdemos por completo, marcamos error
                    after = count_detalle(con, t.id_doc)
                    results.append(
                        ResultRow(
                            id_doc=t.id_doc,
                            status="ERROR",
                            detail_before=before,
                            detail_after=after,
                            local_pdf=t.local_pdf,
                            note=f"lectura_ia_fallo:{res.get('error')}",
                        )
                    )
                    continue

                # si read_one_pdf_with_ai retorna doc_id distinto, lo informamos
                returned_doc = str(res.get("doc_id") or "")
                after = count_detalle(con, t.id_doc)
                note = ""
                if returned_doc and returned_doc != t.id_doc:
                    alt_after = count_detalle(con, returned_doc)
                    note = f"doc_id_releido_distinto:{returned_doc}; detalle_target={after}; detalle_alt={alt_after}"
                if after <= 0:
                    restore_detalle_rows(con, t.id_doc, detail_cols, detail_rows_old)
                    after = count_detalle(con, t.id_doc)
                    results.append(
                        ResultRow(
                            id_doc=t.id_doc,
                            status="ERROR",
                            detail_before=before,
                            detail_after=after,
                            local_pdf=t.local_pdf,
                            note=note or "sin_detalle_post_relectura; detalle_restaurado",
                        )
                    )
                    continue

                # Reciclar clasificacion previa cuando la descripcion coincide.
                try:
                    reused, total_new = recycle_categorization_for_doc(con, t.id_doc, detail_rows_old)
                    recycle_note = f"cat_reciclada={reused}/{total_new}"
                    note = f"{note}; {recycle_note}" if note else recycle_note
                except Exception as e:
                    recycle_note = f"cat_reciclada_error:{e}"
                    note = f"{note}; {recycle_note}" if note else recycle_note

                results.append(
                    ResultRow(
                        id_doc=t.id_doc,
                        status="OK",
                        detail_before=before,
                        detail_after=after,
                        local_pdf=t.local_pdf,
                        note=note or "releido_ok",
                    )
                )
            except Exception as e:
                try:
                    # restauracion de emergencia si algo rompe a mitad de proceso
                    if before > 0 and count_detalle(con, t.id_doc) == 0 and detail_rows_old:
                        restore_detalle_rows(con, t.id_doc, detail_cols, detail_rows_old)
                except Exception:
                    pass
                after = count_detalle(con, t.id_doc)
                results.append(
                    ResultRow(
                        id_doc=t.id_doc,
                        status="ERROR",
                        detail_before=before,
                        detail_after=after,
                        local_pdf=t.local_pdf,
                        note=f"exception:{e}",
                    )
                )

        write_report(report_path, db_path, results, args.apply, backup_path, cleanup)
        ok = len([r for r in results if r.status == "OK"])
        err = len([r for r in results if r.status != "OK"])
        print(f"Reporte: {report_path}")
        print(f"OK: {ok} | ERROR: {err}")
        if backup_path:
            print(f"Backup: {backup_path}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
