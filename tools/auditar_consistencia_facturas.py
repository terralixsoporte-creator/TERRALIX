#!/usr/bin/env python3
"""
Audita coherencia basica entre documentos/detalle y permite corregir IDs mal armados
usando el nombre del PDF (Tipo_RUT_Folio.pdf).

Uso recomendado:
1) Dry run:
   python tools/auditar_consistencia_facturas.py --db "C:/ruta/DteRecibidos_db.db"
2) Aplicar solo correccion de IDs por filename (con respaldo automatico):
   python tools/auditar_consistencia_facturas.py --db "C:/ruta/DteRecibidos_db.db" --fix-id-from-filename --apply
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import shutil
import sqlite3
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = PROJECT_ROOT / "data" / "dte_recibidos.db"
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "reportes"


@dataclass
class Issue:
    issue_type: str
    severity: str
    id_doc: str = ""
    expected_id_doc: str = ""
    ruta_pdf: str = ""
    detail: str = ""


@dataclass
class FixPlan:
    action: str
    old_id: str
    new_id: str = ""
    note: str = ""


def collapse_spaces(text: str) -> str:
    return re.sub(r"\s{2,}", " ", (text or "").strip())


def parse_rut(raw: str) -> str:
    if not raw:
        return ""
    s = str(raw)
    if ":" in s:
        s = s.split(":")[-1]
    s = s.upper()
    only = re.sub(r"[^0-9K]", "", s)
    if len(only) < 2:
        return ""
    body, dv = only[:-1], only[-1]
    if not re.fullmatch(r"[0-9K]", dv):
        return ""
    body = body.lstrip("0") or "0"
    return f"{body}-{dv}"


def parse_folio(raw: str) -> str:
    nums = re.findall(r"\d+", str(raw or ""))
    return nums[-1] if nums else "0"


def infer_identity_from_pdf_path(ruta_pdf: str) -> tuple[str, str, str] | None:
    if not ruta_pdf:
        return None
    raw_name = os.path.basename(str(ruta_pdf)).split("?", 1)[0]
    stem = os.path.splitext(raw_name)[0]
    parts = [p for p in stem.split("_") if p]
    if len(parts) < 3:
        return None

    work = parts[:]
    folio = "0"
    rut = ""

    for i in range(len(work) - 1, -1, -1):
        f = parse_folio(work[i])
        if f and f != "0":
            folio = f
            work.pop(i)
            break

    for i in range(len(work) - 1, -1, -1):
        r = parse_rut(work[i])
        if r:
            rut = r
            work.pop(i)
            break

    tipo = collapse_spaces(" ".join(work)) if work else ""
    if not (tipo and rut and folio and folio != "0"):
        return None
    return tipo, rut, folio


def build_id_doc(tipo_doc: str, rut_emisor: str, folio: str) -> str:
    tipo = collapse_spaces(str(tipo_doc)).replace("/", ":").replace("\\", "_")
    tipo = re.sub(r"\s+", "_", tipo)
    rut = (rut_emisor or "").replace(".", "")
    return f"{tipo}_{rut}_{folio}"


def normalize_match_text(text: str) -> str:
    s = (text or "").lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def token_overlap_ratio(needle: str, haystack: str) -> float:
    tokens = [t for t in normalize_match_text(needle).split() if len(t) >= 3]
    if not tokens:
        return 1.0
    hay = set(normalize_match_text(haystack).split())
    hit = sum(1 for t in tokens if t in hay)
    return hit / len(tokens)


def resolve_local_pdf_path(ruta_pdf: str) -> Path | None:
    if not ruta_pdf:
        return None
    if ruta_pdf.startswith("http://") or ruta_pdf.startswith("https://"):
        return None
    p = Path(ruta_pdf.replace("\\", "/"))
    if p.is_file():
        return p
    alt = Path(os.getenv("RUTA_PDF_DTE_RECIBIDOS", "")).expanduser() / p.name
    if alt.is_file():
        return alt
    return None


def resolve_db_path(cli_db: str | None) -> Path:
    if cli_db:
        return Path(cli_db).expanduser().resolve()

    env_db = (os.getenv("DB_PATH_DTE_RECIBIDOS") or "").strip().strip("\"'")
    if env_db:
        p = Path(env_db)
        if p.is_absolute():
            return p.resolve()
        return (PROJECT_ROOT / env_db).resolve()
    return DEFAULT_DB.resolve()


def make_backup(db_path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = db_path.with_suffix(db_path.suffix + f".bak_{stamp}")
    shutil.copy2(db_path, backup)
    return backup


def fetchall_rows(con: sqlite3.Connection, sql: str, args: tuple = ()) -> list[sqlite3.Row]:
    cur = con.execute(sql, args)
    return cur.fetchall()


def collect_issues_and_plans(con: sqlite3.Connection) -> tuple[list[Issue], list[FixPlan]]:
    issues: list[Issue] = []
    plans: list[FixPlan] = []

    docs = fetchall_rows(
        con,
        """
        SELECT
            d.id_doc,
            COALESCE(d.ruta_pdf, '') AS ruta_pdf,
            COUNT(t.id_det) AS detalle_count
        FROM documentos d
        LEFT JOIN detalle t ON t.id_doc = d.id_doc
        GROUP BY d.id_doc, d.ruta_pdf
        ORDER BY d.id_doc
        """,
    )

    docs_by_id = {r["id_doc"]: r for r in docs}

    # IDs mal formados / mismatch con filename
    for r in docs:
        id_doc = (r["id_doc"] or "").strip()
        ruta_pdf = (r["ruta_pdf"] or "").strip()
        detalle_count = int(r["detalle_count"] or 0)

        if "__" in id_doc or id_doc.endswith("_0"):
            issues.append(
                Issue(
                    issue_type="malformed_id_doc",
                    severity="high",
                    id_doc=id_doc,
                    ruta_pdf=ruta_pdf,
                    detail="Patron sospechoso de id_doc",
                )
            )

        inferred = infer_identity_from_pdf_path(ruta_pdf)
        if not inferred:
            continue
        expected = build_id_doc(*inferred)
        if expected == id_doc:
            continue

        issues.append(
            Issue(
                issue_type="id_doc_mismatch_filename",
                severity="high",
                id_doc=id_doc,
                expected_id_doc=expected,
                ruta_pdf=ruta_pdf,
                detail="id_doc no coincide con identidad inferida desde el nombre del PDF",
            )
        )

        target = docs_by_id.get(expected)
        if target is None:
            plans.append(FixPlan(action="rename_id_doc", old_id=id_doc, new_id=expected))
            continue

        target_det = int(target["detalle_count"] or 0)
        if detalle_count == 0:
            plans.append(
                FixPlan(
                    action="delete_header_only",
                    old_id=id_doc,
                    new_id=expected,
                    note="Existe documento correcto y este no tiene detalle",
                )
            )
        elif target_det == 0:
            plans.append(
                FixPlan(
                    action="move_details_to_expected",
                    old_id=id_doc,
                    new_id=expected,
                    note="Mover detalle al documento correcto",
                )
            )
        else:
            plans.append(
                FixPlan(
                    action="conflict_manual_review",
                    old_id=id_doc,
                    new_id=expected,
                    note="Ambos documentos tienen detalle",
                )
            )

    # Documentos sin detalle
    for r in docs:
        if int(r["detalle_count"] or 0) == 0:
            issues.append(
                Issue(
                    issue_type="documento_sin_detalle",
                    severity="medium",
                    id_doc=r["id_doc"],
                    ruta_pdf=r["ruta_pdf"],
                    detail="Documento existe en cabecera pero no tiene lineas en detalle",
                )
            )

    # Huérfanos en detalle
    orphans = fetchall_rows(
        con,
        """
        SELECT t.id_det, t.id_doc
        FROM detalle t
        LEFT JOIN documentos d ON d.id_doc = t.id_doc
        WHERE d.id_doc IS NULL
        """,
    )
    for r in orphans:
        issues.append(
            Issue(
                issue_type="detalle_huerfano",
                severity="high",
                id_doc=r["id_doc"],
                detail=f"id_det={r['id_det']}",
            )
        )

    # Duplicidad de linea por documento
    dup_lineas = fetchall_rows(
        con,
        """
        SELECT id_doc, linea, COUNT(*) AS c
        FROM detalle
        GROUP BY id_doc, linea
        HAVING COUNT(*) > 1
        """,
    )
    for r in dup_lineas:
        issues.append(
            Issue(
                issue_type="linea_duplicada_por_documento",
                severity="high",
                id_doc=r["id_doc"],
                detail=f"linea={r['linea']} count={r['c']}",
            )
        )

    # Descripcion vacia
    empty_desc = fetchall_rows(
        con,
        """
        SELECT id_doc, COUNT(*) AS c
        FROM detalle
        WHERE descripcion IS NULL OR TRIM(descripcion) = ''
        GROUP BY id_doc
        """,
    )
    for r in empty_desc:
        issues.append(
            Issue(
                issue_type="detalle_descripcion_vacia",
                severity="medium",
                id_doc=r["id_doc"],
                detail=f"lineas_sin_descripcion={r['c']}",
            )
        )

    # Coherencia basica de monto total vs suma detalle (regla laxa)
    outliers = fetchall_rows(
        con,
        """
        SELECT
            d.id_doc,
            COALESCE(d.monto_total, 0) AS monto_total,
            COALESCE(SUM(COALESCE(t.monto_item, 0)), 0) AS suma_detalle,
            COUNT(t.id_det) AS n
        FROM documentos d
        LEFT JOIN detalle t ON t.id_doc = d.id_doc
        GROUP BY d.id_doc
        HAVING COUNT(t.id_det) > 0
        """,
    )
    for r in outliers:
        monto_total = float(r["monto_total"] or 0.0)
        suma_det = float(r["suma_detalle"] or 0.0)
        n = int(r["n"] or 0)
        if n == 0:
            continue
        if monto_total <= 0 and suma_det > 0:
            issues.append(
                Issue(
                    issue_type="monto_total_cero_con_detalle",
                    severity="medium",
                    id_doc=r["id_doc"],
                    detail=f"monto_total={monto_total:.2f} suma_detalle={suma_det:.2f}",
                )
            )
            continue
        if monto_total > 0:
            ratio = suma_det / monto_total
            if ratio < 0.4 or ratio > 1.6:
                issues.append(
                    Issue(
                        issue_type="descuadre_total_vs_detalle",
                        severity="low",
                        id_doc=r["id_doc"],
                        detail=f"ratio={ratio:.2f} (total={monto_total:.2f}, suma_det={suma_det:.2f})",
                    )
                )

    return issues, plans


def collect_pdf_text_issues(con: sqlite3.Connection) -> list[Issue]:
    """
    Verifica si las descripciones del detalle parecen existir en el texto del PDF.
    Marca el documento como sospechoso si la mayoria de lineas no coincide.
    """
    try:
        import fitz  # type: ignore
    except Exception:
        return [
            Issue(
                issue_type="check_descripcion_vs_pdf_no_disponible",
                severity="low",
                detail="No se pudo importar fitz (PyMuPDF).",
            )
        ]

    rows = fetchall_rows(
        con,
        """
        SELECT
            d.id_doc,
            COALESCE(d.ruta_pdf, '') AS ruta_pdf,
            t.linea,
            COALESCE(t.descripcion, '') AS descripcion
        FROM documentos d
        JOIN detalle t ON t.id_doc = d.id_doc
        ORDER BY d.id_doc, t.linea
        """,
    )

    by_doc: dict[str, list[sqlite3.Row]] = {}
    for r in rows:
        by_doc.setdefault(r["id_doc"], []).append(r)

    issues: list[Issue] = []
    for id_doc, items in by_doc.items():
        if not items:
            continue
        ruta_pdf = (items[0]["ruta_pdf"] or "").strip()
        local_pdf = resolve_local_pdf_path(ruta_pdf)
        if local_pdf is None:
            continue

        try:
            pdf = fitz.open(str(local_pdf))
            pdf_text = "\n".join(page.get_text("text") for page in pdf)
            pdf.close()
        except Exception:
            continue

        normalized_pdf = normalize_match_text(pdf_text)
        evaluated = 0
        misses = 0
        samples: list[str] = []
        for r in items:
            desc = (r["descripcion"] or "").strip()
            if not desc:
                continue
            desc_norm = normalize_match_text(desc)
            if len(desc_norm) < 5:
                continue
            evaluated += 1
            exact = desc_norm in normalized_pdf
            overlap = token_overlap_ratio(desc, pdf_text)
            ok = exact or overlap >= 0.60
            if not ok:
                misses += 1
                if len(samples) < 3:
                    samples.append(f"linea {r['linea']}: '{desc}' (overlap={overlap:.2f})")

        if evaluated == 0:
            continue
        ratio = misses / evaluated
        if ratio >= 0.75 or misses >= 3:
            issues.append(
                Issue(
                    issue_type="detalle_no_coincide_con_pdf",
                    severity="high",
                    id_doc=id_doc,
                    ruta_pdf=str(local_pdf),
                    detail=f"lineas_miss={misses}/{evaluated}; ejemplos: {' | '.join(samples)}",
                )
            )
    return issues


def update_detail_id_prefix(con: sqlite3.Connection, id_doc_target: str, old_prefix: str, new_prefix: str) -> None:
    like = f"{old_prefix}:%"
    rows = fetchall_rows(
        con,
        "SELECT id_det FROM detalle WHERE id_doc = ? AND id_det LIKE ?",
        (id_doc_target, like),
    )
    for r in rows:
        old_id_det = r["id_det"]
        new_id_det = f"{new_prefix}:{old_id_det[len(old_prefix) + 1:]}"
        con.execute("UPDATE detalle SET id_det = ? WHERE id_det = ?", (new_id_det, old_id_det))


def apply_fix_plans(con: sqlite3.Connection, plans: Iterable[FixPlan]) -> list[str]:
    logs: list[str] = []
    for p in plans:
        if p.action == "conflict_manual_review":
            logs.append(f"SKIP {p.action} old={p.old_id} new={p.new_id} note={p.note}")
            continue

        if p.action == "delete_header_only":
            con.execute("DELETE FROM documentos WHERE id_doc = ?", (p.old_id,))
            logs.append(f"OK delete_header_only old={p.old_id} kept={p.new_id}")
            continue

        if p.action == "move_details_to_expected":
            con.execute("UPDATE detalle SET id_doc = ? WHERE id_doc = ?", (p.new_id, p.old_id))
            update_detail_id_prefix(con, p.new_id, p.old_id, p.new_id)
            con.execute("DELETE FROM documentos WHERE id_doc = ?", (p.old_id,))
            logs.append(f"OK move_details_to_expected old={p.old_id} -> {p.new_id}")
            continue

        if p.action == "rename_id_doc":
            # 1) mover detalle (si existe)
            con.execute("UPDATE detalle SET id_doc = ? WHERE id_doc = ?", (p.new_id, p.old_id))
            update_detail_id_prefix(con, p.new_id, p.old_id, p.new_id)
            # 2) renombrar cabecera
            con.execute("UPDATE documentos SET id_doc = ? WHERE id_doc = ?", (p.new_id, p.old_id))
            logs.append(f"OK rename_id_doc old={p.old_id} -> {p.new_id}")
            continue

        logs.append(f"SKIP unknown_action old={p.old_id} action={p.action}")
    return logs


def write_report(
    report_path: Path,
    db_path: Path,
    issues: list[Issue],
    plans: list[FixPlan],
    apply_mode: bool,
    backup_path: Path | None,
    apply_logs: list[str],
) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    counter = Counter(i.issue_type for i in issues)
    plan_counter = Counter(p.action for p in plans)

    with report_path.open("w", encoding="utf-8") as f:
        f.write("REPORTE DE CONSISTENCIA DE FACTURAS\n")
        f.write(f"Generado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"DB: {db_path}\n")
        f.write("=" * 100 + "\n\n")

        f.write("RESUMEN DE HALLAZGOS\n")
        for k, v in sorted(counter.items()):
            f.write(f"- {k}: {v}\n")
        if not counter:
            f.write("- sin_hallazgos: 0\n")

        f.write("\nPLAN DE CORRECCION ID\n")
        for k, v in sorted(plan_counter.items()):
            f.write(f"- {k}: {v}\n")
        if not plan_counter:
            f.write("- sin_acciones: 0\n")

        f.write("\nMODO DE EJECUCION\n")
        f.write(f"- apply: {apply_mode}\n")
        if backup_path:
            f.write(f"- backup: {backup_path}\n")

        if apply_logs:
            f.write("\nRESULTADO DE APLICACION\n")
            for line in apply_logs:
                f.write(f"- {line}\n")

        f.write("\nDETALLE DE HALLAZGOS\n")
        for i in issues:
            f.write(
                f"[{i.severity}] {i.issue_type} | id_doc={i.id_doc or '-'}"
                f" | expected={i.expected_id_doc or '-'} | ruta_pdf={i.ruta_pdf or '-'}\n"
            )
            if i.detail:
                f.write(f"  detalle: {i.detail}\n")

        f.write("\nDETALLE PLANES\n")
        for p in plans:
            f.write(f"- action={p.action} old={p.old_id} new={p.new_id or '-'}")
            if p.note:
                f.write(f" note={p.note}")
            f.write("\n")


def write_csv(csv_path: Path, issues: list[Issue]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["severity", "issue_type", "id_doc", "expected_id_doc", "ruta_pdf", "detail"])
        for i in issues:
            w.writerow([i.severity, i.issue_type, i.id_doc, i.expected_id_doc, i.ruta_pdf, i.detail])


def main() -> None:
    parser = argparse.ArgumentParser(description="Audita coherencia de facturas y repara IDs por filename.")
    parser.add_argument("--db", help="Ruta de la DB SQLite. Si se omite usa DB_PATH_DTE_RECIBIDOS o data/dte_recibidos.db.")
    parser.add_argument("--report", help="Ruta de reporte TXT. Por defecto data/reportes/reporte_coherencia_YYYYmmdd_HHMMSS.txt")
    parser.add_argument("--csv", help="Ruta CSV opcional para exportar hallazgos.")
    parser.add_argument("--fix-id-from-filename", action="store_true", help="Activa plan de correccion de IDs por nombre PDF.")
    parser.add_argument(
        "--check-description-vs-pdf",
        action="store_true",
        help="Verifica si las descripciones del detalle aparecen en el texto del PDF (requiere fitz).",
    )
    parser.add_argument("--apply", action="store_true", help="Aplica cambios en la DB (si no, solo dry-run).")
    args = parser.parse_args()

    db_path = resolve_db_path(args.db)
    if not db_path.is_file():
        raise SystemExit(f"DB no encontrada: {db_path}")

    report_path = (
        Path(args.report).expanduser().resolve()
        if args.report
        else (DEFAULT_REPORT_DIR / f"reporte_coherencia_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt").resolve()
    )

    con = sqlite3.connect(str(db_path))
    con.row_factory = sqlite3.Row

    backup_path: Path | None = None
    apply_logs: list[str] = []
    try:
        issues, plans = collect_issues_and_plans(con)
        if args.check_description_vs_pdf:
            issues.extend(collect_pdf_text_issues(con))

        if not args.fix_id_from_filename:
            plans = []

        if args.apply and plans:
            backup_path = make_backup(db_path)
            with con:
                apply_logs = apply_fix_plans(con, plans)
            # Reauditar despues de aplicar
            issues, _plans_after = collect_issues_and_plans(con)
            if args.fix_id_from_filename:
                plans = _plans_after

        write_report(report_path, db_path, issues, plans, bool(args.apply), backup_path, apply_logs)
        if args.csv:
            write_csv(Path(args.csv).expanduser().resolve(), issues)

        print(f"Reporte generado: {report_path}")
        print(f"Hallazgos: {len(issues)}")
        if args.fix_id_from_filename:
            print(f"Planes de correccion pendientes: {len(plans)}")
        if backup_path:
            print(f"Backup: {backup_path}")
        if apply_logs:
            ok = len([x for x in apply_logs if x.startswith('OK ')])
            skip = len([x for x in apply_logs if x.startswith('SKIP ')])
            print(f"Aplicados OK: {ok} | Skip: {skip}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
