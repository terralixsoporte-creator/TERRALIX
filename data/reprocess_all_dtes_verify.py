import argparse
import json
import os
import random
import sqlite3
import time
from pathlib import Path

from app.core.DTE_Recibidos import ai_reader as AIR
from app.core.DTE_Recibidos import dte_loader as DL


def _f_eq(a, b, tol=0.01):
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False


def _load_done(path: Path) -> set[str]:
    done = set()
    if not path.exists():
        return done
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            id_doc = str(obj.get("id_doc") or "").strip()
            if id_doc:
                done.add(id_doc)
    return done


def _append_jsonl(path: Path, obj: dict):
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def _doc_exists(conn: sqlite3.Connection, id_doc: str) -> bool:
    row = conn.execute("SELECT 1 FROM documentos WHERE id_doc=? LIMIT 1", (id_doc,)).fetchone()
    return row is not None


def _get_doc_row(conn: sqlite3.Connection, id_doc: str):
    return conn.execute(
        "SELECT id_doc, fecha_emision, IVA, impuesto_adicional, monto_total FROM documentos WHERE id_doc=?",
        (id_doc,),
    ).fetchone()


def _get_det_rows(conn: sqlite3.Connection, id_doc: str):
    rows = conn.execute(
        "SELECT linea, Descuento, impuesto_adicional, fecha_emision FROM detalle WHERE id_doc=? ORDER BY linea",
        (id_doc,),
    ).fetchall()
    return {int(r["linea"]): r for r in rows}


def _call_doc_ai(img_path: str, *, rut: str, tipo: str, folio: str, max_retries: int = 3) -> dict:
    last = {"ok": False, "error": "unknown"}
    for attempt in range(1, max_retries + 1):
        out = AIR.analizar_documento_desde_imagen(
            img_path,
            rut_emisor_target=rut,
            tipo_doc_target=tipo,
            folio_target=folio,
        )
        if out.get("ok"):
            return out
        last = out
        if attempt < max_retries:
            time.sleep(2 * attempt)
    return last


def _call_det_ai(img_path: str, max_retries: int = 3) -> dict:
    last = {"ok": False, "error": "unknown"}
    for attempt in range(1, max_retries + 1):
        out = AIR.analizar_detalle_desde_imagen(img_path)
        if out.get("ok"):
            return out
        last = out
        if attempt < max_retries:
            time.sleep(2 * attempt)
    return last


def _compute_expected_from_ai(datos_raw: dict, before_doc_row):
    monto_neto = DL.extraer_monto(datos_raw.get("Monto Neto", "0"))
    exp_iva = DL.parse_iva(datos_raw.get("IVA", "0"), monto_neto=monto_neto)
    exp_imp = DL.extraer_monto(datos_raw.get("Impuesto Adicional", "0"), default=0.0)
    exp_total = DL.extraer_monto(datos_raw.get("Total", "0")) or monto_neto

    fecha_raw = datos_raw.get("Fecha Emision", "")
    parsed = DL.parse_fecha(fecha_raw)
    if parsed is not None and not DL._fecha_en_rango_valido(parsed):
        parsed = None

    if before_doc_row is not None:
        before_fecha = DL._normalizar_fecha_db(before_doc_row["fecha_emision"])
        exp_fecha = parsed if parsed is not None else before_fecha
    else:
        exp_fecha = parsed

    return {
        "fecha_emision": exp_fecha,
        "IVA": float(exp_iva or 0.0),
        "impuesto_adicional": float(exp_imp or 0.0),
        "monto_total": float(exp_total or 0.0),
    }


def _build_datos_raw(tipo: str, rut: str, folio: str, doc_ai: dict, det_ai: dict):
    emisor = doc_ai.get("emisor", {}) or {}
    doc = doc_ai.get("doc", {}) or {}
    refs = doc_ai.get("referencias", []) or []
    items = det_ai.get("items", []) or []

    return {
        "Tipo Documento": tipo,
        "Emisor": rut,
        "Numero de Folio": folio,
        "Razon Social": (emisor.get("razon_social") or "").strip(),
        "Giro": (emisor.get("giro") or "").strip(),
        "Fecha Emision": (doc.get("fecha_emision") or "").strip(),
        "Monto Neto": doc.get("monto_neto", "0"),
        "IVA": doc.get("IVA", "0"),
        "Monto Exento": doc.get("monto_exento", "0"),
        "Impuesto Adicional": doc.get("impuesto_adicional", "0"),
        "Total": doc.get("monto_total", "0"),
        "Referencia": AIR._build_referencia_text(refs),
        "DTE_referencia": AIR._build_dte_referencia(refs),
        "__only_numeric_fix__": True,
        "__items_detalle__": items,
    }


def _process_one(pdf: str, id_doc: str, tipo: str, rut: str, folio: str, db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    before_doc = _get_doc_row(conn, id_doc)
    conn.close()

    img_path = AIR._rasterize_first_page(pdf)
    if not img_path:
        return {"id_doc": id_doc, "pdf": pdf, "ok": False, "error": "raster_failed"}

    try:
        doc_ai = _call_doc_ai(img_path, rut=rut, tipo=tipo, folio=folio, max_retries=3)
        if not doc_ai.get("ok"):
            return {
                "id_doc": id_doc,
                "pdf": pdf,
                "ok": False,
                "error": f"doc_ai_error:{doc_ai.get('error')}",
            }

        det_ai = _call_det_ai(img_path, max_retries=3)
        if not det_ai.get("ok"):
            return {
                "id_doc": id_doc,
                "pdf": pdf,
                "ok": False,
                "error": f"det_ai_error:{det_ai.get('error')}",
            }

        datos_raw = _build_datos_raw(tipo, rut, folio, doc_ai, det_ai)
        expected = _compute_expected_from_ai(datos_raw, before_doc)
        DL.guardar_en_bd(datos_raw, pdf)

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        after_doc = _get_doc_row(conn, id_doc)
        det_map = _get_det_rows(conn, id_doc)
        conn.close()

        if after_doc is None:
            return {"id_doc": id_doc, "pdf": pdf, "ok": False, "error": "doc_not_found_after_save"}

        got_fecha = DL._normalizar_fecha_db(after_doc["fecha_emision"])
        got_iva = float(after_doc["IVA"] or 0.0)
        got_imp = float(after_doc["impuesto_adicional"] or 0.0)
        got_total = float(after_doc["monto_total"] or 0.0)

        checks = {
            "fecha_emision": (got_fecha == expected["fecha_emision"]),
            "IVA": _f_eq(got_iva, expected["IVA"]),
            "impuesto_adicional": _f_eq(got_imp, expected["impuesto_adicional"]),
            "monto_total": _f_eq(got_total, expected["monto_total"]),
        }

        detail_checked = 0
        detail_fail = 0
        items = datos_raw.get("__items_detalle__", []) or []
        for idx, it in enumerate(items, start=1):
            row = det_map.get(idx)
            if row is None:
                continue
            exp_desc = DL._to_monto_float(it.get("descuento", 0))
            exp_imp_det = DL._to_monto_float(it.get("impuesto_adicional", 0))
            got_desc = float(row["Descuento"] or 0.0)
            got_imp_det = float(row["impuesto_adicional"] or 0.0)
            detail_checked += 1
            if not (_f_eq(got_desc, exp_desc) and _f_eq(got_imp_det, exp_imp_det)):
                detail_fail += 1

            row_f = DL._normalizar_fecha_db(row["fecha_emision"])
            if row_f is not None and got_fecha is not None and row_f != got_fecha:
                detail_fail += 1

        ok = all(checks.values()) and detail_fail == 0
        return {
            "id_doc": id_doc,
            "pdf": pdf,
            "ok": ok,
            "error": None,
            "checks": checks,
            "detail_checked": detail_checked,
            "detail_fail": detail_fail,
        }
    finally:
        try:
            if img_path and os.path.exists(img_path):
                os.remove(img_path)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Reprocesa todos los DTE y valida carga en DB.")
    parser.add_argument("--limit", type=int, default=0, help="Limite de documentos a procesar (0=todos).")
    parser.add_argument("--seed", type=int, default=20260327, help="Semilla para aleatorizar orden.")
    parser.add_argument("--random-order", action="store_true", help="Procesar en orden aleatorio.")
    parser.add_argument("--only-existing", action="store_true", help="Procesar solo IDs que ya existen en DB.")
    args = parser.parse_args()

    db_path = AIR._get_db_path()
    print(f"[RUN] DB: {db_path}")

    out_jsonl = Path("data") / "qa_all_dtes_result.jsonl"
    out_summary = Path("data") / "qa_all_dtes_summary.json"
    out_jsonl.parent.mkdir(parents=True, exist_ok=True)

    done = _load_done(out_jsonl)
    if done:
        print(f"[RUN] Registros previos detectados (resume): {len(done)}")

    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    ids_db = {r[0] for r in con.execute("SELECT id_doc FROM documentos").fetchall()}
    con.close()

    pdfs = AIR._collect_target_files(None, os.getenv("RUTA_PDF_DTE_RECIBIDOS"))
    candidates = []
    for pdf in pdfs:
        tipo, rut, folio = AIR._infer_from_filename(pdf)
        id_doc = DL.build_id_doc(tipo, rut, folio)
        if args.only_existing and id_doc not in ids_db:
            continue
        if id_doc in done:
            continue
        candidates.append((pdf, id_doc, tipo, rut, folio))

    if args.random_order:
        rnd = random.Random(args.seed)
        rnd.shuffle(candidates)
    else:
        candidates.sort(key=lambda x: x[1])

    if args.limit and args.limit > 0:
        candidates = candidates[: args.limit]

    total = len(candidates)
    print(f"[RUN] Pendientes a procesar: {total}")
    if total == 0:
        print("[RUN] Nada pendiente.")
        return

    ok_count = 0
    fail_count = 0
    start_ts = time.time()
    for idx, (pdf, id_doc, tipo, rut, folio) in enumerate(candidates, start=1):
        t0 = time.time()
        rec = _process_one(pdf, id_doc, tipo, rut, folio, db_path)
        _append_jsonl(out_jsonl, rec)
        if rec.get("ok"):
            ok_count += 1
        else:
            fail_count += 1

        dt = time.time() - t0
        if idx % 10 == 0 or idx == 1 or idx == total:
            elapsed = time.time() - start_ts
            print(
                f"[RUN] {idx}/{total} | ok={ok_count} fail={fail_count} | "
                f"last={id_doc} ({dt:.1f}s) | elapsed={elapsed/60:.1f}m"
            )

    summary = {
        "db_path": db_path,
        "processed_now": total,
        "ok_now": ok_count,
        "fail_now": fail_count,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "result_file": str(out_jsonl),
    }
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print("[RUN] Resumen:", json.dumps(summary, ensure_ascii=False))
    print(f"[RUN] Reportes: {out_jsonl} | {out_summary}")


if __name__ == "__main__":
    main()

