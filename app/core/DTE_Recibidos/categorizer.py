# Script Control:
# - Role: Accounting categorization stage for detail rows.
# - Track file: docs/SCRIPT_CONTROL.md

import os
import sys
import sqlite3
import json
import time
import random
import re
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
from openai import OpenAI

try:
    from local_classifier import get_classifier  # ejecución directa
except ImportError:
    from app.core.DTE_Recibidos.local_classifier import get_classifier


# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = os.getenv("ENV_PATH")
if ENV_PATH and os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)
else:
    possible = Path(__file__).resolve().parents[3] / "data" / "config.env"
    if possible.exists():
        load_dotenv(str(possible))

DB_PATH = (os.getenv("DB_PATH_DTE_RECIBIDOS") or "").strip().strip('"').strip("'")
if not DB_PATH:
    DB_PATH = str(Path(__file__).resolve().parents[3] / "data" / "DteRecibidos_db.db")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("AI_OPENAI_MODEL", "gpt-4.1-mini").strip()
OPENAI_ASSISTANT_ID = os.getenv("OPENAI_ASSISTANT_ID", "").strip()

BATCH_SLEEP = float(os.getenv("REVIEW_BATCH_SLEEP", "0.25"))
MAX_RETRIES_OPENAI = int(os.getenv("REVIEW_MAX_RETRIES_OPENAI", "8"))
TEMPERATURE = float(os.getenv("REVIEW_TEMPERATURE", "0"))
LIMIT_ROWS = int(os.getenv("REVIEW_LIMIT_ROWS", "0"))
ASSISTANT_TRIGGER_CONFIDENCE = int(os.getenv("ASSISTANT_TRIGGER_CONFIDENCE", "75"))
ASSISTANT_MAX_WAIT_SECONDS = int(os.getenv("ASSISTANT_MAX_WAIT_SECONDS", "45"))
ASSISTANT_POLL_SECONDS = float(os.getenv("ASSISTANT_POLL_SECONDS", "1.0"))

# =========================
# REGLAS
# =========================
AUTOPISTA_PROVIDER_PATTERNS = [
    "RUTA DEL MAIPO",
    "SOCIEDAD CONCESIONARIA",
    "AUTOPISTA",
    "COSTANERA NORTE",
    "VESPUCIO NORTE",
    "VESPUCIO SUR",
    "TUNEL SAN CRISTOBAL",
    "TAG",
]
AUTOPISTA_DESC_KEYWORDS = [
    "TAG",
    "PEAJE",
    "INTERESES AFECTOS",
    "INTERESES EXENTOS",
    "GASTOS COBRANZA",
    "TUNEL",
    "AUTOPISTA",
]
RIEGO_KEYWORDS = ["PVC", "CAMLOCK", "LAYFLAT", "VALVULA", "TEE", "CODO", "UNION", "ACOPLE"]
AGROCON_SERVICIOS_KEYWORDS = [
    "HERBICIDA",
    "APLICACION",
    "TRITURADO",
    "TRASLADO MAQUINARIA",
    "SECADO",
    "NIVELACION",
    "CANALETA",
]
AGROCON_VEHICULO_KEYWORDS = ["COMBUSTIBLE", "DIESEL", "ACEITE", "FILTRO", "BATERIA"]
COPEVAL_INSUMOS_KEYWORDS = [
    "GUANTE",
    "BOTA",
    "MASCARILLA",
    "OVEROL",
    "RESPIRADOR",
    "LENTE",
    "PODA",
    "TIJERA",
    "SERRUCHO",
    "AMARRA",
]
COPEVAL_AGROQ_KEYWORDS = [
    "HERBICIDA",
    "FUNGICIDA",
    "COADYUV",
    "CITOGROWER",
    "RIPPER",
    "STREPTO",
    "BORICO",
    "ZINC",
]
COPEVAL_FIN_KEYWORDS = ["DIFERENCIA DE CAMBIO", "NOTA DEBITO", "INTERESES", "GTOS NOTARIO", "COBRANZA"]
SERVICAMPO_ARRIENDO_KEYWORDS = ["ARRIENDO BANO", "BANO QUIMICO", "COBRO MENSUAL EN UF", "MONTAJE HABILITACION"]
FINANCIAL_BANK_KEYWORDS = ["COMISION", "SWIFT", "COMPRA DE DIVISAS", "DIVISA"]
MACHINERY_RENTAL_KEYWORDS = [
    "RETROEXCAVADORA",
    "EXCAVADORA",
    "MINIEXCAVADORA",
    "CARGADOR FRONTAL",
    "MINICARGADOR",
    "MOTONIVELADORA",
    "BULLDOZER",
    "GRUA",
    "RODILLO",
    "COMPACTADOR",
    "TRACTOR",
    "MAQUINARIA",
    "MOV TIERRA",
]
AUTO_PARTS_KEYWORDS = [
    "RADIADOR",
    "RAIDADOR",
    "TAPA RAD",
    "TERMOSTATO",
    "BOMBA DE AGUA",
    "BOMBA AGUA",
    "ALTERNADOR",
    "EMBRAGUE",
    "AMORTIGUADOR",
    "PASTILLA FRENO",
    "DISCO FRENO",
    "HOMOCINETICA",
    "SUSPENSION",
    "CORREA",
]
STRUCTURE_INFRA_KEYWORDS = [
    "ESTRUCTURA",
    "CUBIERTA",
    "CUBIERTAS",
    "MALLA SOMBRA",
    "INSTALACION",
    "ACOPIO",
    "POSTE",
    "CONCRETO",
    "RED BT",
    "TRIFASICA",
    "MEDIDOR",
    "POTENCIA ELECTRICA",
]
RIEGO_PARTS_KEYWORDS = [
    "SOLENOIDE",
    "BOBINA",
    "COPA PP",
    "COPA",
    "VALVULA",
    "FITTING",
    "UNION PE",
    "CODO PE",
]
AGROQ_GENERIC_KEYWORDS = [
    "ACIDO BORICO",
    "BORICO",
    "RIPPER",
    "STREPTO",
    "CITOGROWER",
    "COADYUV",
    "FUNGICIDA",
    "HERBICIDA",
]


# =========================
# OPENAI
# =========================
def get_openai_client() -> Optional[OpenAI]:
    if not OPENAI_API_KEY:
        print("[WARN] OPENAI_API_KEY no configurada; se usaran solo reglas/mantenedor/fallback")
        return None
    try:
        return OpenAI(api_key=OPENAI_API_KEY)
    except Exception as e:
        print(f"[WARN] No se pudo inicializar OpenAI: {e}")
        return None


def openai_chat_json_with_retry(
    client: OpenAI,
    *,
    model: str,
    messages: list,
    temperature: float = 0,
    max_retries: int = 8,
    base_sleep: float = 1.5,
    max_sleep: float = 20.0,
) -> Dict[str, Any]:
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                response_format={"type": "json_object"},
                temperature=temperature,
            )
            return {"ok": True, "content": resp.choices[0].message.content}
        except Exception as e:
            last_err = str(e)
            msg = last_err.lower()
            is_rate = ("rate limit" in msg) or ("rate_limit" in msg) or ("429" in msg)
            is_quota = ("insufficient_quota" in msg) or ("exceeded your current quota" in msg)
            is_transient = is_rate or ("timeout" in msg) or ("temporarily" in msg) or ("503" in msg) or ("502" in msg) or ("504" in msg)
            if is_quota:
                return {"ok": False, "error": "quota_exceeded"}
            if (not is_transient) or attempt == max_retries:
                return {"ok": False, "error": last_err}
            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s *= (0.75 + random.random() * 0.5)
            print(f"[WARN] OpenAI transient/rate_limit ({attempt}/{max_retries}) -> {sleep_s:.1f}s")
            time.sleep(sleep_s)
    return {"ok": False, "error": last_err or "unknown_error"}


def parse_json_from_text(content: Any) -> Dict[str, Any]:
    text = (str(content or "")).strip()
    if not text:
        raise ValueError("empty_response")

    candidates: List[str] = [text]
    fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, flags=re.IGNORECASE | re.DOTALL)
    candidates.extend(fenced)

    if "{" in text and "}" in text:
        start = text.find("{")
        end = text.rfind("}") + 1
        if 0 <= start < end:
            candidates.append(text[start:end])

    for chunk in candidates:
        try:
            data = json.loads(chunk)
            if isinstance(data, dict):
                return data
        except Exception:
            continue
    raise ValueError("json_parse_error")


def _assistant_obj_get(obj: Any, key: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def extract_assistant_text(messages_payload: Any) -> str:
    data = _assistant_obj_get(messages_payload, "data")
    if data is None and isinstance(messages_payload, list):
        data = messages_payload
    if not data:
        return ""

    for msg in data:
        role = _assistant_obj_get(msg, "role", "")
        if role != "assistant":
            continue

        parts = _assistant_obj_get(msg, "content", []) or []
        chunks: List[str] = []
        for part in parts:
            txt_obj = _assistant_obj_get(part, "text")
            if txt_obj is not None:
                value = _assistant_obj_get(txt_obj, "value", "")
                if value:
                    chunks.append(str(value))
                continue

            if isinstance(part, dict):
                if part.get("type") in {"text", "output_text"}:
                    txt = part.get("text")
                    if isinstance(txt, dict):
                        value = txt.get("value") or txt.get("content") or ""
                    else:
                        value = txt if isinstance(txt, str) else (part.get("value") or "")
                    if value:
                        chunks.append(str(value))

        if chunks:
            return "\n".join(chunks).strip()
    return ""


def _create_assistant_run(client: OpenAI, *, thread_id: str, assistant_id: str, model: str) -> Any:
    runs_api = client.beta.threads.runs
    run_kwargs: Dict[str, Any] = {"thread_id": thread_id, "assistant_id": assistant_id}
    if model:
        run_kwargs["model"] = model

    if hasattr(runs_api, "create_and_poll"):
        try:
            return runs_api.create_and_poll(**run_kwargs, poll_interval_ms=max(250, int(ASSISTANT_POLL_SECONDS * 1000)))
        except TypeError:
            pass
        except Exception as e:
            if "model" not in str(e).lower():
                raise
        try:
            return runs_api.create_and_poll(**run_kwargs)
        except Exception as e:
            if "model" in run_kwargs and "model" in str(e).lower():
                run_kwargs.pop("model", None)
                return runs_api.create_and_poll(**run_kwargs)
            raise

    try:
        run = runs_api.create(**run_kwargs)
    except Exception as e:
        if "model" in run_kwargs and "model" in str(e).lower():
            run_kwargs.pop("model", None)
            run = runs_api.create(**run_kwargs)
        else:
            raise

    run_id = _assistant_obj_get(run, "id", "")
    if not run_id:
        return run

    deadline = time.time() + max(5, int(ASSISTANT_MAX_WAIT_SECONDS))
    done_status = {"completed", "failed", "cancelled", "expired", "requires_action"}
    while True:
        status = str(_assistant_obj_get(run, "status", "") or "").lower()
        if status in done_status:
            return run
        if time.time() >= deadline:
            return {"status": "timeout", "id": run_id}
        time.sleep(max(0.2, ASSISTANT_POLL_SECONDS))
        run = runs_api.retrieve(thread_id=thread_id, run_id=run_id)


def openai_assistant_json_with_retry(
    client: OpenAI,
    *,
    assistant_id: str,
    model: str,
    user_prompt: str,
    max_retries: int = 4,
    base_sleep: float = 1.2,
    max_sleep: float = 15.0,
) -> Dict[str, Any]:
    if not assistant_id:
        return {"ok": False, "error": "assistant_id_missing"}

    last_err = ""
    for attempt in range(1, max_retries + 1):
        try:
            thread = client.beta.threads.create()
            thread_id = _assistant_obj_get(thread, "id", "")
            if not thread_id:
                return {"ok": False, "error": "assistant_thread_create_failed"}

            client.beta.threads.messages.create(
                thread_id=thread_id,
                role="user",
                content=user_prompt,
            )

            run = _create_assistant_run(
                client,
                thread_id=thread_id,
                assistant_id=assistant_id,
                model=model,
            )

            run_status = str(_assistant_obj_get(run, "status", "") or "").lower()
            if run_status == "timeout":
                return {"ok": False, "error": "assistant_timeout"}
            if run_status != "completed":
                last_error = _assistant_obj_get(run, "last_error")
                err_code = _assistant_obj_get(last_error, "code", "")
                err_msg = _assistant_obj_get(last_error, "message", "")
                status_info = f"{run_status}:{err_code}:{err_msg}".strip(":")
                return {"ok": False, "error": f"assistant_run_{status_info or 'failed'}"}

            try:
                messages = client.beta.threads.messages.list(thread_id=thread_id, order="desc", limit=15)
            except TypeError:
                messages = client.beta.threads.messages.list(thread_id=thread_id, limit=15)

            raw_text = extract_assistant_text(messages)
            if not raw_text:
                return {"ok": False, "error": "assistant_empty_response"}

            parsed = parse_json_from_text(raw_text)
            return {"ok": True, "data": parsed, "raw": raw_text}
        except Exception as e:
            last_err = str(e)
            msg = last_err.lower()
            is_rate = ("rate limit" in msg) or ("rate_limit" in msg) or ("429" in msg)
            is_quota = ("insufficient_quota" in msg) or ("exceeded your current quota" in msg)
            is_transient = is_rate or ("timeout" in msg) or ("temporarily" in msg) or ("503" in msg) or ("502" in msg) or ("504" in msg)
            if is_quota:
                return {"ok": False, "error": "quota_exceeded"}
            if (not is_transient) or attempt == max_retries:
                return {"ok": False, "error": last_err or "assistant_unknown_error"}
            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s *= (0.75 + random.random() * 0.5)
            print(f"[WARN] Assistant transient/rate_limit ({attempt}/{max_retries}) -> {sleep_s:.1f}s")
            time.sleep(sleep_s)

    return {"ok": False, "error": last_err or "assistant_unknown_error"}


def classify_with_assistant(
    descripcion: str,
    proveedor: str,
    giro: str,
    fecha_emision: Optional[str],
    *,
    client: Optional[OpenAI],
    model: str,
    categorias_validas: Optional[List[str]] = None,
    assistant_id: str = "",
) -> Dict[str, Any]:
    base_out = {"ok": False, "categoria": "", "subcategoria": "", "confianza": 0, "explicacion": ""}
    if client is None:
        return {**base_out, "error": "openai_client_missing"}

    assistant_ref = (assistant_id or OPENAI_ASSISTANT_ID or "").strip()
    if not assistant_ref:
        return {**base_out, "error": "assistant_id_missing"}

    temporal_hint = build_temporal_hint(fecha_emision)
    categorias_json = json.dumps(categorias_validas or [], ensure_ascii=False)
    user_prompt = (
        "Clasifica una linea contable de un ERP agricola chileno.\n"
        "Devuelve SOLO JSON con este formato exacto:\n"
        "{\n"
        "  \"categoria\": \"\",\n"
        "  \"subcategoria\": \"\",\n"
        "  \"confianza\": 0,\n"
        "  \"explicacion\": \"\"\n"
        "}\n"
        "Reglas:\n"
        "- categoria alineada al catalogo entregado.\n"
        "- si subcategoria no es clara, usa \"OTRO\".\n"
        "- confianza en rango 0..100.\n\n"
        "CATEGORIAS_VALIDAS:\n"
        f"{categorias_json}\n\n"
        "CONTEXTO:\n"
        f"- proveedor: {proveedor}\n"
        f"- giro: {giro}\n"
        f"- fecha_emision: {fecha_emision or '(sin dato)'}\n"
        f"- temporalidad: {temporal_hint}\n"
        f"- descripcion: {descripcion}\n"
    )

    call = openai_assistant_json_with_retry(
        client,
        assistant_id=assistant_ref,
        model=model,
        user_prompt=user_prompt,
        max_retries=max(2, min(6, MAX_RETRIES_OPENAI)),
    )
    if not call.get("ok"):
        return {**base_out, "error": call.get("error", "assistant_error")}

    data = call.get("data") or {}
    categoria = (data.get("categoria") or "").strip()
    subcategoria = (data.get("subcategoria") or "").strip()
    confianza = clamp_conf(data.get("confianza", 0))
    explicacion = (data.get("explicacion") or "").strip()

    if not categoria:
        return {**base_out, "error": "assistant_categoria_vacia", "explicacion": explicacion}

    if categorias_validas:
        canon_map = {normalize_text(c): c for c in categorias_validas if c and c.strip()}
        categoria_canon = canon_map.get(normalize_text(categoria))
        if not categoria_canon:
            return {
                **base_out,
                "error": "assistant_categoria_no_valida",
                "explicacion": explicacion,
                "categoria": categoria,
                "subcategoria": subcategoria,
                "confianza": confianza,
            }
        categoria = categoria_canon

    return {
        "ok": True,
        "categoria": categoria,
        "subcategoria": subcategoria,
        "confianza": confianza,
        "explicacion": explicacion,
    }


# =========================
# SQLITE HELPERS
# =========================
def connect_db(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path, timeout=60)
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")
    cur.execute("PRAGMA busy_timeout=60000;")
    con.commit()
    return con


def table_columns(con: sqlite3.Connection, table: str) -> List[str]:
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    return [r[1] for r in cur.fetchall()]


def add_column_if_missing_sqlite(con: sqlite3.Connection, table: str, col_def: str) -> None:
    cols = set(table_columns(con, table))
    colname = col_def.split()[0]
    if colname in cols:
        return
    cur = con.cursor()
    cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")
    con.commit()
    print(f"[INFO] Columna agregada: {table}.{colname}")


def strip_accents(value: str) -> str:
    txt = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(ch for ch in txt if not unicodedata.combining(ch))


def normalize_text(value: str) -> str:
    txt = strip_accents(value).upper()
    txt = txt.replace("\u00ba", "O").replace("\u00aa", "A").replace("&", " Y ").replace("$", "S")
    txt = re.sub(r"(?<=[A-Z0-9])[\?\.,;:_\-\|/\\]+(?=[A-Z0-9])", "", txt)
    txt = re.sub(r"[^A-Z0-9]+", " ", txt)
    return " ".join(txt.split())


def normalize_key_text(value: str) -> str:
    return normalize_text(value)


def contains_any_keyword(text: str, keywords: List[str]) -> bool:
    text_norm = normalize_text(text)
    if not text_norm:
        return False
    padded = f" {text_norm} "
    for kw in keywords:
        k = normalize_text(kw)
        if k and f" {k} " in padded:
            return True
    return False


def first_matching_keyword(text: str, keywords: List[str]) -> str:
    text_norm = normalize_text(text)
    padded = f" {text_norm} "
    for kw in keywords:
        k = normalize_text(kw)
        if k and f" {k} " in padded:
            return k
    return ""


def provider_matches(provider_norm: str, patterns: List[str]) -> bool:
    if not provider_norm:
        return False
    padded = f" {provider_norm} "
    for p in patterns:
        k = normalize_text(p)
        if k and f" {k} " in padded:
            return True
    return False


def clamp_conf(val: Any) -> int:
    try:
        out = int(float(val))
    except Exception:
        out = 0
    return max(0, min(100, out))


def build_rule_result(
    categoria: str,
    subcategoria: str = "",
    tipo_gasto: str = "OTRO",
    *,
    origen: str,
    motivo: str,
    confianza_categoria: int = 90,
    confianza_subcategoria: Optional[int] = None,
    needs_review: int = 0,
) -> Dict[str, Any]:
    conf_cat = clamp_conf(confianza_categoria)
    conf_sub = clamp_conf(confianza_subcategoria if confianza_subcategoria is not None else conf_cat - 3)
    return {
        "categoria": (categoria or "").strip(),
        "subcategoria": (subcategoria or "").strip(),
        "tipo_gasto": (tipo_gasto or "OTRO").strip() or "OTRO",
        "needs_review": int(needs_review or 0),
        "confianza_categoria": conf_cat,
        "confianza_subcategoria": conf_sub,
        "origen": (origen or "REGLA_DESC").strip() or "REGLA_DESC",
        "motivo": (motivo or "").strip(),
    }


def parse_month(fecha_emision: Optional[str]) -> Optional[int]:
    raw = (fecha_emision or "").strip()
    if not raw:
        return None
    fmts = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"]
    for fmt in fmts:
        try:
            return datetime.strptime(raw, fmt).month
        except Exception:
            continue
    m = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", raw)
    if m:
        mm = int(m.group(2))
        return mm if 1 <= mm <= 12 else None
    return None


def build_temporal_hint(fecha_emision: Optional[str]) -> str:
    mm = parse_month(fecha_emision)
    if mm is None:
        return "temporalidad no disponible"
    if mm == 11:
        etapa = "primavera y entrada de cosecha"
    elif mm == 12:
        etapa = "cosecha y postcosecha"
    elif mm in (1, 2):
        etapa = "postcosecha"
    elif mm in (6, 7, 8):
        etapa = "invierno"
    else:
        etapa = "temporada intermedia"
    return (
        f"mes={mm}, etapa={etapa}. Secundario: primavera(poda/aplicaciones/fertilizacion/riego), "
        "cosecha(nov-dic), postcosecha(dic-feb), invierno(poda/reparaciones/infraestructura)."
    )


def classify_by_rules(
    razon_social: str,
    giro: str,
    descripcion: str,
    fecha_emision: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    rs_norm = normalize_text(razon_social)
    giro_norm = normalize_text(giro)
    desc_norm = normalize_text(descripcion)
    provider_ctx = " ".join(x for x in [rs_norm, giro_norm] if x).strip()

    # 1) Reglas duras por proveedor
    if provider_matches(provider_ctx, [
        "FERRETERIA COVADONGA LIMITADA",
        "FERRETERIA Y CONSTRUCTORA NUNEZ SPA",
        "COMERCIAL ALLENDE CAVIERES SPA",
        "PRODALAM SA",
        "AGROSYSTEMS SA",
        "GARCIA RIO SPA",
    ]):
        return build_rule_result(
            "TRABAJOS_CAMPO", "INSUMOS", "OTRO",
            origen="REGLA_RS", motivo="Proveedor ferreteria/insumos", confianza_categoria=95, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, ["SOCIEDAD SERVICAMPO SPA", "SERVICAMPO SPA", "SERVICAMPO"]):
        return build_rule_result(
            "TRABAJOS_CAMPO", "ARRIENDO", "OTRO",
            origen="REGLA_RS", motivo="Proveedor Servicampo", confianza_categoria=95, confianza_subcategoria=90
        )

    if provider_matches(provider_ctx, ["HYDRORENGO SPA", "HIDRORENGO SPA", "HYDR0RENGO SPA"]):
        return build_rule_result(
            "MANTENCION", "RIEGO", "REPUESTOS",
            origen="REGLA_RS", motivo="Proveedor Hydrorengo (incluye OCR)", confianza_categoria=96, confianza_subcategoria=93
        )

    if provider_matches(provider_ctx, ["SERVI RIEGO SA", "OLIVOS SPA", "OLIVOS S P A"]):
        return build_rule_result(
            "MANTENCION", "RIEGO", "REPUESTOS",
            origen="REGLA_RS", motivo="Proveedor de riego", confianza_categoria=95, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, [
        "COMERCIAL NEUMAMUNDO LIMITADA",
        "JUAN PABLO BLANCO SPA",
        "LUBRIPEREZ SPA",
        "AUTO REPUESTOS LOS HEROES LIMITADA",
        "CASTRO PREISLER SPA",
    ]):
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "MANTENCION",
            origen="REGLA_RS", motivo="Proveedor de vehiculos/repuestos", confianza_categoria=95, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, ["COMERCIALIZADORA AUTOMOTRIZ MACROF SPA", "AUTOMOTRIZ MACROF SPA", "MACROF SPA"]):
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "MANTENCION",
            origen="REGLA_RS", motivo="Proveedor automotriz (repuestos vehiculares)", confianza_categoria=96, confianza_subcategoria=93
        )

    if provider_matches(provider_ctx, ["COMERCIAL TODOAGRO SPA", "TODOAGRO SPA", "TODOAGRO"]):
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "MANTENCION",
            origen="REGLA_RS", motivo="Proveedor repuestos maquinaria agricola", confianza_categoria=95, confianza_subcategoria=91
        )

    if provider_matches(provider_ctx, ["MINIMACK SPA", "MINIMACK"]) and contains_any_keyword(desc_norm, ["ARRIENDO", "ALQUILER", "RETROEXCAVADORA", "EXCAVADORA", "MAQUINARIA", "MOV TIERRA"]):
        return build_rule_result(
            "COSECHA", "ARRIENDO", "TRACTOR",
            origen="REGLA_RS_DESC", motivo="Minimack + arriendo de maquinaria", confianza_categoria=96, confianza_subcategoria=93
        )

    if provider_matches(provider_ctx, ["GRUPO G Y R SPA", "GRUPO G R SPA", "GRUPO G & R SPA"]):
        return build_rule_result(
            "MANTENCION", "INFRAESTRUCTURA", "OTRO",
            origen="REGLA_RS", motivo="Proveedor estructuras/cubiertas frutales", confianza_categoria=94, confianza_subcategoria=90
        )

    if provider_matches(provider_ctx, ["JIMENEZ Y PADILLA LIMITADA", "JIMENEZ PADILLA"]) or (contains_any_keyword(giro_norm, ["RIEGO", "INSUMOS RIEGO"]) and contains_any_keyword(desc_norm, RIEGO_PARTS_KEYWORDS)):
        return build_rule_result(
            "MANTENCION", "RIEGO", "REPUESTOS",
            origen="REGLA_RS_DESC", motivo="Proveedor/giro riego + repuesto riego", confianza_categoria=94, confianza_subcategoria=90
        )

    if provider_matches(provider_ctx, AUTOPISTA_PROVIDER_PATTERNS):
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "PEAJES",
            origen="REGLA_RS", motivo="Proveedor autopista/concesionaria/TAG", confianza_categoria=95, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, ["BANCO DE CREDITO E INVERSIONES", "BANCO CREDITO E INVERSIONES", "BCI"]):
        if not provider_matches(provider_ctx, ["SEGUROS"]):
            return build_rule_result(
                "GASTOS_FINANCIEROS", "BANCOS", "OTRO",
                origen="REGLA_RS", motivo="Proveedor bancario BCI", confianza_categoria=95, confianza_subcategoria=92
            )

    if provider_matches(provider_ctx, ["BCI SEGUROS GENERALES SA"]):
        return build_rule_result(
            "GASTOS_FINANCIEROS", "SEGUROS", "OTRO",
            origen="REGLA_RS", motivo="Proveedor BCI Seguros", confianza_categoria=96, confianza_subcategoria=94
        )

    if provider_matches(provider_ctx, ["BICE VIDA COMPANIA DE SEGUROS", "BICE VIDA COMPANIA SEGUROS", "BICE VIDA"]):
        return build_rule_result(
            "GASTOS_FINANCIEROS", "SEGUROS", "OTRO",
            origen="REGLA_RS", motivo="Proveedor BICE Vida (incluye OCR)", confianza_categoria=95, confianza_subcategoria=93
        )

    if provider_matches(provider_ctx, ["COGROWERS SPA", "COGROWERS"]):
        return build_rule_result(
            "ADMINISTRACION", "TI", "SOFTWARE",
            origen="REGLA_RS", motivo="Proveedor plataforma TI", confianza_categoria=95, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, ["EMPRESA NACIONAL DE CERTIFICACION ELECTRONICA"]):
        return build_rule_result(
            "ADMINISTRACION", "SERVICIOS", "OTRO",
            origen="REGLA_RS", motivo="Proveedor certificacion electronica", confianza_categoria=95, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, ["ATKINSON COSTABAL UNDURRAGA ACHURRA LIMITADA", "ATKINSON COSTABAL UNDURRAGA", "ACHURRA LIMITADA"]):
        return build_rule_result(
            "ADMINISTRACION", "SERVICIOS", "OTRO",
            origen="REGLA_RS", motivo="Proveedor servicios administrativos", confianza_categoria=93, confianza_subcategoria=90
        )

    # 2) Reglas por descripcion
    if contains_any_keyword(desc_norm, AUTOPISTA_DESC_KEYWORDS):
        kw = first_matching_keyword(desc_norm, AUTOPISTA_DESC_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "PEAJES",
            origen="REGLA_DESC", motivo=f"Keyword autopista/tag: {kw}", confianza_categoria=91, confianza_subcategoria=88
        )

    if contains_any_keyword(desc_norm, RIEGO_KEYWORDS):
        kw = first_matching_keyword(desc_norm, RIEGO_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "RIEGO", "REPUESTOS",
            origen="REGLA_DESC", motivo=f"Keyword repuesto riego: {kw}", confianza_categoria=90, confianza_subcategoria=86
        )

    if contains_any_keyword(desc_norm, ["ARRIENDO", "ALQUILER"]) and contains_any_keyword(desc_norm, MACHINERY_RENTAL_KEYWORDS):
        kw = first_matching_keyword(desc_norm, MACHINERY_RENTAL_KEYWORDS)
        return build_rule_result(
            "COSECHA", "ARRIENDO", "TRACTOR",
            origen="REGLA_DESC", motivo=f"Arriendo de maquinaria ({kw})", confianza_categoria=94, confianza_subcategoria=90
        )

    if contains_any_keyword(desc_norm, AUTO_PARTS_KEYWORDS):
        kw = first_matching_keyword(desc_norm, AUTO_PARTS_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "MANTENCION",
            origen="REGLA_DESC", motivo=f"Keyword repuesto automotriz: {kw}", confianza_categoria=90, confianza_subcategoria=86
        )

    if contains_any_keyword(desc_norm, AGROQ_GENERIC_KEYWORDS):
        kw = first_matching_keyword(desc_norm, AGROQ_GENERIC_KEYWORDS)
        return build_rule_result(
            "INSUMOS_AGRICOLAS", "AGROQUIMICOS", "OTRO",
            origen="REGLA_DESC", motivo=f"Keyword agroquimico: {kw}", confianza_categoria=91, confianza_subcategoria=88
        )

    if contains_any_keyword(desc_norm, ["CONTROL CALIDAD", "CALIDAD"]) and contains_any_keyword(desc_norm, ["BLACK KAT", "CHOLO", "ACOPIO", "COSECHA"]):
        return build_rule_result(
            "COSECHA", "SUPERVISION_COSECHA", "OTRO",
            origen="REGLA_DESC", motivo="Control de calidad en contexto de cosecha", confianza_categoria=88, confianza_subcategoria=84
        )

    if contains_any_keyword(desc_norm, STRUCTURE_INFRA_KEYWORDS) and contains_any_keyword(desc_norm, ["INSTALACION", "OBRA", "ESTRUCTURA", "RED", "POSTE"]):
        return build_rule_result(
            "MANTENCION", "INFRAESTRUCTURA", "OTRO",
            origen="REGLA_DESC", motivo="Trabajo de infraestructura/estructura", confianza_categoria=89, confianza_subcategoria=85
        )

    # 3) Reglas mixtas proveedor + descripcion
    if provider_matches(provider_ctx, ["AGROCON"]):
        if contains_any_keyword(desc_norm, AGROCON_SERVICIOS_KEYWORDS):
            kw = first_matching_keyword(desc_norm, AGROCON_SERVICIOS_KEYWORDS)
            return build_rule_result(
                "TRABAJOS_CAMPO", "SERVICIOS_AGRICOLAS", "OTRO",
                origen="REGLA_RS_DESC", motivo=f"AGROCON + servicio agricola ({kw})", confianza_categoria=95, confianza_subcategoria=91
            )
        if contains_any_keyword(desc_norm, AGROCON_VEHICULO_KEYWORDS):
            kw = first_matching_keyword(desc_norm, AGROCON_VEHICULO_KEYWORDS)
            return build_rule_result(
                "MANTENCION", "VEHICULOS", "COMBUSTIBLE",
                origen="REGLA_RS_DESC", motivo=f"AGROCON + insumo vehiculo ({kw})", confianza_categoria=94, confianza_subcategoria=90
            )

    if provider_matches(provider_ctx, ["AUTOMOTRIZ", "REPUESTOS", "MACROF"]) and contains_any_keyword(desc_norm, AUTO_PARTS_KEYWORDS):
        kw = first_matching_keyword(desc_norm, AUTO_PARTS_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "MANTENCION",
            origen="REGLA_RS_DESC", motivo=f"Proveedor automotriz + repuesto ({kw})", confianza_categoria=96, confianza_subcategoria=93
        )

    if provider_matches(provider_ctx, ["TODOAGRO"]) and (contains_any_keyword(desc_norm, AUTO_PARTS_KEYWORDS) or contains_any_keyword(desc_norm, MACHINERY_RENTAL_KEYWORDS) or contains_any_keyword(desc_norm, ["SEMIEJE", "NEBULIZADOR", "ARRASTRE", "PERNOS"])):
        kw = first_matching_keyword(desc_norm, AUTO_PARTS_KEYWORDS + MACHINERY_RENTAL_KEYWORDS + ["SEMIEJE", "NEBULIZADOR", "ARRASTRE", "PERNOS"])
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "MANTENCION",
            origen="REGLA_RS_DESC", motivo=f"TODOAGRO + repuesto maquinaria ({kw})", confianza_categoria=95, confianza_subcategoria=91
        )

    if provider_matches(provider_ctx, ["GRUPO G Y R", "GRUPO G R", "GRUPO G & R"]) and contains_any_keyword(desc_norm, STRUCTURE_INFRA_KEYWORDS):
        kw = first_matching_keyword(desc_norm, STRUCTURE_INFRA_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "INFRAESTRUCTURA", "OTRO",
            origen="REGLA_RS_DESC", motivo=f"GRUPO G&R + infraestructura ({kw})", confianza_categoria=95, confianza_subcategoria=91
        )

    if provider_matches(provider_ctx, ["JIMENEZ Y PADILLA", "JIMENEZ PADILLA"]) and contains_any_keyword(desc_norm, RIEGO_PARTS_KEYWORDS):
        kw = first_matching_keyword(desc_norm, RIEGO_PARTS_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "RIEGO", "REPUESTOS",
            origen="REGLA_RS_DESC", motivo=f"JIMENEZ Y PADILLA + repuesto riego ({kw})", confianza_categoria=95, confianza_subcategoria=91
        )

    if provider_matches(provider_ctx, ["COPEVAL"]):
        if contains_any_keyword(desc_norm, COPEVAL_INSUMOS_KEYWORDS):
            kw = first_matching_keyword(desc_norm, COPEVAL_INSUMOS_KEYWORDS)
            return build_rule_result(
                "TRABAJOS_CAMPO", "INSUMOS", "OTRO",
                origen="REGLA_RS_DESC", motivo=f"COPEVAL + EPP/herramienta ({kw})", confianza_categoria=93, confianza_subcategoria=88
            )
        if contains_any_keyword(desc_norm, COPEVAL_AGROQ_KEYWORDS):
            kw = first_matching_keyword(desc_norm, COPEVAL_AGROQ_KEYWORDS)
            return build_rule_result(
                "INSUMOS_AGRICOLAS", "AGROQUIMICOS", "OTRO",
                origen="REGLA_RS_DESC", motivo=f"COPEVAL + agroquimico ({kw})", confianza_categoria=95, confianza_subcategoria=92
            )
        if contains_any_keyword(desc_norm, COPEVAL_FIN_KEYWORDS):
            kw = first_matching_keyword(desc_norm, COPEVAL_FIN_KEYWORDS)
            sub = "CREDITOS" if contains_any_keyword(desc_norm, ["INTERESES", "COBRANZA"]) else "BANCOS"
            return build_rule_result(
                "GASTOS_FINANCIEROS", sub, "OTRO",
                origen="REGLA_RS_DESC", motivo=f"COPEVAL + cargo financiero ({kw})", confianza_categoria=90, confianza_subcategoria=86
            )

    if provider_matches(provider_ctx, AUTOPISTA_PROVIDER_PATTERNS) and contains_any_keyword(desc_norm, AUTOPISTA_DESC_KEYWORDS):
        kw = first_matching_keyword(desc_norm, AUTOPISTA_DESC_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "VEHICULOS", "PEAJES",
            origen="REGLA_RS_DESC", motivo=f"Proveedor autopista + keyword {kw}", confianza_categoria=97, confianza_subcategoria=94
        )

    if provider_matches(provider_ctx, ["HYDRORENGO", "HIDRORENGO", "SERVI RIEGO", "OLIVOS"]) and contains_any_keyword(desc_norm, RIEGO_KEYWORDS):
        kw = first_matching_keyword(desc_norm, RIEGO_KEYWORDS)
        return build_rule_result(
            "MANTENCION", "RIEGO", "REPUESTOS",
            origen="REGLA_RS_DESC", motivo=f"Proveedor riego + keyword {kw}", confianza_categoria=97, confianza_subcategoria=94
        )

    if provider_matches(provider_ctx, ["SERVICAMPO"]) and contains_any_keyword(desc_norm, SERVICAMPO_ARRIENDO_KEYWORDS):
        kw = first_matching_keyword(desc_norm, SERVICAMPO_ARRIENDO_KEYWORDS)
        return build_rule_result(
            "TRABAJOS_CAMPO", "ARRIENDO", "OTRO",
            origen="REGLA_RS_DESC", motivo=f"Servicampo + keyword arriendo ({kw})", confianza_categoria=96, confianza_subcategoria=92
        )

    if provider_matches(provider_ctx, ["BANCO DE CREDITO E INVERSIONES", "BANCO CREDITO E INVERSIONES", "BCI"]) and contains_any_keyword(desc_norm, FINANCIAL_BANK_KEYWORDS):
        kw = first_matching_keyword(desc_norm, FINANCIAL_BANK_KEYWORDS)
        return build_rule_result(
            "GASTOS_FINANCIEROS", "BANCOS", "OTRO",
            origen="REGLA_RS_DESC", motivo=f"BCI + operacion bancaria ({kw})", confianza_categoria=95, confianza_subcategoria=92
        )

    _ = build_temporal_hint(fecha_emision)
    return None


def ensure_mantenedor_categoria_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mantenedor_categoria_proveedor (
            razon_social TEXT NOT NULL,
            giro TEXT NOT NULL,
            categoria TEXT NOT NULL,
            confianza_categoria INTEGER DEFAULT 0,
            fecha_actualizacion TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (razon_social, giro)
        );
        """
    )
    con.commit()


def ensure_mantenedor_keyword_table(con: sqlite3.Connection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS mantenedor_keyword_categoria (
            keyword TEXT NOT NULL,
            categoria TEXT NOT NULL,
            subcategoria TEXT,
            tipo_gasto TEXT,
            prioridad INTEGER DEFAULT 50,
            fecha_actualizacion TEXT DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (keyword, categoria, subcategoria, tipo_gasto)
        );
        """
    )
    con.commit()


def get_categoria_from_mantenedor(con: sqlite3.Connection, razon_social: str, giro: str) -> Optional[str]:
    razon_norm = normalize_key_text(razon_social)
    giro_norm = normalize_key_text(giro)
    if not razon_norm:
        return None

    row = con.execute(
        """
        SELECT categoria
        FROM mantenedor_categoria_proveedor
        WHERE razon_social = ? AND giro = ?
        LIMIT 1
        """,
        (razon_norm, giro_norm),
    ).fetchone()
    if row and row[0]:
        return str(row[0]).strip()

    row = con.execute(
        """
        SELECT categoria
        FROM mantenedor_categoria_proveedor
        WHERE razon_social = ?
        ORDER BY fecha_actualizacion DESC
        LIMIT 1
        """,
        (razon_norm,),
    ).fetchone()
    return str(row[0]).strip() if row and row[0] else None


def upsert_mantenedor_categoria(
    con: sqlite3.Connection,
    razon_social: str,
    giro: str,
    categoria: str,
    confianza: int = 0,
) -> None:
    razon_norm = normalize_key_text(razon_social)
    giro_norm = normalize_key_text(giro)
    categoria_norm = (categoria or "").strip()
    if not razon_norm or not categoria_norm:
        return

    con.execute(
        """
        INSERT INTO mantenedor_categoria_proveedor
            (razon_social, giro, categoria, confianza_categoria, fecha_actualizacion)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(razon_social, giro) DO UPDATE SET
            categoria = excluded.categoria,
            confianza_categoria = excluded.confianza_categoria,
            fecha_actualizacion = CURRENT_TIMESTAMP
        """,
        (razon_norm, giro_norm, categoria_norm, int(confianza or 0)),
    )
    con.commit()


def get_keyword_rule_match(con: sqlite3.Connection, descripcion: str) -> Optional[Dict[str, Any]]:
    desc_norm = normalize_text(descripcion)
    if not desc_norm:
        return None

    rows = con.execute(
        """
        SELECT keyword, categoria, subcategoria, tipo_gasto, prioridad
        FROM mantenedor_keyword_categoria
        WHERE keyword IS NOT NULL AND TRIM(keyword) <> ''
        ORDER BY prioridad DESC, LENGTH(keyword) DESC
        """
    ).fetchall()

    for row in rows:
        keyword = (row["keyword"] or "").strip()
        if keyword and contains_any_keyword(desc_norm, [keyword]):
            prioridad = int(row["prioridad"] or 0)
            conf = max(72, min(96, 72 + prioridad // 2))
            return build_rule_result(
                categoria=(row["categoria"] or "").strip(),
                subcategoria=(row["subcategoria"] or "").strip(),
                tipo_gasto=(row["tipo_gasto"] or "OTRO").strip() or "OTRO",
                origen="REGLA_DESC",
                motivo=f"mantenedor_keyword_categoria: {normalize_text(keyword)}",
                confianza_categoria=conf,
                confianza_subcategoria=max(60, conf - 4),
            )
    return None

# =========================
# CATALOGO
# =========================
def fetch_catalogo(con: sqlite3.Connection) -> List[Dict[str, Any]]:
    rows = con.execute(
        """
        SELECT id, categoria_costo, subcategoria_costo, tipo_gasto
        FROM catalogo_costos
        ORDER BY categoria_costo, subcategoria_costo, tipo_gasto
        """
    ).fetchall()
    return [dict(r) for r in rows]


def find_special_ids(catalogo: List[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    id_sin = None
    id_otros = None
    for r in catalogo:
        c = (r.get("categoria_costo") or "").strip().upper()
        s = (r.get("subcategoria_costo") or "").strip().upper()
        if c == "SIN_CLASIFICAR" and s == "PENDIENTE":
            id_sin = int(r["id"])
        if c == "OTROS" and s == "OTROS":
            id_otros = int(r["id"])
    return id_sin, id_otros


def build_catalog_json(catalogo: List[Dict[str, Any]], max_rows: int = 800) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for r in catalogo[:max_rows]:
        out.append(
            {
                "id": int(r["id"]),
                "categoria": r["categoria_costo"],
                "subcategoria": r["subcategoria_costo"],
                "tipo_gasto": r["tipo_gasto"],
            }
        )
    return out


def get_unique_categories(catalogo: List[Dict[str, Any]]) -> List[str]:
    return sorted({(r.get("categoria_costo") or "").strip() for r in catalogo if (r.get("categoria_costo") or "").strip()})


def build_catalog_by_category(catalogo: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for r in catalogo:
        cat = (r.get("categoria_costo") or "").strip()
        if cat:
            by_cat.setdefault(cat, []).append(r)
    return by_cat


def pick_catalog_row(catalogo: List[Dict[str, Any]], categoria: str, subcategoria: str = "", tipo_gasto: str = "") -> Optional[Dict[str, Any]]:
    cat_n = normalize_text(categoria)
    sub_n = normalize_text(subcategoria)
    tipo_n = normalize_text(tipo_gasto)
    in_cat = [r for r in catalogo if normalize_text(r.get("categoria_costo") or "") == cat_n]
    if not in_cat:
        return None

    if sub_n and tipo_n:
        for r in in_cat:
            if normalize_text(r.get("subcategoria_costo") or "") == sub_n and normalize_text(r.get("tipo_gasto") or "") == tipo_n:
                return r

    if sub_n:
        sub_rows = [r for r in in_cat if normalize_text(r.get("subcategoria_costo") or "") == sub_n]
        if sub_rows:
            for r in sub_rows:
                if normalize_text(r.get("tipo_gasto") or "") == "OTRO":
                    return r
            return sub_rows[0]

    for r in in_cat:
        if normalize_text(r.get("subcategoria_costo") or "") == "OTRO" and normalize_text(r.get("tipo_gasto") or "") == "OTRO":
            return r
    for r in in_cat:
        if normalize_text(r.get("subcategoria_costo") or "") == "OTRO":
            return r
    return in_cat[0]


def pick_category_fallback_row(catalogo: List[Dict[str, Any]], categoria: str) -> Optional[Dict[str, Any]]:
    return (
        pick_catalog_row(catalogo, categoria, "OTRO", "OTRO")
        or pick_catalog_row(catalogo, categoria, "OTRO", "")
        or pick_catalog_row(catalogo, categoria, "", "OTRO")
        or pick_catalog_row(catalogo, categoria, "", "")
    )


def map_assistant_result_to_catalog(
    catalogo: List[Dict[str, Any]],
    categoria: str,
    subcategoria: str = "",
) -> Optional[Dict[str, Any]]:
    cat = (categoria or "").strip()
    sub = (subcategoria or "").strip()
    if not cat:
        return None
    if sub:
        exact = pick_catalog_row(catalogo, cat, sub, "")
        if exact:
            return exact
    return pick_category_fallback_row(catalogo, cat)


def get_fallback_row(
    catalogo: List[Dict[str, Any]],
    catalogo_by_id: Dict[int, Dict[str, Any]],
    id_sin: Optional[int],
    id_otros: Optional[int],
) -> Optional[Dict[str, Any]]:
    for fid in [id_sin, id_otros]:
        if fid is not None and int(fid) in catalogo_by_id:
            return catalogo_by_id[int(fid)]
    for cat, sub in [("SIN_CLASIFICAR", "PENDIENTE"), ("SIN_CLASIFICAR", "OTRO")]:
        row = pick_catalog_row(catalogo, cat, sub, "")
        if row:
            return row
    return catalogo[0] if catalogo else None


def combine_motivos(*parts: str) -> str:
    return " | ".join([p.strip() for p in parts if p and p.strip()])


def should_use_assistant_reasoning(
    *,
    final_row: Optional[Dict[str, Any]],
    categoria_elegida: str,
    conf_cat: int,
    origen_categoria: str,
    motivo_categoria: str,
) -> Tuple[bool, str]:
    if final_row is not None:
        return (False, "")

    origen_norm = normalize_text(origen_categoria)
    motivo_norm = normalize_text(motivo_categoria)

    from_rules_or_mantenedor = origen_norm.startswith("REGLA") or origen_norm == "MANTENEDOR"
    no_rule_classified = (not categoria_elegida.strip()) or (not from_rules_or_mantenedor and origen_norm != "ASSISTANT")
    low_confidence = clamp_conf(conf_cat) < max(1, min(100, ASSISTANT_TRIGGER_CONFIDENCE))
    ambiguous = ("ALINEACION MISMO DTE" in motivo_norm) or ("AMBIG" in motivo_norm) or ("DUDOS" in motivo_norm)

    if no_rule_classified:
        return (True, "sin_regla_clara")
    if low_confidence:
        return (True, "confianza_baja")
    if ambiguous:
        return (True, "categoria_ambigua")
    return (False, "")


def add_doc_category_score(scores: Dict[str, Dict[str, int]], doc_id: str, categoria: str, weight: int) -> None:
    cat = (categoria or "").strip()
    if not doc_id or not cat or cat.upper() == "SIN_CLASIFICAR":
        return
    w = max(1, min(100, int(weight or 0)))
    scores.setdefault(doc_id, {})
    scores[doc_id][cat] = scores[doc_id].get(cat, 0) + w


def get_doc_category_anchor(scores: Dict[str, Dict[str, int]], doc_id: str) -> Tuple[str, int]:
    doc_scores = scores.get(doc_id, {})
    if not doc_scores:
        return ("", 0)
    cat, score = max(doc_scores.items(), key=lambda kv: kv[1])
    return (cat, int(score))


def update_detalle(
    con: sqlite3.Connection,
    *,
    id_det: str,
    catalogo_costo_id: Optional[int],
    needs_review: int,
    categoria: str,
    subcategoria: str,
    tipo_gasto: str,
    confianza_categoria: int,
    confianza_subcategoria: int,
    origen_clasificacion: str,
    motivo_clasificacion: str,
    razon_social: str = "",
    giro: str = "",
    fecha_emision: str = "",
) -> None:
    con.execute(
        """
        UPDATE detalle
        SET
            catalogo_costo_id = ?,
            needs_review = ?,
            categoria = ?,
            subcategoria = ?,
            tipo_gasto = ?,
            confianza_categoria = ?,
            confianza_subcategoria = ?,
            origen_clasificacion = ?,
            motivo_clasificacion = ?,
            confianza_ia = ?,
            razon_social = ?,
            giro = ?,
            fecha_emision = ?
        WHERE id_det = ?
        """,
        (
            catalogo_costo_id,
            int(needs_review),
            categoria,
            subcategoria,
            tipo_gasto,
            clamp_conf(confianza_categoria),
            clamp_conf(confianza_subcategoria),
            (origen_clasificacion or "").strip() or "SIN_CLASIFICAR",
            (motivo_clasificacion or "").strip(),
            clamp_conf(confianza_subcategoria),
            (razon_social or "").strip(),
            (giro or "").strip(),
            (fecha_emision or "").strip(),
            id_det,
        ),
    )


def print_line_debug(
    linea: Any,
    origen: str,
    conf_cat: int,
    conf_sub: int,
    categoria: str,
    subcategoria: str,
    tipo_gasto: str,
    motivo: str,
    descripcion: str,
) -> None:
    print(f"[INFO] Linea {linea} | origen={origen} | conf_cat={conf_cat} | conf_sub={conf_sub}")
    print(f"[INFO] Resultado: {categoria} > {subcategoria} > {tipo_gasto}")
    print(f"[INFO] Motivo: {motivo if motivo else '(sin motivo)'}")
    print(f"desc: {descripcion}")


# =========================
# RE-CLASIFICACION IA
# =========================
def choose_category(
    client: OpenAI,
    *,
    model: str,
    categorias: List[str],
    descripcion: str,
    proveedor_hint: str,
    giro_hint: str,
    fecha_emision: Optional[str] = None,
    categoria_prioritaria: str = "",
    rule_hint: str = "",
) -> Dict[str, Any]:
    temporal_hint = build_temporal_hint(fecha_emision)

    system_msg = (
        "Eres un clasificador contable experto en agricultura chilena (campo de cerezas). "
        "Solo debes elegir CATEGORIA. Devuelve SOLO JSON."
    )

    user_msg = (
        "LISTA DE CATEGORIAS (elige una exacta):\n"
        f"{json.dumps(categorias, ensure_ascii=False)}\n\n"
        "CONTEXTO:\n"
        f"- proveedor: {proveedor_hint}\n"
        f"- giro_proveedor: {giro_hint}\n"
        f"- fecha_emision: {fecha_emision or '(sin dato)'}\n"
        f"- categoria_prioritaria: {categoria_prioritaria or '(ninguna)'}\n"
        f"- rule_hint: {rule_hint or '(ninguno)'}\n"
        f"- temporalidad_secundaria: {temporal_hint}\n\n"
        "LINEA:\n"
        f"- descripcion: {descripcion}\n\n"
        "Devuelve SOLO este JSON:\n"
        "{\n"
        "  \"categoria\": \"\",\n"
        "  \"confianza_categoria\": 0,\n"
        "  \"motivo\": \"\"\n"
        "}\n"
        "Reglas:\n"
        "- categoria exacta de la lista.\n"
        "- proveedor+descripcion > temporalidad.\n"
        "- temporalidad solo secundaria.\n"
        "- usa categoria_prioritaria salvo evidencia fuerte en contra.\n"
    )

    call = openai_chat_json_with_retry(
        client,
        model=model,
        messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
        temperature=TEMPERATURE,
        max_retries=MAX_RETRIES_OPENAI,
    )
    if not call.get("ok"):
        return {"ok": False, "error": call.get("error", "openai_error")}

    try:
        data = json.loads(call["content"])
        cat = (data.get("categoria") or "").strip()
        conf = clamp_conf(data.get("confianza_categoria", 0))
        motivo = (data.get("motivo") or "").strip()
        if cat not in categorias:
            return {"ok": False, "error": "categoria_not_in_list", "raw": data}
        return {"ok": True, "categoria": cat, "confianza_categoria": conf, "motivo": motivo}
    except Exception as e:
        return {"ok": False, "error": f"json_parse_error:{e}"}


def shortlist_catalog_ids(
    client: OpenAI,
    *,
    model: str,
    catalog_json: List[Dict[str, Any]],
    valid_ids: set,
    descripcion: str,
    codigo: str,
    cantidad: Any,
    precio_unitario: Any,
    monto_item: Any,
    proveedor_hint: str,
    giro_hint: str,
    fecha_emision: Optional[str] = None,
    categoria_elegida: str = "",
    rule_hint: str = "",
    k: int = 12,
) -> List[int]:
    if not catalog_json:
        return []
    if len(catalog_json) <= k:
        return [int(x["id"]) for x in catalog_json if int(x["id"]) in valid_ids]

    temporal_hint = build_temporal_hint(fecha_emision)
    system_msg = (
        "Eres un clasificador contable experto. En este PASO 1 solo devuelves candidate_ids. "
        "No inventes IDs y prioriza categoria_elegida cuando exista."
    )

    user_msg = (
        "CATALOGO CANDIDATO:\n"
        f"{json.dumps(catalog_json, ensure_ascii=False)}\n\n"
        "CONTEXTO:\n"
        f"- proveedor: {proveedor_hint}\n"
        f"- giro: {giro_hint}\n"
        f"- fecha_emision: {fecha_emision or '(sin dato)'}\n"
        f"- categoria_elegida: {categoria_elegida or '(ninguna)'}\n"
        f"- rule_hint: {rule_hint or '(ninguno)'}\n"
        f"- temporalidad_secundaria: {temporal_hint}\n\n"
        "DETALLE:\n"
        f"- descripcion: {descripcion}\n"
        f"- codigo: {codigo}\n"
        f"- cantidad: {cantidad}\n"
        f"- precio_unitario: {precio_unitario}\n"
        f"- monto_item: {monto_item}\n\n"
        "JSON salida:\n"
        "{\n"
        "  \"candidate_ids\": [],\n"
        "  \"motivo\": \"\"\n"
        "}\n"
        f"candidate_ids maximo: {k}.\n"
    )

    call = openai_chat_json_with_retry(
        client,
        model=model,
        messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
        temperature=0,
        max_retries=MAX_RETRIES_OPENAI,
    )
    if not call.get("ok"):
        return []

    try:
        data = json.loads(call["content"])
        ids = data.get("candidate_ids", [])
        out: List[int] = []
        for x in ids:
            try:
                xi = int(x)
                if xi in valid_ids and xi not in out:
                    out.append(xi)
            except Exception:
                continue
        return out[:k]
    except Exception:
        return []


def classify_one_line(
    client: OpenAI,
    *,
    model: str,
    catalog_json: List[Dict[str, Any]],
    valid_ids: set,
    id_sin: Optional[int],
    id_otros: Optional[int],
    descripcion: str,
    codigo: str,
    cantidad: Any,
    precio_unitario: Any,
    monto_item: Any,
    proveedor_hint: str = "",
    giro_hint: str = "",
    fecha_emision: Optional[str] = None,
    categoria_elegida: str = "",
    rule_hint: str = "",
    candidate_ids: Optional[List[int]] = None,
    catalogo_by_id: Optional[Dict[int, Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    temporal_hint = build_temporal_hint(fecha_emision)

    candidates: List[Dict[str, Any]] = []
    if candidate_ids and catalogo_by_id:
        for cid in candidate_ids:
            row = catalogo_by_id.get(int(cid))
            if row:
                candidates.append(
                    {
                        "id": int(row["id"]),
                        "categoria": row["categoria_costo"],
                        "subcategoria": row["subcategoria_costo"],
                        "tipo_gasto": row["tipo_gasto"],
                    }
                )

    if not candidates:
        candidates = list(catalog_json)

    if categoria_elegida:
        cat_n = normalize_text(categoria_elegida)
        in_cat = [x for x in candidates if normalize_text(x.get("categoria") or "") == cat_n]
        if in_cat:
            candidates = in_cat

    system_msg = (
        "Eres un clasificador contable experto. Debes elegir exactamente 1 ID del catalogo entregado. "
        "Si confianza < 60 -> needs_review=true. Si hay categoria_elegida, no salgas de esa categoria salvo evidencia fuerte. "
        "Devuelve SOLO JSON."
    )

    user_msg = (
        "CATALOGO OPCIONES:\n"
        f"{json.dumps(candidates, ensure_ascii=False)}\n\n"
        "CONTEXTO:\n"
        f"- proveedor: {proveedor_hint}\n"
        f"- giro: {giro_hint}\n"
        f"- fecha_emision: {fecha_emision or '(sin dato)'}\n"
        f"- categoria_elegida: {categoria_elegida or '(ninguna)'}\n"
        f"- rule_hint: {rule_hint or '(ninguno)'}\n"
        f"- temporalidad_secundaria: {temporal_hint}\n\n"
        "DETALLE:\n"
        f"- descripcion: {descripcion}\n"
        f"- codigo: {codigo}\n"
        f"- cantidad: {cantidad}\n"
        f"- precio_unitario: {precio_unitario}\n"
        f"- monto_item: {monto_item}\n\n"
        "Devuelve SOLO:\n"
        "{\n"
        "  \"catalogo_costo_id\": 0,\n"
        "  \"confianza\": 0,\n"
        "  \"needs_review\": false,\n"
        "  \"motivo\": \"\",\n"
        "  \"origen_clasificacion\": \"IA\"\n"
        "}\n"
    )

    call = openai_chat_json_with_retry(
        client,
        model=model,
        messages=[{"role": "system", "content": system_msg}, {"role": "user", "content": user_msg}],
        temperature=TEMPERATURE,
        max_retries=MAX_RETRIES_OPENAI,
    )
    if not call.get("ok"):
        fallback_id = id_sin if id_sin is not None else id_otros
        if fallback_id is None:
            return {"ok": False, "error": call.get("error", "openai_error")}
        return {"ok": True, "catalogo_costo_id": int(fallback_id), "needs_review": 1, "confianza": 0, "motivo": "openai_error", "origen_clasificacion": "IA"}

    try:
        data = json.loads(call["content"])
    except Exception:
        fallback_id = id_sin if id_sin is not None else id_otros
        if fallback_id is None:
            return {"ok": False, "error": "json_parse_error"}
        return {"ok": True, "catalogo_costo_id": int(fallback_id), "needs_review": 1, "confianza": 0, "motivo": "json_parse_error", "origen_clasificacion": "IA"}

    try:
        cid = int(data.get("catalogo_costo_id", 0))
    except Exception:
        cid = 0
    conf = clamp_conf(data.get("confianza", 0))
    needs = bool(data.get("needs_review", False))
    motivo = (data.get("motivo") or "").strip()
    origen = (data.get("origen_clasificacion") or "IA").strip() or "IA"

    valid_candidate_ids = {int(x["id"]) for x in candidates if "id" in x}
    if cid not in valid_candidate_ids:
        cid = id_sin if id_sin is not None else (id_otros if id_otros is not None else 0)

    if cid == 0 or cid not in valid_ids:
        return {"ok": False, "error": "no_valid_catalog_id"}

    return {
        "ok": True,
        "catalogo_costo_id": int(cid),
        "confianza": conf,
        "needs_review": 1 if (needs or conf < 60) else 0,
        "motivo": motivo,
        "origen_clasificacion": origen,
    }


def get_proveedor_y_giro(con: sqlite3.Connection, id_doc: str) -> Tuple[str, str]:
    row = con.execute(
        "SELECT razon_social, giro FROM documentos WHERE id_doc = ? LIMIT 1",
        (id_doc,),
    ).fetchone()
    if not row:
        return ("", "")
    razon = (row["razon_social"] or "").strip() if "razon_social" in row.keys() else (row[0] or "").strip()
    giro = (row["giro"] or "").strip() if "giro" in row.keys() else ((row[1] or "").strip() if len(row) > 1 else "")
    return (razon, giro)


# =========================
# MAIN
# =========================
def main() -> None:
    print(f"[INFO] DB: {DB_PATH}")
    if not os.path.exists(DB_PATH):
        print("[ERROR] No existe la DB en esa ruta.")
        sys.exit(1)

    client = get_openai_client()
    local_clf = get_classifier(DB_PATH)
    con = connect_db(DB_PATH)

    add_column_if_missing_sqlite(con, "detalle", "codigo TEXT")
    add_column_if_missing_sqlite(con, "detalle", "unidad TEXT")
    add_column_if_missing_sqlite(con, "detalle", "needs_review INTEGER")
    add_column_if_missing_sqlite(con, "detalle", "categoria TEXT")
    add_column_if_missing_sqlite(con, "detalle", "subcategoria TEXT")
    add_column_if_missing_sqlite(con, "detalle", "tipo_gasto TEXT")
    add_column_if_missing_sqlite(con, "detalle", "confianza_categoria INTEGER")
    add_column_if_missing_sqlite(con, "detalle", "confianza_subcategoria INTEGER")
    add_column_if_missing_sqlite(con, "detalle", "origen_clasificacion TEXT")
    add_column_if_missing_sqlite(con, "detalle", "motivo_clasificacion TEXT")
    # Columnas desnormalizadas para consulta directa (evita JOIN con documentos)
    add_column_if_missing_sqlite(con, "detalle", "razon_social TEXT")
    add_column_if_missing_sqlite(con, "detalle", "giro TEXT")
    add_column_if_missing_sqlite(con, "detalle", "fecha_emision TEXT")

    ensure_mantenedor_categoria_table(con)
    ensure_mantenedor_keyword_table(con)

    catalogo = fetch_catalogo(con)
    if not catalogo:
        print("[ERROR] catalogo_costos esta vacio o no existe.")
        sys.exit(1)

    categorias = sorted({c.strip() for c in get_unique_categories(catalogo) if c and c.strip()})
    if not categorias:
        print("[ERROR] No hay categorias en catalogo_costos.")
        sys.exit(1)

    id_sin, id_otros = find_special_ids(catalogo)
    if id_sin is None:
        print("[WARN] Recomendado: agrega SIN_CLASIFICAR/PENDIENTE en catalogo_costos.")
    if id_otros is None:
        print("[WARN] Recomendado: agrega OTROS/OTROS en catalogo_costos.")

    catalogo_by_id = {int(r["id"]): r for r in catalogo}
    catalogo_by_category = build_catalog_by_category(catalogo)

    q = """
        SELECT
            d.id_det,
            d.id_doc,
            d.linea,
            d.codigo,
            d.descripcion,
            d.cantidad,
            d.precio_unitario,
            d.monto_item,
            d.needs_review,
            COALESCE(doc.razon_social, '') AS razon_social,
            COALESCE(doc.giro, '') AS giro,
            COALESCE(doc.fecha_emision, '') AS fecha_emision
        FROM detalle d
        LEFT JOIN documentos doc ON doc.id_doc = d.id_doc
        WHERE d.needs_review IS NULL OR d.needs_review = 1
        ORDER BY d.id_doc, d.linea
    """
    if LIMIT_ROWS and LIMIT_ROWS > 0:
        q += f" LIMIT {int(LIMIT_ROWS)}"

    rows = con.execute(q).fetchall()
    print(f"[INFO] Filas para revisar: {len(rows)}")

    updated = 0
    last_doc: Optional[str] = None
    doc_category_scores: Dict[str, Dict[str, int]] = {}

    for i, r in enumerate(rows, start=1):
        id_det = r["id_det"]
        doc_id = r["id_doc"]
        linea = r["linea"]
        desc = (r["descripcion"] or "").strip()
        codigo = (r["codigo"] or "").strip()
        cantidad = r["cantidad"]
        precio_unitario = r["precio_unitario"]
        monto_item = r["monto_item"]
        proveedor_hint = (r["razon_social"] or "").strip()
        giro_hint = (r["giro"] or "").strip()
        fecha_emision = (r["fecha_emision"] or "").strip()
        doc_anchor_cat, doc_anchor_score = get_doc_category_anchor(doc_category_scores, doc_id)

        if doc_id != last_doc:
            print("\n" + "=" * 110)
            print(f"[DOC] DOCUMENTO: {doc_id}")
            print(f"[DOC] EMISOR  : {proveedor_hint if proveedor_hint else '(vacio)'}")
            print(f"[DOC] GIRO    : {giro_hint if giro_hint else '(vacio)'}")
            print("=" * 110)
            last_doc = doc_id

        fallback_row = get_fallback_row(catalogo, catalogo_by_id, id_sin, id_otros)
        if fallback_row is None:
            continue

        if not desc:
            categoria = (fallback_row.get("categoria_costo") or "SIN_CLASIFICAR").strip()
            subcategoria = (fallback_row.get("subcategoria_costo") or "PENDIENTE").strip()
            tipo_gasto = (fallback_row.get("tipo_gasto") or "OTRO").strip()
            update_detalle(
                con,
                id_det=id_det,
                catalogo_costo_id=int(fallback_row["id"]),
                needs_review=1,
                categoria=categoria,
                subcategoria=subcategoria,
                tipo_gasto=tipo_gasto,
                confianza_categoria=0,
                confianza_subcategoria=0,
                origen_clasificacion="REGLA_DESC",
                motivo_clasificacion="descripcion_vacia",
                razon_social=proveedor_hint,
                giro=giro_hint,
                fecha_emision=fecha_emision,
            )
            con.commit()
            print_line_debug(linea, "REGLA_DESC", 0, 0, categoria, subcategoria, tipo_gasto, "descripcion_vacia", desc)
            updated += 1
            time.sleep(BATCH_SLEEP)
            continue

        categoria_elegida = ""
        conf_cat = 0
        origen_categoria = ""
        motivo_categoria = ""
        final_row: Optional[Dict[str, Any]] = None
        conf_sub = 0
        needs_review = 1
        origen_final = "SIN_CLASIFICAR"
        motivo_final = ""

        # 1) Reglas locales
        rule_res = classify_by_rules(proveedor_hint, giro_hint, desc, fecha_emision)
        if rule_res:
            categoria_elegida = (rule_res.get("categoria") or "").strip()
            conf_cat = clamp_conf(rule_res.get("confianza_categoria", 0))
            origen_categoria = (rule_res.get("origen") or "REGLA_DESC").strip()
            motivo_categoria = (rule_res.get("motivo") or "").strip()

            sub_rule = (rule_res.get("subcategoria") or "").strip()
            tipo_rule = (rule_res.get("tipo_gasto") or "OTRO").strip() or "OTRO"
            if categoria_elegida and sub_rule:
                row_pick = pick_catalog_row(catalogo, categoria_elegida, sub_rule, tipo_rule)
                if row_pick:
                    final_row = row_pick
                    conf_sub = clamp_conf(rule_res.get("confianza_subcategoria", conf_cat))
                    needs_review = int(rule_res.get("needs_review", 0) or 0)
                    origen_final = origen_categoria
                    motivo_final = motivo_categoria
                    picked_sub = (row_pick.get("subcategoria_costo") or "").strip()
                    if normalize_text(picked_sub) != normalize_text(sub_rule):
                        motivo_final = combine_motivos(motivo_final, f"ajuste_subcategoria_catalogo:{picked_sub}")

        # 2) Mantenedor keyword
        if final_row is None and not categoria_elegida:
            kw_res = get_keyword_rule_match(con, desc)
            if kw_res:
                categoria_elegida = (kw_res.get("categoria") or "").strip()
                conf_cat = clamp_conf(kw_res.get("confianza_categoria", 0))
                origen_categoria = (kw_res.get("origen") or "REGLA_DESC").strip()
                motivo_categoria = (kw_res.get("motivo") or "").strip()
                sub_rule = (kw_res.get("subcategoria") or "").strip()
                tipo_rule = (kw_res.get("tipo_gasto") or "OTRO").strip() or "OTRO"
                if categoria_elegida and sub_rule:
                    row_pick = pick_catalog_row(catalogo, categoria_elegida, sub_rule, tipo_rule)
                    if row_pick:
                        final_row = row_pick
                        conf_sub = clamp_conf(kw_res.get("confianza_subcategoria", conf_cat))
                        needs_review = int(kw_res.get("needs_review", 0) or 0)
                        origen_final = origen_categoria
                        motivo_final = motivo_categoria

        # 3) Mantenedor proveedor/giro
        if final_row is None and not categoria_elegida:
            cat_mant = get_categoria_from_mantenedor(con, proveedor_hint, giro_hint)
            if cat_mant and cat_mant in categorias:
                categoria_elegida = cat_mant
                conf_cat = 92
                origen_categoria = "MANTENEDOR"
                motivo_categoria = "mantenedor_razon_social_giro"

        # 3.5) Alineacion por mismo DTE
        if final_row is None and not categoria_elegida and doc_anchor_cat and doc_anchor_cat in categorias and doc_anchor_score >= 80:
            categoria_elegida = doc_anchor_cat
            conf_cat = min(95, max(80, doc_anchor_score))
            origen_categoria = "MANTENEDOR"
            motivo_categoria = combine_motivos(motivo_categoria, f"alineacion_mismo_dte:{doc_anchor_cat}")

        # 4) ML Local – cuando reglas y mantenedores no resuelven categoria ni fila
        if final_row is None and not categoria_elegida:
            ml_res = local_clf.predict(desc, proveedor_hint, giro_hint, fecha_emision)
            if ml_res.get("ok"):
                ml_id  = ml_res["catalogo_costo_id"]
                ml_row = catalogo_by_id.get(int(ml_id))
                if ml_row:
                    final_row         = ml_row
                    conf_cat          = clamp_conf(ml_res.get("confianza", 0))
                    conf_sub          = conf_cat
                    needs_review      = ml_res.get("needs_review", 1)
                    categoria_elegida = (ml_row.get("categoria_costo") or "").strip()
                    origen_final      = "ML_LOCAL"
                    motivo_final      = ml_res.get("motivo_clasificacion", "modelo_local")
            else:
                motivo_categoria = combine_motivos(
                    motivo_categoria,
                    f"ml_sin_resultado:{ml_res.get('error')}",
                )

        # 5) Si regla cerro categoria+subcategoria, no usar IA
        if final_row is not None:
            categoria = (final_row.get("categoria_costo") or "").strip()
            subcategoria = (final_row.get("subcategoria_costo") or "").strip()
            tipo_gasto = (final_row.get("tipo_gasto") or "").strip()
            nr = 1 if (needs_review or conf_cat < 60 or conf_sub < 60) else 0
            update_detalle(
                con,
                id_det=id_det,
                catalogo_costo_id=int(final_row["id"]),
                needs_review=nr,
                categoria=categoria,
                subcategoria=subcategoria,
                tipo_gasto=tipo_gasto,
                confianza_categoria=conf_cat,
                confianza_subcategoria=conf_sub,
                origen_clasificacion=origen_final,
                motivo_clasificacion=motivo_final,
                razon_social=proveedor_hint,
                giro=giro_hint,
                fecha_emision=fecha_emision,
            )
            con.commit()
            add_doc_category_score(doc_category_scores, doc_id, categoria, conf_cat)
            print_line_debug(linea, origen_final, conf_cat, conf_sub, categoria, subcategoria, tipo_gasto, motivo_final, desc)

            if nr == 0 and categoria.upper() != "SIN_CLASIFICAR":
                try:
                    upsert_mantenedor_categoria(con, proveedor_hint, giro_hint, categoria, conf_cat)
                except Exception as e:
                    print(f"[WARN] No se pudo actualizar mantenedor_categoria_proveedor: {e}")

            updated += 1
            if i % 10 == 0:
                print(f"[INFO] Progreso: {i}/{len(rows)} | actualizadas: {updated}")
            time.sleep(BATCH_SLEEP)
            continue

        # 6) Si no hay categoria confiable -> SIN_CLASIFICAR
        if not categoria_elegida or conf_cat < 60:
            categoria = (fallback_row.get("categoria_costo") or "SIN_CLASIFICAR").strip()
            subcategoria = (fallback_row.get("subcategoria_costo") or "PENDIENTE").strip()
            tipo_gasto = (fallback_row.get("tipo_gasto") or "OTRO").strip()
            motivo = combine_motivos(motivo_categoria, "categoria_no_confiable_o_vacia")
            update_detalle(
                con,
                id_det=id_det,
                catalogo_costo_id=int(fallback_row["id"]),
                needs_review=1,
                categoria=categoria,
                subcategoria=subcategoria,
                tipo_gasto=tipo_gasto,
                confianza_categoria=conf_cat,
                confianza_subcategoria=0,
                origen_clasificacion=origen_categoria or "SIN_CLASIFICAR",
                motivo_clasificacion=motivo,
                razon_social=proveedor_hint,
                giro=giro_hint,
                fecha_emision=fecha_emision,
            )
            con.commit()
            print_line_debug(linea, origen_categoria or "SIN_CLASIFICAR", conf_cat, 0, categoria, subcategoria, tipo_gasto, motivo, desc)
            updated += 1
            if i % 10 == 0:
                print(f"[INFO] Progreso: {i}/{len(rows)} | actualizadas: {updated}")
            time.sleep(BATCH_SLEEP)
            continue

        # 7) ML Local – subclasificacion cuando categoria ya definida por reglas
        ml_sub_res = local_clf.predict(desc, proveedor_hint, giro_hint, fecha_emision)
        picked: Optional[Dict[str, Any]] = None
        conf_sub_final = 0
        origen_sub = origen_categoria or "ML_LOCAL"
        motivo_sub = motivo_categoria

        if (
            ml_sub_res.get("ok")
            and normalize_text(ml_sub_res.get("categoria", "")) == normalize_text(categoria_elegida)
        ):
            ml_id  = ml_sub_res["catalogo_costo_id"]
            picked = catalogo_by_id.get(int(ml_id))
            if picked:
                conf_sub_final = clamp_conf(ml_sub_res.get("confianza", 0))
                needs_review   = ml_sub_res.get("needs_review", 1)
                motivo_sub     = combine_motivos(motivo_categoria, ml_sub_res.get("motivo_clasificacion", "modelo_local"))
                origen_sub     = combine_motivos(origen_categoria, "ML_LOCAL")

        if picked is None:
            picked         = pick_category_fallback_row(catalogo, categoria_elegida) or fallback_row
            conf_sub_final = 65
            needs_review   = 0
            motivo_sub     = combine_motivos(motivo_categoria, "fallback_subcategoria_otro")
            origen_sub     = combine_motivos(origen_categoria, "ML_FALLBACK")

        categoria    = (picked.get("categoria_costo")   or "").strip()
        subcategoria = (picked.get("subcategoria_costo") or "").strip()
        tipo_gasto   = (picked.get("tipo_gasto")        or "").strip()
        nr           = 1 if (needs_review or conf_cat < 60 or conf_sub_final < 60) else 0

        update_detalle(
            con,
            id_det=id_det,
            catalogo_costo_id=int(picked["id"]),
            needs_review=nr,
            categoria=categoria,
            subcategoria=subcategoria,
            tipo_gasto=tipo_gasto,
            confianza_categoria=conf_cat,
            confianza_subcategoria=conf_sub_final,
            origen_clasificacion=origen_sub,
            motivo_clasificacion=motivo_sub,
            razon_social=proveedor_hint,
            giro=giro_hint,
            fecha_emision=fecha_emision,
        )
        con.commit()
        add_doc_category_score(doc_category_scores, doc_id, categoria, conf_cat)
        print_line_debug(linea, origen_sub, conf_cat, conf_sub_final, categoria, subcategoria, tipo_gasto, motivo_sub, desc)

        if nr == 0 and categoria.upper() != "SIN_CLASIFICAR":
            try:
                upsert_mantenedor_categoria(con, proveedor_hint, giro_hint, categoria, conf_cat)
            except Exception as e:
                print(f"[WARN] No se pudo actualizar mantenedor_categoria_proveedor: {e}")

        updated += 1
        if i % 10 == 0:
            print(f"[INFO] Progreso: {i}/{len(rows)} | actualizadas: {updated}")
        time.sleep(BATCH_SLEEP)

    print(f"[INFO] Terminado. Filas actualizadas: {updated}")
    con.close()


if __name__ == "__main__":
    main()

