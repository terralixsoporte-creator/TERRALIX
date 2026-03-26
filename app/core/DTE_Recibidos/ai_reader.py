# Script Control:
# - Role: AI extraction stage (document + detail) and DB insertion bridge.
# - Track file: docs/SCRIPT_CONTROL.md
import os
import sys
import glob
import base64
import mimetypes
import re
import unicodedata
from datetime import datetime
import sqlite3
import json
import io
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dotenv import load_dotenv
from PIL import Image
import time
import random
import tempfile
import atexit

# =========================
# BASE PATH (raiz TERRALIX)
# =========================
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))
BASE_DIR = _ROOT

from app.core.paths import get_env_path

# =========================
# ENV
# =========================
load_dotenv(str(get_env_path()), override=False)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

AI_OPENAI_MODEL = os.getenv("AI_OPENAI_MODEL")
AI_CATEGORIA_ENABLED = os.getenv("AI_CATEGORIA_ENABLED", "false").lower() == "true"
AI_DETALLE_DEBUG = os.getenv("AI_DETALLE_DEBUG", "false").lower() == "true"

# =========================
# DEPENDENCIAS TERRALIX
# =========================
from app.core.DTE_Recibidos import dte_loader as DL

# =========================
# PyMuPDF (raster PDF)
# =========================
try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

# =========================
# OPENAI CLIENT
# =========================
from openai import OpenAI

def _get_openai_client() -> OpenAI | None:
    key = os.getenv("OPENAI_API_KEY")
    if not key:
        print("OPENAI_API_KEY no configurada. Omite anÃ¡lisis de imagen.")
        return None
    try:
        return OpenAI(api_key=key)
    except Exception as e:
        print(f"No se pudo inicializar OpenAI: {e}")
        return None

def _openai_chat_json_with_retry(
    client: OpenAI,
    *,
    model: str,
    messages: list,
    response_format: dict,
    temperature: float = 0,
    max_retries: int = 10,
    base_sleep: float = 2.0,
    max_sleep: float = 60.0,
    ) -> Dict[str, Any]:
    """
    Llama OpenAI y reintenta ante rate_limit / timeouts / 5xx.
    Devuelve {"ok": True, "content": "..."} o {"ok": False, "error": "..."}.
    """
    last_err = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = client.chat.completions.create(
                model=model,
                messages=messages,
                response_format=response_format,
                temperature=temperature,
            )
            return {"ok": True, "content": resp.choices[0].message.content}
        except Exception as e:
            last_err = str(e)
            msg = last_err.lower()

            is_rate = ("rate limit" in msg) or ("rate_limit" in msg) or ("429" in msg)
            is_quota = ("insufficient_quota" in msg) or ("exceeded your current quota" in msg)
            is_transient = is_rate or ("timeout" in msg) or ("temporarily" in msg) or ("503" in msg) or ("502" in msg) or ("504" in msg)

            # Si es quota, no sirve reintentar
            if is_quota:
                return {"ok": False, "error": "quota_exceeded"}

            # Si no es transitorio, no reintentar
            if (not is_transient) or attempt == max_retries:
                return {"ok": False, "error": last_err}

            # backoff exponencial + jitter
            sleep_s = min(max_sleep, base_sleep * (2 ** (attempt - 1)))
            sleep_s *= (0.75 + random.random() * 0.5)  # jitter 0.75x-1.25x
            print(f"rate_limit/transient (intento {attempt}/{max_retries}) â†’ esperando {sleep_s:.1f}s")
            time.sleep(sleep_s)

    return {"ok": False, "error": last_err or "unknown_error"}

def _to_float_simple(x) -> float:
    """Convierte a float sin fÃ³rmulas:
    - Si es None/""/"null" â†’ 0.0
    - Si es nÃºmero â†’ float(x)
    - Si es string â†’ normaliza miles/decimales ('.' miles, ',' decimales) y convierte.
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        try:
            return float(x)
        except Exception:
            return 0.0
    s = str(x).strip()
    if s == "" or s.lower() == "null":
        return 0.0
    # Normaliza formato chileno: quita puntos de miles, cambia coma por punto
    s = s.replace(".", "").replace(",", ".")
    # Filtra caracteres no numÃ©ricos, permite dÃ­gitos, punto y signo
    s = "".join(ch for ch in s if ch.isdigit() or ch in ".-")
    try:
        return float(s)
    except Exception:
        return 0.0

def _coerce_item_numeric_fields(it: Dict[str, Any]) -> Dict[str, Any]:
    """Convierte claves numÃ©ricas conocidas a float sin aplicar fÃ³rmulas."""
    for k in ("cantidad", "precio_unitario", "monto_item", "descuento", "impuesto_adicional"):
        if k in it:
            it[k] = _to_float_simple(it.get(k))
    return it


def _strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFD", text or "") if unicodedata.category(ch) != "Mn"
    )


def _norm_key(key: Any) -> str:
    s = _strip_accents(str(key or "")).lower().strip()
    s = s.replace("-", "_").replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _clean_codigo(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    code = str(value).strip()
    if not code:
        return ""
    code = code.strip(".,;:|")
    code = re.sub(r"\s+", "", code)
    if len(code) > 48:
        return ""
    # Evita guardar placeholders poco informativos.
    if code.upper() in {"N/A", "NA", "NULL", "NONE", "SINCODIGO", "S/C"}:
        return ""
    return code


def _extract_codigo_from_text(text: str) -> str:
    s = str(text or "").strip()
    if not s:
        return ""

    # Ej: "COD: ABC123" | "SKU-XY99" | "ITEM 12345"
    m = re.search(
        r"\b(?:COD(?:IGO)?|SKU|ITEM)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-./]{1,40})\b",
        s.upper(),
    )
    if m:
        code = _clean_codigo(m.group(1))
        if code:
            return code

    # Ej: "ABC123 - FERTILIZANTE..."
    m = re.match(r"^\s*([A-Z0-9][A-Z0-9\-./]{2,40})\s*[-|]\s+.+$", s.upper())
    if m:
        code = _clean_codigo(m.group(1))
        if code:
            return code

    return ""


def _extract_codigo_from_item(item: Dict[str, Any]) -> str:
    # 1) Campo explícito de código, en sus variantes más comunes.
    for key in (
        "codigo",
        "cod",
        "codigo_producto",
        "codigo_item",
        "codigo_articulo",
        "item_code",
        "sku",
    ):
        if key in item:
            code = _clean_codigo(item.get(key))
            if code:
                return code

    # 2) Fallback: intentar extraer código embebido en descripción.
    desc = str(item.get("detalle") or item.get("descripcion") or "")
    return _extract_codigo_from_text(desc)


def _normalize_item_fields(it: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for raw_k, v in it.items():
        nk = _norm_key(raw_k)
        if nk in {"detalle", "descripcion", "descripcion_item", "glosa", "producto"}:
            if "detalle" not in out:
                out["detalle"] = str(v or "").strip()
        elif nk in {"codigo", "cod", "codigo_producto", "codigo_item", "codigo_articulo", "item_code", "sku"}:
            out["codigo"] = _clean_codigo(v)
        elif nk in {"cantidad", "unidad", "precio_unitario", "descuento", "impuesto_adicional", "monto_item"}:
            out[nk] = v
        else:
            # Conserva campos no estándar para trazabilidad/debug.
            out[str(raw_k)] = v

    if "detalle" not in out:
        out["detalle"] = str(it.get("detalle", it.get("descripcion", "")) or "").strip()

    code = _extract_codigo_from_item(out)
    if code:
        out["codigo"] = code

    return out

def analizar_detalle_desde_imagen(image_path: str) -> Dict[str, Any]:
    """EnvÃ­a el recorte de la tabla de Detalle a OpenAI y devuelve JSON con items.

    Estructura esperada:
    {
    "ok": True,
    "items": [
        {
            "codigo": str | null,
            "detalle"|"descripcion": str,
            "cantidad": number | str,
            "unidad": str | null,
            "precio_unitario": number | str,
            "descuento": number | str | null,
            "impuesto_adicional": number | str | null,
            "monto_item": number | str
        }, ...
        ],
    "quality": { ... }
    }
    """
    # 0) Fallback de debug (solo si el modo debug estÃ¡ habilitado)
    if AI_DETALLE_DEBUG:
        try:
            base = os.path.basename(image_path)
            if base.startswith("detalle_crop_"):
                base = base[len("detalle_crop_"):]
            base_no_ext = os.path.splitext(base)[0]
            project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
            debug_dir = os.path.join(project_root, "data", "debug", "detalle_json")
            debug_json_path = os.path.join(debug_dir, f"{base_no_ext}_items.json")
            if os.path.exists(debug_json_path):
                with open(debug_json_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    if AI_DETALLE_DEBUG:
                        print(f"Usando JSON debug existente: {debug_json_path} â†’ {len(data)} Ã­tems")
                    return {"ok": True, "items": data, "source": "debug_json"}
                elif isinstance(data, dict) and "items" in data:
                    if AI_DETALLE_DEBUG:
                        print(f"Usando JSON debug existente: {debug_json_path} â†’ {len(data.get('items', []))} Ã­tems")
                    return {"ok": True, **data, "source": "debug_json"}
        except Exception as e:
            # No bloquear por errores de lectura de debug; continuar con OpenAI
            if AI_DETALLE_DEBUG:
                print(f"No se pudo leer JSON debug: {e}")

    client = _get_openai_client()
    if client is None:
        return {"ok": False, "error": "missing_api_key", "source": "openai"}

    mime = mimetypes.guess_type(image_path)[0] or "image/jpeg"
    try:
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
    except Exception as e:
        return {"ok": False, "error": f"file_read_error: {e}", "source": "openai"}

    system_msg = (
        "Eres 'Lector Tablas de Facturas' especializado en documentos chilenos (CLP). "
        "Lees tablas de DTE y devuelves JSON con campos estÃ¡ndar. "
        "REGLAS ESTRICTAS: "
        "1. No inventes datos, no conviertas moneda "
        "2. SIEMPRE usa formato chileno para montos: punto (.) para miles, coma (,) para decimales "
        "3. Interpreta '$' como pesos chilenos, omite el sÃ­mbolo pero conserva el formato numÃ©rico chileno "
        "4. Ejemplos: 1.500 (mil quinientos), 25.000,50 (veinticinco mil con cincuenta centavos)"
    )
    user_text = (
        "Extrae filas de la tabla 'Detalle' y normaliza columnas: "
        "codigo (si existe), detalle/descripcion, cantidad, precio_unitario, monto_item, unidad (si existe), "
        "descuento (si existe), impuesto_adicional (si existe). "
        "Si hay una columna de codigo/SKU/item, copiala textual en 'codigo'. "
        "Si el codigo viene al inicio de la descripcion (por ejemplo 'ABC123 - PRODUCTO'), separalo en 'codigo'. "
        "CRÃTICO - Formato de montos: SIEMPRE usa formato chileno estricto: "
        "- Los montos chilenos son ENTEROS (sin decimales) "
        "- Punto (.) ÃšNICAMENTE como separador de miles: 1.000, 25.000, 350.000 "
        "- Ejemplos correctos: '1.500', '25.000', '350.000', '1.234' "
        "- Ejemplos INCORRECTOS: '1,500', '25.000,50', '350000', '1234.56' "
        "- NO agregar decimales (comas) - los pesos chilenos son enteros "
        "Aplica este formato a precio_unitario, monto_item, descuento. "
        "No cambies la escala, omite sÃ­mbolo '$' pero conserva el nÃºmero exacto."
    )

    call = _openai_chat_json_with_retry(
        client,
        model=AI_OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                ],
            },
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )

    if not call.get("ok"):
        err = call.get("error", "openai_error")
        if err == "quota_exceeded":
            return {"ok": False, "error": "quota_exceeded", "source": "openai"}
        return {"ok": False, "error": f"openai_error: {err}", "source": "openai"}

    content = call["content"]
    try:
        data = json.loads(content)
        # Handle both array format and object format from ChatGPT
        if isinstance(data, list):
            result = {"ok": True, "items": data, "source": "openai"}
        elif isinstance(data, dict) and "items" in data:
            r = {"ok": True, **data}
            r["source"] = "openai"
            result = r
        elif isinstance(data, dict) and "Detalle" in data:
            # OpenAI sometimes returns "Detalle" instead of "items"
            result = {"ok": True, "items": data["Detalle"], "source": "openai"}
        elif isinstance(data, dict) and "detalle" in data and isinstance(data.get("detalle"), list):
            # Sometimes returns lowercase 'detalle' with a list of rows
            result = {"ok": True, "items": data["detalle"], "source": "openai"}
        elif isinstance(data, dict) and ("detalle" in data or "descripcion" in data):
            # Some responses return a single item object at the root, possibly partial
            result = {"ok": True, "items": [data], "source": "openai"}
        else:
            # Unexpected format, treat as empty items
            # Provide reason to aid diagnostics
            keys = list(data.keys()) if isinstance(data, dict) else None
            result = {"ok": True, "items": [], "source": "openai", "reason": "empty_items", "content_keys": keys}

        # Normaliza claves y coerciona campos numéricos requeridos.
        try:
            items = result.get("items", [])
            if isinstance(items, list):
                norm_items: List[Any] = []
                for it in items:
                    if isinstance(it, dict):
                        normalized = _normalize_item_fields(dict(it))
                        normalized = _coerce_item_numeric_fields(normalized)
                        norm_items.append(normalized)
                    else:
                        norm_items.append(it)
                result["items"] = norm_items
        except Exception:
            # No bloquear por errores de normalización/coerción; continuar con items originales.
            pass

        # Guardado opcional en JSON de debug para reproducibilidad (solo si estÃ¡ habilitado)
        if AI_DETALLE_DEBUG:
            try:
                os.makedirs(debug_dir, exist_ok=True)
                with open(debug_json_path, "w", encoding="utf-8") as f:
                    # Guardar la respuesta completa para mejor trazabilidad durante debug
                    json.dump(result, f, ensure_ascii=False, indent=2)
                print(f"Guardado JSON debug: {debug_json_path}")
            except Exception as e:
                print(f"No se pudo guardar JSON debug: {e}")

        return result
    except Exception:
        return {"ok": False, "error": "json_parse_error", "raw": content, "source": "openai"}

def analizar_documento_desde_imagen(image_path: str, rut_emisor_target: str, tipo_doc_target: str, folio_target: str) -> Dict[str, Any]:
    """
    Lee la primera pÃ¡gina COMPLETA y extrae SOLO:
    - razon_social del EMISOR con rut_emisor_target
    - giro del EMISOR con rut_emisor_target
    - fecha_emision, montos, referencias (opcional)
    Mantiene doc identity por filename (tipo/rut/folio).
    """
    client = _get_openai_client()
    if client is None:
        return {"ok": False, "error": "missing_api_key", "source": "openai"}

    mime = mimetypes.guess_type(image_path)[0] or "image/jpeg"
    try:
        with open(image_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode("ascii")
    except Exception as e:
        return {"ok": False, "error": f"file_read_error: {e}", "source": "openai"}

    system_msg = (
        "Eres un extractor experto de DTE chilenos (Factura/Boleta/NC/ND/GuÃ­a). "
        "Tu tarea es extraer datos del EMISOR, no del receptor.\n\n"
        "REGLAS ESTRUCTURALES (muy importantes):\n"
        "1) El bloque del EMISOR estÃ¡ en el encabezado superior (normalmente arriba a la izquierda) e incluye RazÃ³n Social y Giro. Su ubicaciÃ³n es sobre el encuadre del RECEPTOR\n"
        "2) El bloque del RECEPTOR suele comenzar con textos como: 'SEÃ‘OR(ES)', 'RECEPTOR', 'CLIENTE', o 'R.U.T.' cerca de 'SEÃ‘OR(ES)' y NO debe usarse para RazÃ³n Social/Giro del emisor.\n"
        "3) Si aparece mÃ¡s de una RazÃ³n Social o mÃ¡s de un 'Giro:', debes elegir el que pertenece al EMISOR.\n"
        "4) No inventes. Si no estÃ¡s seguro, devuelve string vacÃ­o.\n"
        "5) El RUT correcto del EMISOR se entrega como input confiable (desde filename). RazÃ³n Social/Giro deben corresponder al EMISOR, no al receptor.\n\n"
        "Debes responder Ãºnicamente JSON vÃ¡lido con el esquema solicitado. Sin texto extra."
        )

    # Normaliza el rut target (a veces viene sin puntos)
    rut_target_txt = rut_emisor_target.strip()

    user_text = (
        f"Datos confiables por filename (NO debatir, NO corregir):\n"
        f"- tipo_doc = '{tipo_doc_target}'\n"
        f"- rut_emisor = '{rut_target_txt}'\n"
        f"- folio = '{folio_target}'\n\n"
        "Instrucciones (MUY IMPORTANTE):\n"
        "1) Identifica el bloque del EMISOR en el encabezado superior (normalmente arriba a la izquierda, arriba del cuadro del RECEPTOR del DTE).\n"
        "2) Identifica el inicio del bloque del RECEPTOR, tÃ­picamente cuando aparece 'SEÃ‘OR(ES):' (o 'RECEPTOR', 'CLIENTE').\n"
        "3) El receptor es AGRICOLA LAS TIPUANAS SPA, con giro CULTIVO DE CITRICOS. Si te encuentras con estos, ignÃ³ralos y busca el emisor y el giro del emisor.\n"
        "3) Para razon_social y giro del EMISOR, usa SOLO informaciÃ³n del bloque del EMISOR (antes de 'SEÃ‘OR(ES):').\n"
        "   - NO uses datos del receptor.\n"
        "4) Extrae razon_social del emisor (la lÃ­nea principal / mÃ¡s destacada del bloque superior).\n"
        "5) Extrae giro del emisor (lÃ­nea que contiene 'Giro:' o equivalente). Si no existe, deja giro = ''\n"
        "6) Extrae fecha_emision y montos del documento solo si se ven claramente.\n"
        "7) Si existe secciÃ³n 'Referencias', extrae cada referencia.\n\n"
        "Criterio de aceptaciÃ³n (estricto):\n"
        f"- 'emisor.rut_emisor' debe ser EXACTAMENTE '{rut_target_txt}'.\n"
        "- Si NO estÃ¡s seguro de razon_social o giro del EMISOR, deja '' (vacÃ­o) en vez de inventar.\n"
        "- Si no hay referencias, devuelve referencias = []\n"
        "- No incluyas texto fuera del JSON.\n\n"
        "Devuelve SOLO este JSON exacto:\n"
        "{\n"
        "  \"emisor\": {\n"
        "    \"rut_emisor\": \"\",\n"
        "    \"razon_social\": \"\",\n"
        "    \"giro\": \"\"\n"
        "  },\n"
        "  \"doc\": {\n"
        "    \"fecha_emision\": \"\",\n"
        "    \"monto_neto\": \"0\",\n"
        "    \"IVA\": \"0\",\n"
        "    \"monto_exento\": \"0\",\n"
        "    \"impuesto_adicional\": \"0\",\n"
        "    \"monto_total\": \"0\"\n"
        "  },\n"
        "  \"referencias\": [\n"
        "    {\n"
        "      \"tipo_doc_ref\": \"\",\n"
        "      \"folio_ref\": \"\",\n"
        "      \"rut_emisor_ref\": \"\",\n"
        "      \"fecha_ref\": \"\",\n"
        "      \"razon_ref\": \"\",\n"
        "      \"descripcion\": \"\"\n"
        "    }\n"
        "  ]\n"
        "}\n"
    )

    call = _openai_chat_json_with_retry(
        client,
        model=AI_OPENAI_MODEL,
        messages=[
            {"role": "system", "content": system_msg},
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": user_text},
                    {"type": "image_url", "image_url": {"url": f"data:{mime};base64,{b64}"}},
                ],
            },
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )

    if not call.get("ok"):
        err = call.get("error", "openai_error")
        if err == "quota_exceeded":
            return {"ok": False, "error": "quota_exceeded", "source": "openai"}
        return {"ok": False, "error": f"openai_error: {err}", "source": "openai"}

    content = call["content"]
    try:
        data = json.loads(content)
        data.setdefault("emisor", {})
        data.setdefault("doc", {})
        refs = data.get("referencias", [])
        if refs is None or not isinstance(refs, list):
            refs = []
        data["referencias"] = refs

        # Forzar rut emisor al target (anclaje)
        data["emisor"]["rut_emisor"] = rut_target_txt

        # defaults
        data["emisor"].setdefault("razon_social", "")
        data["emisor"].setdefault("giro", "")
        for k in ["fecha_emision", "monto_neto", "IVA", "monto_exento", "impuesto_adicional", "monto_total"]:
            data["doc"].setdefault(k, "0" if k != "fecha_emision" else "")

        return {"ok": True, **data, "source": "openai"}
    except Exception:
        return {"ok": False, "error": "json_parse_error", "raw": content, "source": "openai"}

from PIL import Image
try:
    import fitz
except Exception:
    fitz = None
from app.core.DTE_Recibidos import dte_loader as DL

def _rasterize_first_page(pdf_path: str) -> Optional[str]:
    """
    Rasteriza la primera pÃ¡gina del PDF y la guarda como JPG en un archivo temporal.
    Devuelve el path del JPG temporal (para mandar a la API).
    OJO: este archivo debe borrarse luego (ver read_one_pdf_with_ai).
    """
    if not fitz:
        return None

    try:
        doc = fitz.open(pdf_path)
        page = doc[0]
        pix = page.get_pixmap(dpi=200)

        # Crear archivo temporal (NO se borra automÃ¡ticamente porque la API necesita leerlo)
        tmp = tempfile.NamedTemporaryFile(prefix="dte_p1_", suffix=".jpg", delete=False)
        tmp_path = tmp.name
        tmp.close()

        img = Image.open(io.BytesIO(pix.tobytes("jpg")))
        img.save(tmp_path, "JPEG")

        doc.close()

        # Seguro extra: si el proceso se cae, igual intentarÃ¡ borrarlo al salir
        atexit.register(lambda p=tmp_path: os.path.exists(p) and os.remove(p))

        return tmp_path
    except Exception:
        return None

def _infer_from_filename(filename: str) -> Tuple[str, str, str]:
    base = os.path.splitext(os.path.basename(filename))[0]
    parts = [p for p in base.split("_") if p]
    folio = "0"
    rut = ""
    # Buscar folio desde el final
    for i in range(len(parts) - 1, -1, -1):
        f = DL.parse_folio(parts[i])
        if f and f != "0":
            folio = f
            parts.pop(i)
            break
    # Buscar rut desde el final
    for i in range(len(parts) - 1, -1, -1):
        r = DL.parse_rut(parts[i])
        if r:
            rut = r
            parts.pop(i)
            break
    # El resto conforma el tipo de documento
    tipo = DL._collapse_spaces(" ".join(parts)) if parts else "Factura"
    return tipo, rut, folio

def _build_referencia_text(refs: List[Dict[str, Any]]) -> str:
    # Texto humano con detalle de referencia
    lines = []
    for r in refs:
        parts = []
        if r.get("tipo_doc_ref"): parts.append(r["tipo_doc_ref"])
        if r.get("folio_ref"): parts.append(f"Folio {r['folio_ref']}")
        if r.get("rut_emisor_ref"): parts.append(f"RUT {r['rut_emisor_ref']}")
        if r.get("fecha_ref"): parts.append(f"Fecha {r['fecha_ref']}")
        if r.get("descripcion"): parts.append(r["descripcion"])
        if r.get("razon_ref"): parts.append(r["razon_ref"])
        if parts:
            lines.append(" - ".join(parts))
    return " | ".join(lines)

def _build_dte_referencia(refs: List[Dict[str, Any]]) -> str:
    # Campo compacto: Tipo+Folio (+rut si existe)
    # ejemplo: "GUIA DESPACHO:1234:76123456-7|NOTA CREDITO:55:..."
    parts = []
    for r in refs:
        t = (r.get("tipo_doc_ref") or "").strip()
        f = (r.get("folio_ref") or "").strip()
        rut = (r.get("rut_emisor_ref") or "").strip()
        if not (t or f or rut):
            continue
        chunk = ":".join([x for x in [t, f, rut] if x])
        parts.append(chunk)
    return "|".join(parts)

DOC_COLUMNS = [
    "id_doc", "tipo_doc", "folio", "rut_emisor", "razon_social", "giro",
    "fecha_emision", "IVA", "monto_excento", "impuesto_adicional", "monto_total",
    "referencia", "DTE_referencia", "ruta_pdf", "detalle_link",
    "fecha_carga", "detalle"
]

def _build_document_row_preview(datos_raw: Dict[str, Any], pdf_path: str) -> Dict[str, Any]:
    """
    Arma un dict con el formato lÃ³gico de la tabla documento (solo para debug).
    NO toca BD.
    """
    tipo = str(datos_raw.get("Tipo Documento", "") or "")
    rut = str(datos_raw.get("Emisor", "") or "")
    folio = str(datos_raw.get("Numero de Folio", "") or "")
    id_doc = f"{tipo}_{rut}_{folio}"

    return {
        "id_doc": id_doc,
        "tipo_doc": tipo,
        "folio": folio,
        "rut_emisor": rut,
        "razon_social": datos_raw.get("Razon Social", ""),
        "giro": datos_raw.get("Giro", ""),
        "fecha_emision": datos_raw.get("Fecha Emision", ""),
        "IVA": datos_raw.get("IVA", "0"),
        "monto_excento": datos_raw.get("Monto Exento", "0"),
        "impuesto_adicional": datos_raw.get("Impuesto Adicional", "0"),
        "monto_total": datos_raw.get("Total", "0"),
        "referencia": datos_raw.get("Referencia", ""),
        "DTE_referencia": datos_raw.get("DTE_referencia", ""),              # si tu loader lo usa, si no queda ""
        "ruta_pdf": pdf_path,
        "detalle_link": datos_raw.get("detalle_link", ""),   # si existe
        "saldo": datos_raw.get("saldo", ""),                 # si existe
        "categoria_doc": datos_raw.get("categoria_doc", ""), # si existe
        "fecha_carga": datetime.now().isoformat(timespec="seconds"),
        "detalle": "__items_detalle__",
    }

def _print_debug_insert_preview(pdf_path: str, datos_raw: Dict[str, Any]) -> None:
    doc_row = _build_document_row_preview(datos_raw, pdf_path)
    detalle = datos_raw.get("__items_detalle__", [])

    print("\n" + "=" * 110)
    print(f"DEBUG INSERT PREVIEW - {os.path.basename(pdf_path)}")
    print("=" * 110)

    print("\n[DOCUMENTO] (lo que deberÃ­a insertarse en tabla documento)")
    print(json.dumps(doc_row, ensure_ascii=False, indent=2))

    print("\n[DETALLE] (lo que deberÃ­a insertarse en tabla detalle)")
    if isinstance(detalle, list):
        print(f"Items: {len(detalle)}")
        print(json.dumps(detalle, ensure_ascii=False, indent=2))
    else:
        print("Detalle no es lista, valor:")
        print(detalle)

    print("=" * 110 + "\n")

def read_one_pdf_with_ai(pdf_path: str, debug: bool = False) -> Dict[str, Any]:
    img_path = _rasterize_first_page(pdf_path)
    if not img_path:
        return {"ok": False, "error": "raster_failed"}

    try:
        # 0) IDENTIDAD DESDE FILENAME (MANDATORIO)
        tipo_fn, rut_fn, folio_fn = _infer_from_filename(pdf_path)

        # 1) Documento anclado al RUT del filename
        doc_ai = analizar_documento_desde_imagen(
            img_path,
            rut_emisor_target=rut_fn,
            tipo_doc_target=tipo_fn,
            folio_target=folio_fn
        )
        if not doc_ai.get("ok"):
            return {"ok": False, "error": f"doc_ai_error: {doc_ai.get('error')}"}

        emisor = doc_ai.get("emisor", {}) or {}
        doc = doc_ai.get("doc", {}) or {}
        refs = doc_ai.get("referencias", []) or []

        # 2) Detalle
        det_ai = analizar_detalle_desde_imagen(img_path)
        if not det_ai.get("ok"):
            return {"ok": False, "error": det_ai.get("error", "ai_error")}
        items = det_ai.get("items", [])

        if debug:
            print("=== DOC AI ===")
            print(json.dumps(doc_ai, ensure_ascii=False, indent=2))
            print("=== DET AI ===")
            print(json.dumps(det_ai, ensure_ascii=False, indent=2))

        # 3) Construir referencia / DTE_referencia
        referencia_txt = _build_referencia_text(refs)
        dte_ref_txt = _build_dte_referencia(refs)

        datos_raw = {
            "Tipo Documento": tipo_fn,
            "Emisor": rut_fn,
            "Numero de Folio": folio_fn,
            "Razon Social": (emisor.get("razon_social") or "").strip(),
            "Giro": (emisor.get("giro") or "").strip(),
            "Fecha Emision": (doc.get("fecha_emision") or "").strip(),
            "Monto Neto": doc.get("monto_neto", "0"),
            "IVA": doc.get("IVA", "0"),
            "Monto Exento": doc.get("monto_exento", "0"),
            "Impuesto Adicional": doc.get("impuesto_adicional", "0"),
            "Total": doc.get("monto_total", "0"),
            "Referencia": referencia_txt,
            "DTE_referencia": dte_ref_txt,
            "__items_detalle__": items,
        }

        # El preview completo puede ser muy pesado en consola/GUI.
        # Solo mostrarlo en modo debug.
        if debug or AI_DETALLE_DEBUG:
            _print_debug_insert_preview(pdf_path, datos_raw)

        DL.guardar_en_bd(datos_raw, pdf_path)

        return {
            "ok": True,
            "items": items,
            "doc_id": DL.build_id_doc(tipo_fn, rut_fn, folio_fn)
        }

    finally:
        # ðŸ”¥ borrar SIEMPRE la imagen temporal
        try:
            if img_path and os.path.exists(img_path):
                os.remove(img_path)
        except Exception as e:
            print(f"No se pudo borrar temp image: {img_path} ({e})")
    
def _collect_target_files(file_arg: Optional[str], dir_arg: Optional[str]) -> List[str]:
    out: List[str] = []
    if file_arg and os.path.isfile(file_arg):
        out = [os.path.abspath(file_arg)]
    elif dir_arg and os.path.isdir(dir_arg):
        out = sorted(glob.glob(os.path.join(dir_arg, "*.pdf")))
    else:
        env_dir = os.getenv("RUTA_PDF_DTE_RECIBIDOS")
        if env_dir and os.path.isdir(env_dir):
            out = sorted(glob.glob(os.path.join(env_dir, "*.pdf")))
    return out

def _get_db_path() -> str:
    """
    Resuelve el path real de la DB SQLite.
    Lee DB_PATH_DTE_RECIBIDOS desde config.env (absoluto o relativo a BASE_DIR).
    Fallback: BASE_DIR/data/DteRecibidos_db.db
    """
    env_db = (os.getenv("DB_PATH_DTE_RECIBIDOS") or "").strip().strip('"').strip("'")

    if env_db:
        p = Path(env_db)

        # Absoluto
        if p.is_absolute() and p.is_file():
            return str(p)

        # Relativo a BASE_DIR
        p2 = (BASE_DIR / env_db).resolve()
        if p2.is_file():
            return str(p2)

    fallback = (BASE_DIR / "data" / "DteRecibidos_db.db").resolve()
    return str(fallback)

def _doc_exists_in_db(id_doc: str, db_path: str) -> bool:
    """
    True si existe id_doc en tabla documentos.
    """
    con = None
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("SELECT 1 FROM documentos WHERE id_doc = ? LIMIT 1;", (id_doc,))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"Error consultando DB ({db_path}): {e}")
        # Si falla la consulta, NO saltar: mejor procesar para no perder docs
        return False
    finally:
        try:
            if con:
                con.close()
        except Exception:
            pass


def _predict_pdf_filename(tipo_doc: str, rut_emisor: str, folio: str) -> str:
    nombre_pdf = f"{tipo_doc}_{rut_emisor}_{folio}.pdf"
    return nombre_pdf.replace("/", "_").replace(" ", "_")


def _resolve_pdf_path_current_machine(
    stored_pdf_path: str,
    *,
    pdf_dir: Optional[str] = None,
    tipo_doc: str = "",
    rut_emisor: str = "",
    folio: str = "",
) -> Optional[str]:
    base_dir_raw = (pdf_dir or os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "").strip()
    base_dir = Path(base_dir_raw).expanduser() if base_dir_raw else None

    raw = (stored_pdf_path or "").strip()
    p = Path(raw).expanduser() if raw else None

    candidates: List[Path] = []

    if base_dir and p and p.name:
        candidates.append(base_dir / p.name)
    if base_dir and p and not p.is_absolute():
        candidates.append(base_dir / p)
    if p:
        candidates.append(p)

    if base_dir and tipo_doc and rut_emisor and folio:
        candidates.append(base_dir / _predict_pdf_filename(tipo_doc, rut_emisor, folio))

    seen = set()
    for c in candidates:
        key = str(c).lower()
        if key in seen:
            continue
        seen.add(key)
        try:
            if c.exists():
                return str(c.resolve())
        except Exception:
            continue
    return None


def backfill_missing_codigo_from_pdfs(
    db_path: Optional[str] = None,
    *,
    pdf_dir: Optional[str] = None,
    categoria_objetivo: Optional[str] = "INSUMOS_AGRICOLAS",
    max_docs: int = 0,
    debug: bool = False,
) -> Dict[str, Any]:
    """
    Rellena detalle.codigo para filas vacías leyendo nuevamente el detalle desde PDF.
    Por defecto se enfoca en categoria INSUMOS_AGRICOLAS.
    """
    db_resolved = db_path or _get_db_path()
    if not db_resolved or not os.path.isfile(db_resolved):
        return {"ok": False, "error": f"db_not_found:{db_resolved}"}

    con = sqlite3.connect(db_resolved)
    con.row_factory = sqlite3.Row
    try:
        cur = con.cursor()
        cols = [r[1] for r in cur.execute("PRAGMA table_info(detalle);").fetchall()]
        if "codigo" not in cols:
            cur.execute("ALTER TABLE detalle ADD COLUMN codigo TEXT")
            con.commit()
            print("[MIGRATION] Columna detalle.codigo agregada.")

        where_parts = ["(d.codigo IS NULL OR TRIM(d.codigo) = '')"]
        params: List[Any] = []
        if categoria_objetivo and categoria_objetivo.upper() != "ALL":
            where_parts.append("UPPER(TRIM(COALESCE(d.categoria, ''))) = ?")
            params.append(categoria_objetivo.strip().upper())

        q = f"""
            SELECT
                d.id_doc,
                COUNT(*) AS lineas_sin_codigo,
                MAX(COALESCE(doc.ruta_pdf, '')) AS ruta_pdf,
                MAX(COALESCE(doc.tipo_doc, '')) AS tipo_doc,
                MAX(COALESCE(doc.rut_emisor, '')) AS rut_emisor,
                MAX(COALESCE(doc.folio, '')) AS folio
            FROM detalle d
            LEFT JOIN documentos doc ON doc.id_doc = d.id_doc
            WHERE {' AND '.join(where_parts)}
            GROUP BY d.id_doc
            ORDER BY d.id_doc
        """
        if max_docs and max_docs > 0:
            q += f" LIMIT {int(max_docs)}"

        docs = cur.execute(q, params).fetchall()
        if not docs:
            return {
                "ok": True,
                "categoria_objetivo": categoria_objetivo or "ALL",
                "n_docs_objetivo": 0,
                "n_docs_procesados": 0,
                "n_docs_con_pdf_faltante": 0,
                "n_docs_error_ia": 0,
                "n_filas_codigo_actualizadas": 0,
            }

        n_docs_procesados = 0
        n_docs_con_pdf_faltante = 0
        n_docs_error_ia = 0
        n_filas_codigo_actualizadas = 0

        for i, d in enumerate(docs, start=1):
            id_doc = (d["id_doc"] or "").strip()
            ruta_pdf_db = (d["ruta_pdf"] or "").strip()
            tipo_doc = (d["tipo_doc"] or "").strip()
            rut_emisor = (d["rut_emisor"] or "").strip()
            folio = (d["folio"] or "").strip()

            pdf_path = _resolve_pdf_path_current_machine(
                ruta_pdf_db,
                pdf_dir=pdf_dir,
                tipo_doc=tipo_doc,
                rut_emisor=rut_emisor,
                folio=folio,
            )
            if not pdf_path:
                n_docs_con_pdf_faltante += 1
                print(f"[BACKFILL-CODIGO][SKIP] PDF no encontrado para {id_doc}")
                continue

            print(
                f"[BACKFILL-CODIGO] ({i}/{len(docs)}) Leyendo detalle de {id_doc} "
                f"(faltantes={int(d['lineas_sin_codigo'] or 0)})"
            )

            img_path = _rasterize_first_page(pdf_path)
            if not img_path:
                n_docs_error_ia += 1
                print(f"[BACKFILL-CODIGO][ERROR] No se pudo rasterizar PDF: {pdf_path}")
                continue

            try:
                det_ai = analizar_detalle_desde_imagen(img_path)
                if not det_ai.get("ok"):
                    n_docs_error_ia += 1
                    print(f"[BACKFILL-CODIGO][ERROR] IA detalle fallo para {id_doc}: {det_ai.get('error')}")
                    continue

                items = det_ai.get("items", [])
                if not isinstance(items, list) or not items:
                    print(f"[BACKFILL-CODIGO][WARN] Sin items IA para {id_doc}.")
                    continue

                updated_doc = 0
                for linea, it in enumerate(items, start=1):
                    if not isinstance(it, dict):
                        continue
                    item_norm = _normalize_item_fields(dict(it))
                    code = _extract_codigo_from_item(item_norm)
                    if not code:
                        continue
                    r_upd = cur.execute(
                        """
                        UPDATE detalle
                        SET codigo = ?
                        WHERE id_doc = ?
                          AND linea = ?
                          AND (codigo IS NULL OR TRIM(codigo) = '')
                        """,
                        (code, id_doc, linea),
                    )
                    if r_upd.rowcount and r_upd.rowcount > 0:
                        updated_doc += int(r_upd.rowcount)

                if updated_doc > 0:
                    con.commit()
                    n_filas_codigo_actualizadas += updated_doc
                    print(f"[BACKFILL-CODIGO][OK] {id_doc}: {updated_doc} fila(s) actualizada(s).")
                elif debug:
                    print(f"[BACKFILL-CODIGO][DEBUG] {id_doc}: sin cambios.")

                n_docs_procesados += 1
            finally:
                try:
                    if img_path and os.path.exists(img_path):
                        os.remove(img_path)
                except Exception:
                    pass

        return {
            "ok": True,
            "categoria_objetivo": categoria_objetivo or "ALL",
            "n_docs_objetivo": len(docs),
            "n_docs_procesados": n_docs_procesados,
            "n_docs_con_pdf_faltante": n_docs_con_pdf_faltante,
            "n_docs_error_ia": n_docs_error_ia,
            "n_filas_codigo_actualizadas": n_filas_codigo_actualizadas,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            con.close()
        except Exception:
            pass

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--file")
    parser.add_argument("--dir")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--backfill-codigo",
        action="store_true",
        help="Rellena detalle.codigo releeyendo PDFs para filas con codigo vacio.",
    )
    parser.add_argument(
        "--categoria",
        default="INSUMOS_AGRICOLAS",
        help="Categoria objetivo para --backfill-codigo (usa ALL para sin filtro).",
    )
    parser.add_argument(
        "--max-docs",
        type=int,
        default=0,
        help="Limite de documentos a procesar en --backfill-codigo (0 = sin limite).",
    )
    args = parser.parse_args()

    db_path = _get_db_path()
    if not os.path.isfile(db_path):
        print(f"DB no encontrada en: {db_path}")
        print("Revisa DB_PATH_DTE_RECIBIDOS en config.env o que el archivo exista.")
        sys.exit(1)

    if args.backfill_codigo:
        result = backfill_missing_codigo_from_pdfs(
            db_path=db_path,
            pdf_dir=args.dir,
            categoria_objetivo=args.categoria,
            max_docs=args.max_docs,
            debug=args.debug,
        )
        print(f"[BACKFILL-CODIGO] {result}")
        sys.exit(0 if result.get("ok") else 1)

    targets = _collect_target_files(args.file, args.dir)
    if not targets:
        print("No se encontraron PDFs para procesar.")
        sys.exit(1)

    for i, pdf in enumerate(targets):
        # 0) IDENTIDAD DESDE FILENAME (MANDATORIO)
        tipo_fn, rut_fn, folio_fn = _infer_from_filename(pdf)
        id_doc = DL.build_id_doc(tipo_fn, rut_fn, folio_fn)

        # Si ya existe en DB -> saltar
        if _doc_exists_in_db(id_doc, db_path):
            print(f"\nYa existe en DB -> {id_doc} (skip) [{i+1}/{len(targets)}]")
            continue

        print(f"\nIA leyendo ({i+1}/{len(targets)}): {pdf}")
        print(f"id_doc: {id_doc}")

        max_retries = 4
        attempt = 0
        success = False

        while attempt < max_retries and not success:
            attempt += 1
            if attempt > 1:
                print(f"Reintentando ({attempt}/{max_retries})...")

            res = read_one_pdf_with_ai(pdf, debug=args.debug)

            if res.get("ok"):
                print(f"Guardado en BD: {res.get('doc_id')} - items: {len(res.get('items', []))}")
                success = True
            else:
                print(f"Error: {res.get('error')}")
                if attempt < max_retries:
                    wait = 3 * attempt
                    print(f"Esperando {wait}s antes de reintentar...")
                    time.sleep(wait)

        if not success:
            print(f"No se pudo procesar despues de {max_retries} intentos -> {pdf}")

        # throttle para evitar rate limits
        time.sleep(1.2)

