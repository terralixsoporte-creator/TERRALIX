# Script Control:
# - Role: SQLite schema and persistence utilities for DTE Recibidos.
import os
import re
import warnings
import io
import json
import unicodedata
import shutil
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Table, Column, Integer, String, Float,
    Date, MetaData, ForeignKey, TIMESTAMP, text, select, event,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError

# === BASE PATH (raiz del proyecto TERRALIX) ===
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.append(str(_ROOT))

from app.core.paths import get_env_path, get_user_data_dir
from app.core.cloud_sync import initialize_supabase_sync

# === RUTAS CLAVE ===
load_dotenv(str(get_env_path()), override=False)

warnings.filterwarnings("ignore", category=UserWarning)

# Fallbacks escribibles en AppData (nunca en _internal/)
_user_data = get_user_data_dir()
DEFAULT_DB_PATH = str(_user_data / "DteRecibidos_db.db")
DEFAULT_PDF_DIR = str(_user_data / "pdfs")

DB_PATH  = (os.getenv("DB_PATH_DTE_RECIBIDOS") or "").strip().strip("").strip("’") or DEFAULT_DB_PATH
RUTA_PDF = (os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "").strip().strip("").strip("’") or DEFAULT_PDF_DIR

# Asegura directorios para DB y PDF
try:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
except Exception:
    pass
try:
    os.makedirs(RUTA_PDF, exist_ok=True)
except Exception:
    pass

# === MODELOS ===
# (YOLO y EasyOCR eliminados — la lectura de PDFs se hace via ai_reader.py)

# ============================================================
# ðŸ§± CONEXIÃ“N Y ESTRUCTURA DE LA BASE DE DATOS
# ============================================================

def rut_sin_puntos(rut: str) -> str:
    return rut.replace(".", "")

def slug(s: str) -> str:
    """Convierte a un slug sencillo para id_doc."""
    s = " ".join(str(s).split())
    s = s.replace("/", ":").replace("\\", "_")
    s = re.sub(r"\s+", "_", s)
    return s

def build_id_doc(tipo_doc: str, rut_emisor: str, folio: str) -> str:
    return f"{slug(tipo_doc)}_{rut_sin_puntos(rut_emisor)}_{folio}"

try:
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )

except Exception as e:
    print(f"No se pudo inicializar la base de datos en {DB_PATH}: {e}")
    # intento de fallback a la ruta por defecto
    DB_PATH = DEFAULT_DB_PATH
    print(f"Intentando fallback a {DB_PATH}")
    engine = create_engine(
        f"sqlite:///{DB_PATH}",
        echo=False,
        connect_args={"check_same_thread": False},
        pool_pre_ping=True,
    )
    
# Configura SQLite para mejor concurrencia
try:
    with engine.connect() as _conn:
        _conn.exec_driver_sql("PRAGMA journal_mode=WAL;")
        _conn.exec_driver_sql("PRAGMA synchronous=NORMAL;")
except Exception as e:
    print(f"No se pudo establecer WAL/synchronous en SQLite: {e}")

@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    try:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()
    except Exception:
        pass

meta = MetaData()

# âœ… Tabla cabecera (documentos)
documentos = Table(
    "documentos", meta,
    Column("id_doc", String, primary_key=True),
    Column("tipo_doc", String),
    Column("folio", String),
    Column("rut_emisor", String),
    Column("razon_social", String),
    Column("giro", String),
    Column("fecha_emision", Date),
    Column("IVA", Float),
    Column("monto_excento", Float),
    Column("impuesto_adicional", Float),
    Column("monto_total", Float),
    Column("referencia", String),
    Column("DTE_referencia", String),
    Column("ruta_pdf", String),
    Column("detalle_link", String),
    Column("fecha_carga", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

# âœ… Tabla detalle (lÃ­neas) + FK con CASCADE + columnas nuevas
detalle = Table(
    "detalle", meta,
    Column("id_det", String, primary_key=True),
    Column(
        "id_doc",
        String,
        ForeignKey("documentos.id_doc", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False
    ),
    Column("linea", Integer),
    Column("codigo", String),
    Column("descripcion", String),
    Column("unidad", String),
    Column("cantidad", Float),
    Column("precio_unitario", Float),
    Column("impuesto_adicional", Float),
    Column("Descuento", Float),
    Column("monto_item", Float),

    # âœ… columnas nuevas (se llenan despuÃ©s por IA)
    Column("categoria", String),
    Column("subcategoria", String),
    Column("tipo_gasto", String),
)

try:
    meta.create_all(engine)
    print("Tablas creadas/verificadas: documentos, detalle")
except Exception as e:
    print(f"Error creando/verificando tablas: {e}")

# ============================================================
# ðŸ§© FUNCIONES AUXILIARES (OCR + BASE DE DATOS)
# ============================================================

def add_column_if_missing(engine, table, coldef_sql):
    with engine.begin() as conn:
        cols = [r[1] for r in conn.exec_driver_sql(f"PRAGMA table_info({table});")]
        colname = coldef_sql.split()[2]
        if colname not in cols:
            conn.exec_driver_sql(f"ALTER TABLE {table} {coldef_sql}")

import re

def _to_float(numstr: str) -> float:
    """
    Convierte strings numericos tolerando:
    - miles con punto/coma: 11.246 / 11,246
    - decimales con punto/coma: 90000.0 / 90000,0
    - texto mixto con simbolos.
    """
    if numstr is None:
        return 0.0
    if isinstance(numstr, (int, float)):
        return float(numstr)

    s = str(numstr).strip().replace(" ", "").replace("\u00A0", "")
    s = re.sub(r"[^0-9,.\-]", "", s)
    if not s or s in {"-", ".", ","}:
        return 0.0

    negative = s.startswith("-")
    if negative:
        s = s[1:]
    if not s:
        return 0.0

    if "." in s and "," in s:
        # Usa como decimal el separador que aparece mas a la derecha.
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        # 12,345,678 -> miles; 1234,56 -> decimal
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            s = "".join(parts)
        else:
            s = s.replace(",", ".")
    elif "." in s:
        parts = s.split(".")
        # 12.345.678 -> miles; 90000.0 -> decimal
        if len(parts) > 1 and all(len(p) == 3 for p in parts[1:]):
            s = "".join(parts)

    try:
        val = float(s)
        return -val if negative else val
    except Exception:
        return 0.0

def extraer_monto(txt: str, default: float = 0.0) -> float:
    """
    Extrae el monto priorizando el nÃºmero DESPUÃ‰S del ÃšLTIMO '$'.
    Si no hay '$', toma el Ãºltimo nÃºmero plausible en el texto.
    Acepta formatos con miles y decimales tipo '351.500' o '1,23'.
    """
    if txt is None:
        return default
    if isinstance(txt, (int, float)):
        return float(txt)

    s = " ".join(str(txt).split())
    if not s:
        return default

    # Prioriza texto posterior al ultimo '$' (suele ser el monto objetivo).
    scope = s.split("$")[-1] if "$" in s else s

    # Captura tokens numericos completos (evita trocear 90000 en 900/00/0).
    tokens = re.findall(r"-?\d[\d\.,]*", scope)
    if not tokens and scope is not s:
        tokens = re.findall(r"-?\d[\d\.,]*", s)
    if not tokens:
        return default

    # Escoge el token con mayor cantidad de digitos; en empate, el ultimo.
    best_idx = 0
    best_score = (-1, -1)
    for i, tok in enumerate(tokens):
        digits = len(re.sub(r"\D", "", tok))
        score = (digits, i)
        if score >= best_score:
            best_score = score
            best_idx = i

    val = _to_float(tokens[best_idx])
    return val if val != 0.0 or "0" in tokens[best_idx] else default

def extraer_porcentaje(txt: str):
    """Devuelve 19.0 si encuentra '19%' (tolerante a '19 %')."""
    if not txt:
        return None
    m = re.search(r"(\d{1,2}(?:,\d+)?)\s*%", str(txt))
    if m:
        return float(m.group(1).replace(",", "."))
    return None

def parse_iva(campo_iva: str, monto_neto: float | None = None) -> float:
    """
    Intenta monto desde '$'. Si no hay, intenta porcentaje desde 'monto_neto'.
    Ãšltimo recurso: Ãºltimo nÃºmero en el texto.
    """
    monto = extraer_monto(campo_iva, default=0.0)
    pct = extraer_porcentaje(campo_iva)

    # Caso tipico "19%": evita guardar 19 como monto IVA.
    if pct is not None and monto_neto and monto_neto > 0:
        if monto <= 100 and abs(monto - pct) < 0.0001:
            return round(monto_neto * (pct / 100.0))

    if monto > 0:
        return monto

    if pct is not None and monto_neto and monto_neto > 0:
        return round(monto_neto * (pct / 100.0))

    return extraer_monto(campo_iva, default=0.0)

# -------- Normalizaciones bÃ¡sicas --------
def _strip_accents_keep_enie_to_n(s: str) -> str:
    if not s:
        return ""
    s = s.replace("Ã‘", "\uE000").replace("Ã±", "\uE001")
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = unicodedata.normalize("NFC", s)
    return s.replace("\uE000", "N").replace("\uE001", "n")

def _collapse_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", (s or "").strip())

def _remove_ocr_artifacts(s: str) -> str:
    # saca [UNK], subpalabras ##xxx, y repeticiones raras
    s = re.sub(r"\[UNK\]", "", s)
    s = re.sub(r"\s*##\w+", "", s)
    s = re.sub(r"[â€œâ€\"â€šâ€™Â´`]+", "", s)
    return _collapse_spaces(s)

# -------- RUT --------
def parse_rut(raw: str) -> str:
    """
    Toma algo como 'R.U.T. 76.046.889-4' o 'R.U.T.: 7.468.512-9' o 'RUT:76002042-7'
    y devuelve '76046889-4' (sin puntos) o '7468512-9', asegurando no perder dÃ­gitos.
    """
    if not raw:
        return ""
    s = str(raw)
    # toma solo lo que estÃ¡ despuÃ©s del Ãºltimo ':', si existe
    if ":" in s:
        s = s.split(":")[-1]
    s = s.upper()

    # elimina todo lo que no sea dÃ­gito o K
    solo = re.sub(r"[^0-9K]", "", s)
    if len(solo) < 2:
        return ""

    cuerpo, dv = solo[:-1], solo[-1]
    # protege que el dv sea K o dÃ­gito
    if not re.fullmatch(r"[0-9K]", dv):
        return ""
    # quita ceros a la izquierda innecesarios (pero deja '0' si fuera el caso patolÃ³gico)
    cuerpo = cuerpo.lstrip("0") or "0"
    return f"{cuerpo}-{dv}"

# -------- Folio --------
def parse_folio(raw: str) -> str:
    """
    De 'N? 10796', 'NÂº 685869', 'N 673018' â†’ '10796' / '685869' / '673018'
    """
    s = str(raw or "")
    m = re.findall(r"\d+", s)
    return m[-1] if m else "0"

def infer_identity_from_pdf_filename(pdf_path: str) -> tuple[str, str, str] | None:
    """
    Intenta inferir (tipo_doc, rut, folio) desde el nombre del PDF:
    Tipo_RUT_FOLIO.pdf
    Retorna None si no logra resolver una identidad completa.
    """
    if not pdf_path:
        return None
    try:
        raw_name = os.path.basename(str(pdf_path)).split("?", 1)[0]
        stem = os.path.splitext(raw_name)[0]
        parts = [p for p in stem.split("_") if p]
        if len(parts) < 3:
            return None

        work = parts[:]
        folio = "0"
        rut = ""

        # folio: desde el final
        for i in range(len(work) - 1, -1, -1):
            f = parse_folio(work[i])
            if f and f != "0":
                folio = f
                work.pop(i)
                break

        # rut: desde el final
        for i in range(len(work) - 1, -1, -1):
            r = parse_rut(work[i])
            if r:
                rut = r
                work.pop(i)
                break

        tipo_doc = _collapse_spaces(" ".join(work)) if work else ""
        if not (tipo_doc and rut and folio and folio != "0"):
            return None
        return tipo_doc, rut, folio
    except Exception:
        return None

# -------- Fecha de emisiÃ³n --------
def parse_fecha(raw: str):
    """
    Parsea fechas de emision en formatos comunes:
    - '29 de Mayo del 2025'
    - '20/10/2025', '20-10-2025', '2025-10-20', '2025/10/20'
    - '20/10/25'
    Devuelve date o None.
    """
    if not raw:
        return None
    if isinstance(raw, date):
        return raw

    s = str(raw).strip().lower()
    if not s:
        return None

    # Normaliza acentos y deja texto limpio
    s_norm = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s_norm = re.sub(r"fecha\s*emision\s*[:\-]?\s*", "", s_norm)
    s_norm = re.sub(r"\s+", " ", s_norm).strip()

    meses = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04", "mayo": "05",
        "junio": "06", "julio": "07", "agosto": "08", "septiembre": "09", "setiembre": "09",
        "octubre": "10", "noviembre": "11", "diciembre": "12",
        "ene": "01", "feb": "02", "mar": "03", "abr": "04", "may": "05",
        "jun": "06", "jul": "07", "ago": "08", "sep": "09", "set": "09",
        "oct": "10", "nov": "11", "dic": "12",
    }

    # "11 de septiembre del 2025" o "11 de sep 2025"
    m = re.search(r"(\d{1,2})\s*de\s*([a-z]+)\s*(?:del?\s*)?(\d{2,4})", s_norm)
    if m:
        d, mes_txt, y = m.groups()
        mes = meses.get(mes_txt)
        if mes:
            y = f"20{int(y):02d}" if len(y) == 2 else y
            try:
                return datetime.strptime(f"{int(d):02d}/{mes}/{y}", "%d/%m/%Y").date()
            except Exception:
                pass

    # formatos numéricos
    candidate = s_norm.replace(".", "/").replace("-", "/")
    for fmt in ("%d/%m/%Y", "%Y/%m/%d", "%d/%m/%y", "%Y%m%d"):
        try:
            return datetime.strptime(candidate, fmt).date()
        except Exception:
            continue
    return None


def _fecha_en_rango_valido(fecha: date) -> bool:
    if not fecha:
        return False
    min_fecha = date(2000, 1, 1)
    max_fecha = date.today() + timedelta(days=7)
    return min_fecha <= fecha <= max_fecha


def _normalizar_fecha_db(value):
    if value is None:
        return None
    if isinstance(value, date):
        return value
    parsed = parse_fecha(str(value))
    return parsed

# -------- RazÃ³n social / Giro --------
def _remove_label_prefix(s: str, label_regex: str) -> str:
    # quita prefijos como "RazÃ³n Social:", "Giro:", etc.
    return re.sub(label_regex, "", s, flags=re.IGNORECASE).strip()

def clean_razon_social(raw: str) -> str:
    """
    Quita 'RazÃ³n Social:' y artefactos OCR, conserva letras, dÃ­gitos y separadores razonables.
    No pone tildes ni 'Ã±' (Ã±â†’n).
    """
    s = _collapse_spaces(str(raw or ""))
    s = _remove_label_prefix(s, r"^\s*raz[oÃ³]n\s*social\s*[:\-â€“â€”]?\s*")
    s = _remove_ocr_artifacts(s)
    # conserva letras/dÃ­gitos/espacios/&/./-/_
    s = re.sub(r"[^A-Za-z0-9\s\.\-\&_/]", " ", s)
    s = _collapse_spaces(s)
    s = _strip_accents_keep_enie_to_n(s)
    return s

def clean_giro(raw: str) -> str:
    """
    Quita 'Giro:' y variantes, limpia artefactos, tildes y Ã±â†’n.
    Evita quedarse con 'jose ##tac' o 'y se': elimina subpalabras y tokens de 1-2 letras sueltas.
    """
    s = _collapse_spaces(str(raw or ""))
    s = _remove_label_prefix(s, r"^\s*g[iÃ­]ro(?:\s+(comercial|actividad(?:\s+econ[oÃ³]mica)?))?\s*[:\-â€“â€”]?\s*")
    s = _remove_ocr_artifacts(s)
    # reemplaza caracteres raros por espacio y colapsa
    s = re.sub(r"[^A-Za-z0-9\s\.\-\&_/]", " ", s)
    # elimina tokens de 1-2 letras sueltas que suelen ser ruido ("y", "se", "de", etc.)
    tokens = [t for t in s.split() if not re.fullmatch(r"[A-Za-z]{1,2}", t)]
    s = " ".join(tokens)
    s = _collapse_spaces(s)
    s = _strip_accents_keep_enie_to_n(s)
    return s


def clean_descripcion_detalle(raw: str) -> str:
    """
    Normaliza descripcion de lineas de detalle para guardar en DB:
    - MAYUSCULAS
    - sin tildes (y ñ->N)
    - espacios compactados
    - caracteres raros eliminados
    """
    s = _collapse_spaces(str(raw or ""))
    s = _remove_ocr_artifacts(s)
    s = _strip_accents_keep_enie_to_n(s)
    s = s.upper()
    # Conserva texto util para descripcion de productos.
    s = re.sub(r"[^A-Z0-9\s\.\,\-\+\%\(\)\/\&:_]", " ", s)
    s = _collapse_spaces(s)
    return s


def _normalizar_unidad(raw: str) -> str:
    s = _collapse_spaces(str(raw or "")).upper().strip()
    if not s:
        return ""
    s = re.sub(r"[^A-Z0-9]", "", s)
    if not s:
        return ""

    aliases = {
        "KG": "KG",
        "KGS": "KG",
        "KILO": "KG",
        "KILOS": "KG",
        "GR": "GR",
        "GRM": "GR",
        "GRS": "GR",
        "GRAMO": "GR",
        "GRAMOS": "GR",
        "LT": "LT",
        "LTS": "LT",
        "LITRO": "LT",
        "LITROS": "LT",
        "L": "LT",
        "ML": "ML",
        "CC": "CC",
        "UN": "UN",
        "UND": "UN",
        "UNID": "UN",
        "UNIDAD": "UN",
        "UNIDADES": "UN",
    }
    return aliases.get(s, s if len(s) <= 8 else "")


def _unidad_desde_descripcion(descripcion_normalizada: str) -> str:
    """
    Regla de negocio: si la descripcion contiene unidad, esa unidad manda.
    Se evalua sobre descripcion ya normalizada (MAYUSCULA, sin tildes).
    """
    d = _collapse_spaces(str(descripcion_normalizada or "")).upper()
    if not d:
        return ""

    checks = [
        (r"\b(KG|KGS|KILO|KILOS)\b", "KG"),
        (r"\b(LT|LTS|LITRO|LITROS)\b", "LT"),
        (r"\b(GR|GRM|GRS|GRAMO|GRAMOS)\b", "GR"),
        (r"\bML\b", "ML"),
        (r"\bCC\b", "CC"),
        (r"\b(UN|UND|UNID|UNIDAD|UNIDADES)\b", "UN"),
    ]
    for pattern, unidad in checks:
        if re.search(pattern, d):
            return unidad
    return ""


def resolver_unidad_detalle(descripcion_normalizada: str, unidad_origen: str) -> str:
    """
    Prioridad:
    1) Unidad detectada en descripcion (manda)
    2) Unidad de origen entregada por extractor
    """
    from_desc = _unidad_desde_descripcion(descripcion_normalizada)
    if from_desc:
        return from_desc
    return _normalizar_unidad(unidad_origen)

# Asegura columnas nuevas
# compatibilidad con DBs antiguas (si ya existe una DB)
add_column_if_missing(engine, "documentos", "ADD COLUMN fecha_emision DATE")
add_column_if_missing(engine, "documentos", "ADD COLUMN IVA FLOAT")
add_column_if_missing(engine, "documentos", "ADD COLUMN monto_excento FLOAT")
add_column_if_missing(engine, "documentos", "ADD COLUMN impuesto_adicional FLOAT")
add_column_if_missing(engine, "documentos", "ADD COLUMN monto_total FLOAT")
add_column_if_missing(engine, "documentos", "ADD COLUMN referencia TEXT")
add_column_if_missing(engine, "documentos", "ADD COLUMN DTE_referencia TEXT")
add_column_if_missing(engine, "documentos", "ADD COLUMN giro TEXT")
add_column_if_missing(engine, "documentos", "ADD COLUMN detalle_link TEXT")

# âœ… nuevas columnas del detalle
add_column_if_missing(engine, "detalle", "ADD COLUMN codigo TEXT")
add_column_if_missing(engine, "detalle", "ADD COLUMN unidad TEXT")
add_column_if_missing(engine, "detalle", "ADD COLUMN categoria TEXT")
add_column_if_missing(engine, "detalle", "ADD COLUMN subcategoria TEXT")
add_column_if_missing(engine, "detalle", "ADD COLUMN tipo_gasto TEXT")

# Inicializa cola/triggers de sincronizacion con Supabase.
try:
    initialize_supabase_sync(db_path=DB_PATH, log=lambda _m: None)
except Exception:
    pass
# ============================================================
# ðŸ”— REFERENCIAS Y SALDOS
# ============================================================

def _infer_tipo_from_text(ref_text: str) -> str:
    t = ref_text.lower()
    if "credito" in t:
        return "nota de crÃ©dito"
    if "debito" in t:
        return "nota de dÃ©bito"
    if "factura" in t:
        return "factura"
    return "factura"

def parse_id_doc_referencia(ref_text: str) -> str | None:
    try:
        t = ref_text or ""
        folio_m = re.search(r"folio\s*[:#]?\s*(\d+)", t, re.IGNORECASE)
        rut_m = re.search(r"(\d{1,3}(?:\.\d{3})+-[\dkK])", t)
        tipo = _infer_tipo_from_text(t)
        if folio_m and rut_m:
            folio_ref = folio_m.group(1)
            rut_ref = parse_rut(rut_m.group(1))
            return build_id_doc(tipo, rut_ref, folio_ref)
    except Exception:
        pass
    return None

# ============================================================
# ðŸ§© OCR UTILIDADES
# ============================================================


# ============================================================
# ðŸ’¾ GUARDAR EN BD
# ============================================================

def get_all_doc_ids(engine) -> set:
    """Obtiene un set con todos los id_doc de la base de datos."""
    try:
        with engine.connect() as conn:
            q = select(documentos.c.id_doc)
            result = conn.execute(q).fetchall()
            return {row[0] for row in result}
    except Exception as e:
        print(f"No se pudieron obtener los IDs de documentos existentes: {e}")
        return set()

def existe_en_bd(conn, tipo_doc, rut_emisor, folio):
    doc_id = build_id_doc(tipo_doc, rut_emisor, folio)
    q = select(documentos.c.id_doc).where(documentos.c.id_doc == doc_id)
    return conn.execute(q).fetchone() is not None

def _es_nota_credito(tipo_doc: str = "", doc_id: str = "") -> bool:
    def _norm(s: str) -> str:
        s = _strip_accents_keep_enie_to_n(str(s or ""))
        s = s.lower().replace("_", " ")
        return _collapse_spaces(s)

    for cand in (_norm(tipo_doc), _norm(doc_id)):
        if not cand:
            continue
        if cand == "61" or cand.startswith("61 "):
            return True
        if "nota" in cand and "credito" in cand:
            return True
    return False

def _forzar_montos_negativos_nota_credito(conn, doc_id: str):
    try:
        conn.exec_driver_sql(
            """
            UPDATE detalle
               SET precio_unitario = -ABS(COALESCE(precio_unitario, 0)),
                   monto_item      = -ABS(COALESCE(monto_item, 0))
             WHERE id_doc = ?
            """,
            (doc_id,),
        )
    except Exception as e:
        print(f"No se pudo normalizar montos negativos para NC ({doc_id}): {e}")

def _insertar_items_detalle(conn, doc_id: str, items: list[dict], tipo_doc: str = ""):
    def _collapse_spaces(s: str) -> str:
        return " ".join(s.split())
    def _f(x):
        """Convierte montos en formato chileno a float."""
        if not x or x == "null" or x is None:
            return 0.0
        if isinstance(x, (int, float)):
            return float(x)
        try:
            # Siempre usar _to_float para manejar formato chileno correctamente
            return _to_float(str(x))
        except Exception:
            return extraer_monto(str(x), default=0.0)

    is_nota_credito = _es_nota_credito(tipo_doc=tipo_doc, doc_id=doc_id)

    for idx, it in enumerate(items, start=1):
        codigo_raw = it.get("codigo", "")
        codigo_txt = str(codigo_raw).strip() if codigo_raw is not None else ""
        unidad_raw = it.get("unidad", "")
        desc_txt = clean_descripcion_detalle(it.get("detalle", it.get("descripcion", "")))
        unidad_txt = resolver_unidad_detalle(desc_txt, str(unidad_raw).strip() if unidad_raw is not None else "")

        precio_unitario_val = _f(it.get("precio_unitario", 0))
        monto_item_val = _f(it.get("monto_item", 0))
        if is_nota_credito:
            precio_unitario_val = -abs(precio_unitario_val)
            monto_item_val = -abs(monto_item_val)

        row = {
            "id_det": f"{doc_id}:{idx}",
            "id_doc": doc_id,
            "linea": idx,
            "codigo": codigo_txt,
            "descripcion": desc_txt,
            "unidad": unidad_txt,
            "cantidad": (it.get("cantidad", 0)),
            "precio_unitario": precio_unitario_val,
            "impuesto_adicional": (it.get("impuesto_adicional", 0)),
            "Descuento": (it.get("descuento", 0)),
            "monto_item": monto_item_val,
        }
        try:
            conn.execute(detalle.insert().values(**row))
        except Exception as e:
            print(f"No se pudo insertar linea {idx} en detalle: {e}")

def _detalles_count(conn, doc_id: str) -> int:
    try:
        q = select(detalle.c.id_det).where(detalle.c.id_doc == doc_id)
        return len(conn.execute(q).fetchall())
    except Exception:
        return 0


def _extraer_referencia_ids(datos_raw: dict) -> tuple[str, str]:
    ref_text = ""
    for k, v in (datos_raw or {}).items():
        if isinstance(k, str) and "referen" in k.lower():
            ref_text = str(v)
            break
    ref_id = parse_id_doc_referencia(ref_text) if ref_text else None
    if ref_id:
        return ref_id, ref_id
    return "", ""


def _actualizar_cabecera_documento(conn, doc_id: str, doc_data: dict):
    # Correccion controlada: solo campos de cabecera que suelen venir con OCR errado.
    campos_permitidos = (
        "fecha_emision",
        "IVA",
        "impuesto_adicional",
        "monto_total",
    )
    payload = {}
    for k in campos_permitidos:
        if k not in (doc_data or {}):
            continue
        v = doc_data.get(k)
        if k == "fecha_emision" and v is None:
            continue
        payload[k] = v
    if not payload:
        return
    conn.execute(
        documentos.update().where(documentos.c.id_doc == doc_id).values(**payload)
    )


def _obtener_fecha_emision_documento(conn, doc_id: str):
    try:
        row = conn.execute(
            select(documentos.c.fecha_emision).where(documentos.c.id_doc == doc_id)
        ).fetchone()
        if not row:
            return None
        return _normalizar_fecha_db(row[0])
    except Exception:
        return None


def _verificar_fecha_guardada(conn, doc_id: str, fecha_esperada):
    if fecha_esperada is None:
        return
    try:
        actual = _obtener_fecha_emision_documento(conn, doc_id)
        if actual != fecha_esperada:
            print(
                "[WARN] Fecha emitida no coincide tras guardar: "
                f"id_doc={doc_id} esperada={fecha_esperada} guardada={actual}"
            )
    except Exception:
        pass


def _to_monto_float(value) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).strip()
    if not s or s.lower() == "null":
        return 0.0
    try:
        return _to_float(s)
    except Exception:
        return extraer_monto(s, default=0.0)


def _actualizar_descuento_e_impuesto_detalle(conn, doc_id: str, items: list[dict]) -> int:
    if not items:
        return 0

    updated = 0
    for idx, it in enumerate(items, start=1):
        descuento_val = _to_monto_float(it.get("descuento", 0))
        imp_adic_val = _to_monto_float(it.get("impuesto_adicional", 0))
        try:
            res = conn.execute(
                detalle.update()
                .where(detalle.c.id_doc == doc_id)
                .where(detalle.c.linea == idx)
                .values(
                    Descuento=descuento_val,
                    impuesto_adicional=imp_adic_val,
                )
            )
            rowcount = int(getattr(res, "rowcount", 0) or 0)
            if rowcount > 0:
                updated += rowcount
        except Exception as e:
            print(f"No se pudo actualizar descuento/impuesto detalle ({doc_id}:{idx}): {e}")

    return updated


def alinear_fecha_emision_detalle_con_documentos(apply_fix: bool = True) -> dict:
    """
    Verifica/corrige que detalle.fecha_emision coincida con documentos.fecha_emision por id_doc.
    """
    try:
        with engine.begin() as conn:
            cols_det = [r[1] for r in conn.exec_driver_sql("PRAGMA table_info(detalle);").fetchall()]
            if "fecha_emision" not in cols_det:
                return {
                    "ok": False,
                    "error": "detalle_sin_columna_fecha_emision",
                    "total_detalle": 0,
                    "with_documento": 0,
                    "missing_documento": 0,
                    "mismatches_before": 0,
                    "updated": 0,
                    "mismatches_after": 0,
                }

            total_detalle = int(conn.exec_driver_sql("SELECT COUNT(*) FROM detalle;").scalar() or 0)
            with_documento = int(
                conn.exec_driver_sql(
                    """
                    SELECT COUNT(*)
                    FROM detalle d
                    JOIN documentos doc ON doc.id_doc = d.id_doc
                    """
                ).scalar()
                or 0
            )
            missing_documento = max(0, total_detalle - with_documento)

            mismatch_sql = """
                SELECT COUNT(*)
                FROM detalle d
                JOIN documentos doc ON doc.id_doc = d.id_doc
                WHERE COALESCE(substr(trim(d.fecha_emision), 1, 10), '')
                      <> COALESCE(substr(trim(CAST(doc.fecha_emision AS TEXT)), 1, 10), '')
            """
            mismatches_before = int(conn.exec_driver_sql(mismatch_sql).scalar() or 0)

            updated = 0
            if apply_fix and mismatches_before > 0:
                upd_res = conn.exec_driver_sql(
                    """
                    UPDATE detalle
                       SET fecha_emision = (
                           SELECT substr(trim(CAST(doc.fecha_emision AS TEXT)), 1, 10)
                           FROM documentos doc
                           WHERE doc.id_doc = detalle.id_doc
                       )
                     WHERE EXISTS (
                           SELECT 1
                           FROM documentos doc
                           WHERE doc.id_doc = detalle.id_doc
                             AND COALESCE(substr(trim(detalle.fecha_emision), 1, 10), '')
                                 <> COALESCE(substr(trim(CAST(doc.fecha_emision AS TEXT)), 1, 10), '')
                       )
                    """
                )
                rowcount = int(getattr(upd_res, "rowcount", 0) or 0)
                updated = rowcount if rowcount >= 0 else mismatches_before

            mismatches_after = int(conn.exec_driver_sql(mismatch_sql).scalar() or 0)
            if mismatches_after < mismatches_before and updated == 0:
                updated = mismatches_before - mismatches_after

            return {
                "ok": True,
                "total_detalle": total_detalle,
                "with_documento": with_documento,
                "missing_documento": missing_documento,
                "mismatches_before": mismatches_before,
                "updated": updated,
                "mismatches_after": mismatches_after,
            }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "total_detalle": 0,
            "with_documento": 0,
            "missing_documento": 0,
            "mismatches_before": 0,
            "updated": 0,
            "mismatches_after": 0,
        }


def guardar_en_bd(datos_raw, ruta_pdf):
    """Guarda una factura procesada en la base de datos."""
    only_numeric_fix = bool(datos_raw.get("__only_numeric_fix__", False))

    # Etiquetas OCR originales
    tipo_doc_raw   = datos_raw.get("Tipo Documento", "Factura ElectrÃ³nica")
    rut_raw        = datos_raw.get("Emisor", "")
    folio_raw      = datos_raw.get("Numero de Folio", "0")
    fecha_raw      = datos_raw.get("Fecha Emision", "")
    rs_raw         = datos_raw.get("Razon Social", "")
    giro_raw       = datos_raw.get("Giro", "")
    monto_neto_raw = datos_raw.get("Monto Neto", "0")
    iva_raw        = datos_raw.get("IVA", "")
    monto_exento_raw = datos_raw.get("Monto Exento", datos_raw.get("monto_exento", "0"))
    imp_adic_raw   = datos_raw.get("Impuesto Adicional", datos_raw.get("impuesto_adicional", "0"))
    total_raw      = datos_raw.get("Total", "")

    # Limpiezas/parseo robusto (OCR)
    tipo_doc_ocr = _collapse_spaces(str(tipo_doc_raw))
    rut_ocr      = parse_rut(rut_raw)                # ejemplo: '76046889-4'
    folio_ocr    = parse_folio(folio_raw)            # ejemplo: '685869'

    # Identidad final (tipo/rut/folio):
    # prioriza nombre de archivo cuando está disponible, para evitar cruces por OCR.
    tipo_doc = tipo_doc_ocr
    rut = rut_ocr
    folio = folio_ocr
    inferred = infer_identity_from_pdf_filename(ruta_pdf)
    if inferred:
        tipo_fn, rut_fn, folio_fn = inferred
        if (tipo_doc != tipo_fn) or (rut != rut_fn) or (folio != folio_fn):
            print(
                "Identidad OCR inconsistente con nombre PDF. "
                f"Usando filename -> tipo='{tipo_fn}', rut='{rut_fn}', folio='{folio_fn}' "
                f"(OCR: tipo='{tipo_doc}', rut='{rut}', folio='{folio}')"
            )
        tipo_doc, rut, folio = tipo_fn, rut_fn, folio_fn

    fecha_parseada = parse_fecha(fecha_raw)
    if fecha_parseada is not None and not _fecha_en_rango_valido(fecha_parseada):
        print(
            "[WARN] Fecha de emision fuera de rango, se ignora: "
            f"id_doc={build_id_doc(tipo_doc, rut, folio)} raw='{fecha_raw}' parsed={fecha_parseada}"
        )
        fecha_parseada = None

    razon    = clean_razon_social(rs_raw)
    giro     = clean_giro(giro_raw)

    # IVA / montos
    monto_neto = extraer_monto(monto_neto_raw)
    iva_val    = parse_iva(iva_raw, monto_neto=monto_neto)
    monto_exento_val = extraer_monto(monto_exento_raw, default=0.0)
    impuesto_adicional_val = extraer_monto(imp_adic_raw, default=0.0)
    total_val  = extraer_monto(total_raw) or monto_neto  # si no hay total, al menos guarda neto

    # ID determinÃ­stico
    doc_id = build_id_doc(tipo_doc, rut, folio)  # usa rut sin puntos (ya lo hace build_id_doc)
    referencia_val, dte_referencia_val = _extraer_referencia_ids(datos_raw)

    doc_data = {
        "id_doc": doc_id,
        "tipo_doc": tipo_doc,
        "folio": folio,
        "rut_emisor": rut,             # SOLO 'XXXXXXXX-D'
        "razon_social": razon,         # limpio, sin tildes/Ã±
        "giro": giro,                  # limpio, sin tildes/Ã±
        "IVA": iva_val,
        "monto_excento": monto_exento_val,
        "impuesto_adicional": impuesto_adicional_val,
        "monto_total": total_val,
        "referencia": referencia_val,
        "DTE_referencia": dte_referencia_val,
        "ruta_pdf": ruta_pdf,
    }

    with engine.begin() as conn:
        ya_existe = existe_en_bd(conn, tipo_doc, rut, folio)
        fecha_objetivo = fecha_parseada

        if ya_existe:
            doc_id_dup = doc_id
            fecha_actual = _obtener_fecha_emision_documento(conn, doc_id_dup)
            if fecha_objetivo is None:
                fecha_objetivo = fecha_actual
                if str(fecha_raw or "").strip():
                    print(
                        "[WARN] Fecha de emision no parseable; se conserva fecha existente: "
                        f"id_doc={doc_id_dup} raw='{fecha_raw}' actual={fecha_actual}"
                    )

            if fecha_objetivo is not None:
                doc_data["fecha_emision"] = fecha_objetivo
            else:
                doc_data.pop("fecha_emision", None)

            # Siempre actualiza campos selectivos de cabecera al releer PDFs.
            try:
                _actualizar_cabecera_documento(conn, doc_id_dup, doc_data)
                _verificar_fecha_guardada(conn, doc_id_dup, fecha_objetivo)
            except Exception as e:
                print(f"No se pudo actualizar cabecera existente ({doc_id_dup}): {e}")

            det_count = _detalles_count(conn, doc_id_dup)
            items_dup = datos_raw.get("__items_detalle__") or []
            if det_count > 0:
                det_actualizados = 0
                if only_numeric_fix and items_dup:
                    det_actualizados = _actualizar_descuento_e_impuesto_detalle(conn, doc_id_dup, items_dup)

                if _es_nota_credito(tipo_doc=tipo_doc, doc_id=doc_id_dup):
                    _forzar_montos_negativos_nota_credito(conn, doc_id_dup)
                if only_numeric_fix:
                    print(
                        f"Actualizado en BD: {doc_id_dup} "
                        f"(campos selectivos cabecera + detalle desc/imp lineas={det_actualizados})"
                    )
                else:
                    print(f"Actualizado en BD: {doc_id_dup} (cabecera corregida, detalle conservado)")
                return
            else:
                if only_numeric_fix:
                    print(f"Actualizado en BD: {doc_id_dup} (campos selectivos cabecera; sin detalle para corregir)")
                    return

                # Documento existe pero sin detalle: usar items de ai_reader si vienen
                if items_dup:
                    try:
                        _insertar_items_detalle(conn, doc_id_dup, items_dup, tipo_doc=tipo_doc)
                        print(f"Actualizado en BD: {doc_id_dup} (cabecera + detalle agregado)")
                    except Exception as e:
                        print(f"No se pudo insertar detalle (duplicado): {e}")
                else:
                    print(f"Actualizado en BD: {doc_id_dup} (solo cabecera)")
                return

        if fecha_objetivo is not None:
            doc_data["fecha_emision"] = fecha_objetivo
        else:
            doc_data.pop("fecha_emision", None)
            if str(fecha_raw or "").strip():
                print(
                    "[WARN] Fecha de emision no parseable en insercion; se guarda NULL: "
                    f"id_doc={doc_id} raw='{fecha_raw}'"
                )

        try:
            conn.execute(documentos.insert().values(**doc_data))
            # Inserta items de detalle si vienen desde ai_reader
            items = datos_raw.get("__items_detalle__") or []
            if items:
                _insertar_items_detalle(conn, doc_id, items, tipo_doc=tipo_doc)
            _verificar_fecha_guardada(conn, doc_id, fecha_objetivo)
            print(f"Subida a BD: {doc_id}")
        except IntegrityError:
            # Condicion de carrera: si otro hilo lo insertó, actualiza cabecera de todas formas.
            try:
                _actualizar_cabecera_documento(conn, doc_id, doc_data)
                _verificar_fecha_guardada(conn, doc_id, fecha_objetivo)
                print(f"Actualizado en BD: {doc_id} (post-integrity)")
            except Exception:
                print(f"Leido: {doc_id} (ya existi­a, sin subida)")


# ============================================================
# PIPELINE PRINCIPAL
# ============================================================

# (procesar_factura eliminada — lectura via ai_reader.py)

def _insertar_minimo_desde_nombre(ruta_pdf_abs: str, tipo: str, rut: str, folio: str):
    try:
        doc_id = build_id_doc(tipo, rut, folio)
        with engine.begin() as conn:
            if existe_en_bd(conn, tipo, rut, folio):
                return
            row = {
                "id_doc": doc_id,
                "tipo_doc": tipo,
                "folio": folio,
                "rut_emisor": rut,
                "razon_social": "",
                "giro": "",
                "fecha_emision": datetime.now().date(),
                "IVA": 0.0,
                "monto_excento": 0.0,
                "impuesto_adicional": 0.0,
                "monto_total": 0.0,
                "referencia": "",
                "DTE_referencia": "",
                "ruta_pdf": ruta_pdf_abs if ruta_pdf_abs else "",
                "detalle_link": "",
            }
            conn.execute(documentos.insert().values(**row))
            print(f"Subida mi­nima a BD: {doc_id}")
    except Exception as e:
        print(f"No se pudo insertar registro maximo: {e}")

def procesar_todos_los_pdfs(progress_cb=None):
    existing_ids = get_all_doc_ids(engine)
    files = [f for f in os.listdir(RUTA_PDF) if f.lower().endswith(".pdf")]
    to_process = []
    for file in files:
        base = os.path.splitext(os.path.basename(file))[0]
        parts = base.split("_")
        if len(parts) >= 3:
            tipo_doc_guess = _collapse_spaces(parts[0])
            rut_guess = parse_rut(parts[1])
            folio_guess = parse_folio(parts[2])
            doc_id_guess = build_id_doc(tipo_doc_guess, rut_guess, folio_guess)
            if doc_id_guess in existing_ids:
                continue
        to_process.append(file)
    total = len(to_process)
    done = 0
    for file in files:
        if not file.lower().endswith(".pdf"):
            continue
        ruta_pdf = os.path.join(RUTA_PDF, file)
        ruta_pdf_abs = os.path.abspath(ruta_pdf)
        base = os.path.splitext(os.path.basename(file))[0]
        parts = base.split("_")
        if len(parts) >= 3:
            tipo_doc_guess = _collapse_spaces(parts[0])
            rut_guess = parse_rut(parts[1])
            folio_guess = parse_folio(parts[2])
            doc_id_guess = build_id_doc(tipo_doc_guess, rut_guess, folio_guess)
            if doc_id_guess in existing_ids:
                print(f"Omitido (ya en BD): {doc_id_guess}")
                continue
        print(f"Lei­do: {file}")
        try:
            if len(parts) >= 3:
                _insertar_minimo_desde_nombre(ruta_pdf_abs, tipo_doc_guess, rut_guess, folio_guess)
            else:
                print("Saltado: no se pudo inferir id desde nombre")
            done += 1
            if progress_cb:
                try:
                    progress_cb(done, total if total > 0 else 1)
                except Exception:
                    pass
        except Exception as e:
            print(f"Error procesando {file}: {e}")

    # Fin del proceso

if __name__ == "__main__":
    procesar_todos_los_pdfs()

