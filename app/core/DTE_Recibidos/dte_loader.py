# Script Control:
# - Role: SQLite schema, OCR fallback, and persistence utilities.
# - Track file: docs/SCRIPT_CONTROL.md
import os
import re
import warnings
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, Date, MetaData, ForeignKey, TIMESTAMP, text, select, event
from sqlalchemy.engine import Engine
try:
    import easyocr
except Exception:
    easyocr = None
try:
    import cv2
except Exception:
    cv2 = None
try:
    from ultralytics import YOLO
except Exception:
    YOLO = None
try:
    import fitz
except Exception:
    fitz = None
try:
    from PIL import Image
except Exception:
    Image = None
import io
import json
import unicodedata
import shutil
from typing import List
try:
    from transformers import AutoTokenizer, AutoModelForMaskedLM, pipeline
except Exception:
    AutoTokenizer = None
    AutoModelForMaskedLM = None
    pipeline = None
# Stubs de IA (se sobreescriben solo si AI estÃ¡ habilitada y el mÃ³dulo existe)
def analizar_detalle_desde_imagen(*args, **kwargs):
    return {"ok": False, "error": "ai_disabled"}
def categorizar_items_por_ai(*args, **kwargs):
    return {"ok": False, "error": "ai_disabled"}
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy.exc import IntegrityError

# === BASE PATH (raÃ­z del proyecto TERRALIX) ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent # Read_Load_PDF â†’ Modulo â†’ core â†’ app â†’ TERRALIX
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# === FUNCIONES DE RUTA COMPATIBLES CON PyInstaller ===
def resource_path(relative_path: str) -> str:
    """Obtiene ruta absoluta compatible con entorno dev y PyInstaller."""
    try:
        base_path = sys._MEIPASS  # tipo: ignore
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

# === RUTAS CLAVE ===
ENV_PATH = resource_path(str(BASE_DIR / "data/config.env"))
load_dotenv(ENV_PATH)

warnings.filterwarnings("ignore", category=UserWarning)

DEFAULT_DB_PATH = resource_path(str(BASE_DIR / "data/dte_recibidos.db"))
DEFAULT_PDF_DIR = resource_path(str(BASE_DIR / "data/pdfs"))

DB_PATH = os.getenv("DB_PATH_DTE_RECIBIDOS") or DEFAULT_DB_PATH
RUTA_PDF = os.getenv("RUTA_PDF_DTE_RECIBIDOS") or DEFAULT_PDF_DIR

# Asegura directorios para DB y PDF
try:
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
except Exception:
    pass
os.makedirs(RUTA_PDF, exist_ok=True)
# Logs simplificados: evitar prints innecesarios
AI_DETALLE_ENABLED = os.getenv("AI_DETALLE_ENABLED", "true").lower() == "true"
AI_DETALLE_DEBUG = os.getenv("AI_DETALLE_DEBUG", "false").lower() == "true"
_AI_DETALLE_DISABLED_RUNTIME = False
if AI_DETALLE_ENABLED:
    try:
        # Import tardÃ­o para evitar fallo si no estÃ¡ instalado openai
        try:
            from .ai_openai import analizar_detalle_desde_imagen as _an, categorizar_items_por_ai as _cat
        except Exception:
            from ai_openai import analizar_detalle_desde_imagen as _an, categorizar_items_por_ai as _cat
        analizar_detalle_desde_imagen = _an  # type: ignore
        categorizar_items_por_ai = _cat      # type: ignore
    except Exception as _e:
        print(f"âš ï¸ IA deshabilitada (no disponible): {_e}")
        _AI_DETALLE_DISABLED_RUNTIME = True

# === MODELOS ===
MODEL_PATH = resource_path(str(BASE_DIR / "app/core/DTE_Recibidos/YOLOv11Model.pt"))

if YOLO:
    try:
        model = YOLO(MODEL_PATH)
    except Exception:
        model = None
else:
    model = None

# --- EasyOCR: fallback a CPU si no hay GPU ---
if easyocr:
    try:
        reader = easyocr.Reader(['es', 'en'], gpu=True)
    except Exception:
        try:
            reader = easyocr.Reader(['es', 'en'], gpu=False)
        except Exception:
            reader = None
else:
    reader = None

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
    print(f"âŒ No se pudo inicializar la base de datos en {DB_PATH}: {e}")
    # intento de fallback a la ruta por defecto
    DB_PATH = DEFAULT_DB_PATH
    print(f"âž¡ï¸ Intentando fallback a {DB_PATH}")
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
    print(f"âš ï¸ No se pudo establecer WAL/synchronous en SQLite: {e}")

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
    print("âœ… Tablas creadas/verificadas: documentos, detalle")
except Exception as e:
    print(f"âŒ Error creando/verificando tablas: {e}")

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
    Convierte un string con formato chileno a float.
    En Chile: los montos son enteros (sin decimales), el punto es separador de miles.
    Ej: '351.500' -> 351500.0, '1.234' -> 1234.0
    """
    if not numstr:
        return 0.0
    s = str(numstr).strip()
    s = s.replace(" ", "")
    
    # En formato chileno, el punto es separador de miles, no decimal
    # Si hay coma, la ignoramos ya que en CLP no hay decimales
    if "," in s:
        s = s.split(",")[0]  # Tomar solo la parte antes de la coma
    
    # Remover separadores de miles (puntos)
    s = s.replace(".", "")
    
    try:
        return float(s)
    except:
        return 0.0

def extraer_monto(txt: str, default: float = 0.0) -> float:
    """
    Extrae el monto priorizando el nÃºmero DESPUÃ‰S del ÃšLTIMO '$'.
    Si no hay '$', toma el Ãºltimo nÃºmero plausible en el texto.
    Acepta formatos con miles y decimales tipo '351.500' o '1,23'.
    """
    if not txt:
        return default
    s = " ".join(str(txt).split())

    # 1) despuÃ©s del Ãºltimo '$'
    if "$" in s:
        tail = s.split("$")[-1]
        m = re.search(r"(\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?)", tail)
        if m:
            return _to_float(m.group(1))

    # 2) fallback: Ãºltimo nÃºmero del texto
    nums = re.findall(r"\d{1,3}(?:[.\s]\d{3})*(?:,\d+)?|\d+(?:,\d+)?", s)
    if nums:
        return _to_float(nums[-1])

    return default

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
    # monto explÃ­cito
    monto = extraer_monto(campo_iva, default=0.0)
    if monto > 0:
        return monto

    # porcentaje â†’ calcula desde neto
    pct = extraer_porcentaje(campo_iva)
    if pct is not None and monto_neto and monto_neto > 0:
        return round(monto_neto * (pct / 100.0))

    # fallback: Ãºltimo nÃºmero
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
    Soporta: '29 de Mayo del 2025', '27 de Marzo del 2023', '20/10/2025', '2025-10-20', '20-10-2025'
    Devuelve date o None.
    """
    if not raw:
        return None
    s = str(raw).strip().lower()
    # normaliza acentos
    s_norm = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

    meses = {
        "enero":"01","febrero":"02","marzo":"03","abril":"04","mayo":"05",
        "junio":"06","julio":"07","agosto":"08","septiembre":"09","setiembre":"09",
        "octubre":"10","noviembre":"11","diciembre":"12"
    }

    # "11 de septiembre del 2025"
    m = re.search(r"(\d{1,2})\s*de\s*([a-z]+)\s*(?:del\s*)?(\d{4})", s_norm)
    if m:
        d, mes_txt, y = m.groups()
        mes = meses.get(mes_txt, "01")
        try:
            return datetime.strptime(f"{int(d):02d}/{mes}/{y}", "%d/%m/%Y").date()
        except:
            pass

    # formatos numÃ©ricos
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            continue
    return None

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

# Asegura columnas nuevas
# compatibilidad con DBs antiguas (si ya existe una DB)
add_column_if_missing(engine, "documentos", "ADD COLUMN referencia TEXT")
add_column_if_missing(engine, "documentos", "ADD COLUMN DTE_referencia TEXT")
add_column_if_missing(engine, "documentos", "ADD COLUMN giro TEXT")
add_column_if_missing(engine, "documentos", "ADD COLUMN detalle_link TEXT")

# âœ… nuevas columnas del detalle
add_column_if_missing(engine, "detalle", "ADD COLUMN categoria TEXT")
add_column_if_missing(engine, "detalle", "ADD COLUMN subcategoria TEXT")
add_column_if_missing(engine, "detalle", "ADD COLUMN tipo_gasto TEXT")
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

def pdf_a_imagen(pdf_path, output_dir=resource_path(str(BASE_DIR / "data/temp_images"))):
    if not fitz or not Image:
        return None
    os.makedirs(output_dir, exist_ok=True)
    doc = fitz.open(pdf_path)
    page = doc[0]
    pix = page.get_pixmap(dpi=200)
    img_path = os.path.join(output_dir, os.path.splitext(os.path.basename(pdf_path))[0] + ".jpg")
    img = Image.open(io.BytesIO(pix.tobytes("jpg")))
    img.save(img_path, "JPEG")
    doc.close()
    return img_path

def mejorar_imagen(img):
    if not cv2:
        return img
    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    gray = cv2.convertScaleAbs(gray, alpha=1.1, beta=10)
    gray = cv2.GaussianBlur(gray, (3, 3), 0)
    _, thr = cv2.threshold(gray, 150, 255, cv2.THRESH_BINARY)
    return cv2.cvtColor(thr, cv2.COLOR_GRAY2BGR)

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
        print(f"âš ï¸ No se pudieron obtener los IDs de documentos existentes: {e}")
        return set()

def existe_en_bd(conn, tipo_doc, rut_emisor, folio):
    doc_id = build_id_doc(tipo_doc, rut_emisor, folio)
    q = select(documentos.c.id_doc).where(documentos.c.id_doc == doc_id)
    return conn.execute(q).fetchone() is not None

def _insertar_items_detalle(conn, doc_id: str, items: list[dict]):
    def _collapse_spaces(s: str) -> str:
        return " ".join(s.split())
    def _f(x):
        """Convierte montos en formato chileno a float."""
        if not x or x == "null" or x is None:
            return 0.0
        try:
            # Siempre usar _to_float para manejar formato chileno correctamente
            return _to_float(str(x))
        except Exception:
            return extraer_monto(str(x), default=0.0)

    for idx, it in enumerate(items, start=1):
        row = {
            "id_det": f"{doc_id}:{idx}",
            "id_doc": doc_id,
            "linea": idx,
            "codigo": str(it.get("codigo", "")),
            "descripcion": _collapse_spaces(str(it.get("detalle", it.get("descripcion", "")))),
            "cantidad": (it.get("cantidad", 0)),
            "precio_unitario": (it.get("precio_unitario", 0)),
            "impuesto_adicional": (it.get("impuesto_adicional", 0)),
            "Descuento": (it.get("descuento", 0)),
            "monto_item": (it.get("monto_item", 0))
        }
        if AI_DETALLE_DEBUG:
            try:
                print(
                    f"ðŸ§ª InserciÃ³n detalle: id_det={doc_id}:{idx} | "
                    f"codigo='{row['codigo']}' | descripcion='{row['descripcion']}' | "
                    f"cantidad={row['cantidad']} | precio_unitario={row['precio_unitario']} | "
                    f"impuesto_adicional={row['impuesto_adicional']} | descuento={row['Descuento']} | "
                    f"monto_item={row['monto_item']}"
                    f"categoria={str(it.get("categoria", "")).strip()}"
                    f"subcategoria: {str(it.get("subcategoria", "")).strip()}"
                    f"tipo_gasto: {str(it.get("tipo_gasto", "")).strip()}"
                )
            except Exception:
                print("(no se pudo imprimir la lÃ­nea antes de insertar)")
        try:
            conn.execute(detalle.insert().values(**row))
        except Exception as e:
            print(f"âš ï¸ No se pudo insertar lÃ­nea {idx} en detalle: {e}")

def _detalles_count(conn, doc_id: str) -> int:
    try:
        q = select(detalle.c.id_det).where(detalle.c.id_doc == doc_id)
        return len(conn.execute(q).fetchall())
    except Exception:
        return 0

def guardar_en_bd(datos_raw, ruta_pdf):
    """Guarda una factura procesada en la base de datos."""
    # Etiquetas OCR originales
    tipo_doc_raw   = datos_raw.get("Tipo Documento", "Factura ElectrÃ³nica")
    rut_raw        = datos_raw.get("Emisor", "")
    folio_raw      = datos_raw.get("Numero de Folio", "0")
    fecha_raw      = datos_raw.get("Fecha Emision", "")
    rs_raw         = datos_raw.get("Razon Social", "")
    giro_raw       = datos_raw.get("Giro", "")
    monto_neto_raw = datos_raw.get("Monto Neto", "0")
    iva_raw        = datos_raw.get("IVA", "")
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
                "âš ï¸ Identidad OCR inconsistente con nombre PDF. "
                f"Usando filename -> tipo='{tipo_fn}', rut='{rut_fn}', folio='{folio_fn}' "
                f"(OCR: tipo='{tipo_doc}', rut='{rut}', folio='{folio}')"
            )
        tipo_doc, rut, folio = tipo_fn, rut_fn, folio_fn

    fecha    = parse_fecha(fecha_raw) or datetime.now().date()  # si no se pudo, hoy (o pon None si prefieres)
    razon    = clean_razon_social(rs_raw)
    giro     = clean_giro(giro_raw)

    # IVA / montos
    monto_neto = extraer_monto(monto_neto_raw)
    iva_val    = parse_iva(iva_raw, monto_neto=monto_neto)
    total_val  = extraer_monto(total_raw) or monto_neto  # si no hay total, al menos guarda neto

    # ID determinÃ­stico
    doc_id = build_id_doc(tipo_doc, rut, folio)  # usa rut sin puntos (ya lo hace build_id_doc)

    # Pre-analiza AI fuera de transacciÃ³n para evitar mantener bloqueos prolongados
    items_ai_pre = []
    if AI_DETALLE_ENABLED and not _AI_DETALLE_DISABLED_RUNTIME:
        crop_path_pre = datos_raw.get("__detalle_crop_path__")
        if crop_path_pre and os.path.exists(crop_path_pre):
            try:
                ai_res_pre = analizar_detalle_desde_imagen(crop_path_pre)
                if ai_res_pre.get("ok"):
                    items_ai_pre = ai_res_pre.get("items", [])
                    if len(items_ai_pre) == 0:
                        src = ai_res_pre.get("source", "openai")
                        err = ai_res_pre.get("error")
                        reason = ai_res_pre.get("reason")
                        keys = ai_res_pre.get("content_keys")
                        print(f"âš ï¸ AI Detalle (pre) devolviÃ³ 0 Ã­tems (source={src}, error={err}, reason={reason}, keys={keys})")
                    elif AI_DETALLE_DEBUG:
                        print(f"ðŸ§ª AI Detalle (pre): {len(items_ai_pre)} Ã­tems detectados")
                else:
                    print(f"âš ï¸ AI detalle fallo (pre): {ai_res_pre}")
            except Exception as e:
                print(f"âš ï¸ No se pudo analizar Detalle vÃ­a AI (pre): {e}")
        elif crop_path_pre:
            print(f"âš ï¸ Recorte de detalle no encontrado en disco: {crop_path_pre}")

    with engine.begin() as conn:
        ya_existe = existe_en_bd(conn, tipo_doc, rut, folio)
        if ya_existe:
            doc_id_dup = doc_id
            det_count = _detalles_count(conn, doc_id_dup)
            if det_count > 0:
                # Documento duplicado con detalle existente: sin cambios
                print(f"ðŸ“„ LeÃ­do: {doc_id_dup} (ya existÃ­a con detalle, sin subida)")
                return
            else:
                # Documento existe pero sin detalle:
                # 1) si ya vienen items en datos_raw (ej. ai_reader), insertar siempre
                # 2) si no vienen, intentar usar pre-analisis interno (si IA local habilitada)
                items_dup = datos_raw.get("__items_detalle__") or []
                if (not items_dup) and AI_DETALLE_ENABLED and not _AI_DETALLE_DISABLED_RUNTIME:
                    items_dup = items_ai_pre

                if items_dup:
                    try:
                        _insertar_items_detalle(conn, doc_id_dup, items_dup)
                        # CategorizaciÃ³n AI si procede
                        try:
                            cat_res = categorizar_items_por_ai(items_dup)
                            if cat_res.get("ok"):
                                cats = cat_res.get("categorias", [])
                                for c in cats:
                                    linea = int(c.get("linea", 0))
                                    categoria = str(c.get("categoria", "")).strip()
                                    if linea > 0 and categoria:
                                        conn.execute(
                                            detalle.update()
                                            .where((detalle.c.id_doc == doc_id_dup) & (detalle.c.linea == linea))
                                            .values(categoria=categoria)
                                        )
                        except Exception as e:
                            print(f"âš ï¸ Error categorizando items por AI: {e}")
                        print(f"âœ… Subida a BD: {doc_id_dup} (detalle agregado)")
                    except Exception as e:
                        print(f"âš ï¸ No se pudo insertar detalle AI (duplicado): {e}")
                else:
                    if AI_DETALLE_ENABLED and not _AI_DETALLE_DISABLED_RUNTIME:
                        print(f"ðŸ“„ LeÃ­do: {doc_id_dup} (sin detalle AI, sin subida)")
                    else:
                        print(f"ðŸ“„ LeÃ­do: {doc_id_dup} (AI desactivada y sin items preleÃ­dos, sin subida)")
                return

        doc_data = {
            "id_doc": doc_id,
            "tipo_doc": tipo_doc,
            "folio": folio,
            "rut_emisor": rut,             # SOLO 'XXXXXXXX-D'
            "razon_social": razon,         # limpio, sin tildes/Ã±
            "giro": giro,                  # limpio, sin tildes/Ã±
            "fecha_emision": fecha,
            "IVA": iva_val,
            "monto_excento": 0.0,
            "impuesto_adicional": 0.0,
            "monto_total": total_val,
            # intenta extraer referencia desde etiquetas OCR (cualquier clave que contenga 'referen')
            "referencia": "",
            "DTE_referencia": "",
            "ruta_pdf": ruta_pdf
        }
        try:
            conn.execute(documentos.insert().values(**doc_data))
            # Inserta Ã­tems de detalle: si ya vienen, Ãºsalos; si no, intenta AI con recorte
            items_ai = datos_raw.get("__items_detalle__") or []
            if not items_ai and AI_DETALLE_ENABLED and not _AI_DETALLE_DISABLED_RUNTIME:
                # usa el pre-resultado si existe para evitar llamada dentro de la transacciÃ³n
                items_ai = items_ai_pre
                if AI_DETALLE_DEBUG and items_ai:
                    print(f"ðŸ§ª AI Detalle (pre-usada): {len(items_ai)} Ã­tems detectados")
            if items_ai:
                _insertar_items_detalle(conn, doc_id, items_ai)
                # CategorizaciÃ³n AI por lÃ­nea y documento
                try:
                    cat_res = categorizar_items_por_ai(items_ai)
                    if cat_res.get("ok"):
                        cats = cat_res.get("categorias", [])
                        for c in cats:
                            linea = int(c.get("linea", 0))
                            categoria = str(c.get("categoria", "")).strip()
                            if linea > 0 and categoria:
                                conn.execute(
                                    detalle.update()
                                    .where((detalle.c.id_doc == doc_id) & (detalle.c.linea == linea))
                                    .values(categoria=categoria)
                                )
                except Exception as e:
                    print(f"âš ï¸ Error categorizando items por AI: {e}")
            # Intenta vincular referencia si viene en OCR
            try:
                ref_text = ""
                for k, v in datos_raw.items():
                    if isinstance(k, str) and "referen" in k.lower():
                        ref_text = str(v)
                        break
                ref_id = parse_id_doc_referencia(ref_text) if ref_text else None
                if ref_id:
                    conn.execute(documentos.update().where(documentos.c.id_doc == doc_id).values(referencia=ref_id, DTE_referencia=ref_id))
            except Exception as e:
                print(f"âš ï¸ No se pudo parsear referencia: {e}")
            print(f"âœ… Subida a BD: {doc_id}")
        except IntegrityError:
            print(f"ðŸ“„ LeÃ­do: {doc_id} (ya existÃ­a, sin subida)")


# ============================================================
# ðŸš€ PIPELINE PRINCIPAL
# ============================================================

def procesar_factura(image_path):
    results = model.predict(image_path, save=False, verbose=False)
    datos = {}
    detalle_crop_path = None
    base_out = resource_path(str(BASE_DIR / "data/temp_images"))
    os.makedirs(base_out, exist_ok=True)
    for r in results:
        im = cv2.imread(r.path) if cv2 else None
        if im is None:
            continue
        for box, cls in zip(r.boxes.xyxy, r.boxes.cls):
            nombre = model.names[int(cls)]
            x1, y1, x2, y2 = map(int, box)
            m = 10; x1, y1 = max(0, x1 - m), max(0, y1 - m)
            x2, y2 = min(im.shape[1], x2 + m), min(im.shape[0], y2 + m)
            crop = im[y1:y2, x1:x2]
            if crop.size == 0:
                continue
            proc = mejorar_imagen(crop)
            if not reader:
                continue
            txt = reader.readtext(proc, detail=0)
            datos[nombre] = " ".join(txt)
            if nombre.lower() == "detalle":
                try:
                    detalle_crop_path = os.path.join(base_out, f"detalle_crop_{os.path.basename(image_path)}")
                    if cv2:
                        cv2.imwrite(detalle_crop_path, crop)
                    datos["__detalle_crop_path__"] = detalle_crop_path
                    if AI_DETALLE_DEBUG:
                        print(f"ðŸ–¼ï¸ Recorte de Detalle guardado: {detalle_crop_path}")
                except Exception as e:
                    print(f"âš ï¸ No se pudo guardar recorte Detalle: {e}")
    return datos

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
            print(f"âœ… Subida mÃ­nima a BD: {doc_id}")
    except Exception as e:
        print(f"âš ï¸ No se pudo insertar registro mÃ­nimo: {e}")

def procesar_todos_los_pdfs(progress_cb=None):
    existing_ids = get_all_doc_ids(engine)
    ml_ok = bool(model) and bool(reader) and bool(cv2) and bool(fitz) and bool(Image)
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
                print(f"â­ï¸ Omitido (ya en BD): {doc_id_guess}")
                continue
        print(f"ðŸ“„ LeÃ­do: {file}")
        img_path = None
        try:
            if not ml_ok:
                if len(parts) >= 3:
                    _insertar_minimo_desde_nombre(ruta_pdf_abs, tipo_doc_guess, rut_guess, folio_guess)
                else:
                    print("âš ï¸ Saltado: no se pudo inferir id desde nombre")
            else:
                img_path = pdf_a_imagen(ruta_pdf)
                if not img_path:
                    if len(parts) >= 3:
                        _insertar_minimo_desde_nombre(ruta_pdf_abs, tipo_doc_guess, rut_guess, folio_guess)
                    else:
                        print("âš ï¸ Saltado: sin imagen y sin inferencia de nombre")
                else:
                    datos = procesar_factura(img_path)
                    guardar_en_bd(datos, ruta_pdf_abs)
            done += 1
            if progress_cb:
                try:
                    progress_cb(done, total if total > 0 else 1)
                except Exception:
                    pass
        except Exception as e:
            print(f"âŒ Error procesando {file}: {e}")
        finally:
            # Limpieza por archivo: imagen temporal
            try:
                if img_path and os.path.exists(img_path):
                    os.remove(img_path)
                    if AI_DETALLE_DEBUG:
                        print(f"ðŸ§¹ Imagen temporal eliminada: {img_path}")
            except Exception as e:
                print(f"âš ï¸ No se pudo eliminar imagen temporal: {e}")
    # Fin del proceso

    # Limpieza
    temp_dir = Path(BASE_DIR / "data/temp_images")
    if temp_dir.exists():
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    procesar_todos_los_pdfs()

