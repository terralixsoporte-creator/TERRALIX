"""
Clasificador local TF-IDF + LinearSVC para líneas de detalle DTE.
Sin dependencia de OpenAI. Se entrena con datos históricos de SQLite.

Features:
  - descripcion (char n-grams + word n-grams)
  - razon_social + giro del proveedor
  - tokens de temporada agrícola (cosecha, invierno, etc.) extraídos
    desde fecha_emision del documento Y desde la propia descripción

Uso rápido:
  python local_classifier.py               # entrena y guarda modelo
  python local_classifier.py --test        # entrena + prueba con ejemplos
  python local_classifier.py --db <ruta>   # usa DB específica
"""

from __future__ import annotations

import os
import re
import sys
import pickle
import sqlite3
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

import numpy as np
from sklearn.calibration import CalibratedClassifierCV
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import FeatureUnion, Pipeline
from sklearn.svm import LinearSVC

# =============================================================================
# RUTAS
# =============================================================================
_THIS_DIR = Path(__file__).resolve().parent
BASE_DIR = _THIS_DIR.parents[2]          # raíz del proyecto TERRALIX
MODEL_PATH_DEFAULT = BASE_DIR / "data" / "classifier_dte.pkl"

# =============================================================================
# CONSTANTES TEMPORALES (agricultura chilena – cítricos)
# =============================================================================
# Mes → etiqueta de temporada
_SEASON_MAP: Dict[int, str] = {
    1:  "TEMP_POSTCOSECHA",
    2:  "TEMP_POSTCOSECHA",
    3:  "TEMP_OTONO",
    4:  "TEMP_OTONO",
    5:  "TEMP_OTONO",
    6:  "TEMP_INVIERNO",
    7:  "TEMP_INVIERNO",
    8:  "TEMP_INVIERNO",
    9:  "TEMP_PRIMAVERA",
    10: "TEMP_PRIMAVERA",
    11: "TEMP_COSECHA",
    12: "TEMP_COSECHA TEMP_POSTCOSECHA",
}

# Nombres de meses en español para extraer desde la descripción
_MONTH_NAMES: Dict[str, int] = {
    "ENERO": 1,    "ENE": 1,
    "FEBRERO": 2,  "FEB": 2,
    "MARZO": 3,    "MAR": 3,
    "ABRIL": 4,    "ABR": 4,
    "MAYO": 5,
    "JUNIO": 6,    "JUN": 6,
    "JULIO": 7,    "JUL": 7,
    "AGOSTO": 8,   "AGO": 8,
    "SEPTIEMBRE": 9, "SEP": 9, "SEPT": 9,
    "OCTUBRE": 10, "OCT": 10,
    "NOVIEMBRE": 11, "NOV": 11,
    "DICIEMBRE": 12, "DIC": 12,
}

# Typos conocidos en los datos históricos
_TIPO_NORM: Dict[str, str] = {
    "PEAJE":             "PEAJES",
    "SERVICIOS_AGRICOLA": "SERVICIOS_AGRICOLAS",
}

# Umbral mínimo de confianza para aceptar predicción (0–1)
MIN_CONFIDENCE: float = 0.40
# Por encima de este umbral se marca needs_review=0
HIGH_CONFIDENCE: float = 0.70
# Mínimo de ejemplos por clase para entrenar; si menos → remap a OTRO
MIN_SAMPLES_PER_CLASS: int = 2


# =============================================================================
# FUNCIONES DE FEATURES
# =============================================================================

def normalize_text(value: str) -> str:
    """Normaliza texto: minúsculas→mayúsculas sin tildes, solo alfanumérico."""
    txt = unicodedata.normalize("NFKD", str(value or ""))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.upper()
    txt = re.sub(r"[^A-Z0-9]+", " ", txt)
    return " ".join(txt.split())


def _parse_month_from_date(fecha: Optional[str]) -> Optional[int]:
    """Extrae el mes numérico desde una cadena de fecha (varios formatos)."""
    raw = (fecha or "").strip()
    if not raw:
        return None
    from datetime import datetime
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(raw, fmt).month
        except Exception:
            pass
    m = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", raw)
    if m:
        mm = int(m.group(2))
        return mm if 1 <= mm <= 12 else None
    return None


def _extract_month_from_description(desc_normalized: str) -> Optional[int]:
    """
    Busca nombres de meses en la descripción ya normalizada.
    Ej: "SERVICIO NOVIEMBRE 2024" → 11
    """
    tokens = desc_normalized.split()
    # Primero tokens completos (evita que "MAR" matchee "MARZO" ambiguamente)
    for word in tokens:
        if word in _MONTH_NAMES:
            return _MONTH_NAMES[word]
    # Luego patrones numéricos tipo "11/2024" o "2024-11"
    m = re.search(r"\b(0?[1-9]|1[0-2])[/\-](20\d{2})\b", desc_normalized)
    if m:
        return int(m.group(1))
    m = re.search(r"\b(20\d{2})[/\-](0?[1-9]|1[0-2])\b", desc_normalized)
    if m:
        return int(m.group(2))
    return None


def build_temporal_tokens(
    fecha_emision: Optional[str],
    descripcion_norm: str = "",
) -> str:
    """
    Devuelve tokens de temporada agrícola para agregar al feature text.
    Prioriza fecha_emision; si no hay, busca en la descripción.

    Ej: "TEMP_MES_11 TEMP_COSECHA"
    """
    mm = _parse_month_from_date(fecha_emision)
    if mm is None:
        mm = _extract_month_from_description(descripcion_norm)
    if mm is None:
        return ""
    season = _SEASON_MAP.get(mm, "")
    return f"TEMP_MES_{mm:02d} {season}".strip()


def build_feature_text(
    descripcion: str,
    razon_social: str = "",
    giro: str = "",
    fecha_emision: Optional[str] = None,
) -> str:
    """
    Construye el texto de features para el vectorizador.
    Formato: <desc> PRV <proveedor> GIR <giro> <tokens_temporales>
    """
    desc_n  = normalize_text(descripcion)
    prov_n  = normalize_text(razon_social)
    giro_n  = normalize_text(giro)
    temp_tk = build_temporal_tokens(fecha_emision, desc_n)

    parts = [desc_n]
    if prov_n:
        parts.append(f"PRV {prov_n}")
    if giro_n:
        parts.append(f"GIR {giro_n}")
    if temp_tk:
        parts.append(temp_tk)
    return " ".join(parts)


# =============================================================================
# CARGA DE DATOS DE ENTRENAMIENTO
# =============================================================================

def _resolve_label(
    catalogo_by_id: Dict[int, Dict],
    fallback_map: Dict[str, int],
    catalogo_costo_id: Any,
    categoria: str,
    subcategoria: str,
    tipo_gasto: str,
) -> Optional[int]:
    """Resuelve el catalogo_costo_id definitivo para una fila de entrenamiento."""
    if catalogo_costo_id:
        try:
            cid = int(catalogo_costo_id)
            if cid in catalogo_by_id:
                return cid
        except (ValueError, TypeError):
            pass

    cat  = (categoria   or "").strip().upper()
    sub  = _TIPO_NORM.get((subcategoria or "").strip().upper(),
                          (subcategoria or "").strip().upper())
    tipo = _TIPO_NORM.get((tipo_gasto   or "").strip().upper(),
                          (tipo_gasto   or "").strip().upper())

    # Exacto → sin subcategoria exacta → fallback OTRO
    for key in (f"{cat}|{sub}|{tipo}", f"{cat}|{sub}|OTRO", f"{cat}|OTRO|OTRO"):
        if key in fallback_map:
            return fallback_map[key]
    return None


def load_training_data(
    db_path: str,
) -> Tuple[List[str], List[int], Dict[int, Dict]]:
    """
    Carga datos de entrenamiento desde SQLite.
    Retorna (textos, etiquetas_catalogo_id, catalogo_by_id).
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    # Catálogo completo
    cat_rows = con.execute(
        "SELECT id, categoria_costo, subcategoria_costo, tipo_gasto "
        "FROM catalogo_costos"
    ).fetchall()
    catalogo_by_id: Dict[int, Dict] = {int(r["id"]): dict(r) for r in cat_rows}

    # Mapa de lookup cat|sub|tipo → id
    fallback_map: Dict[str, int] = {}
    for r in cat_rows:
        cat  = (r["categoria_costo"]  or "").strip().upper()
        sub  = (r["subcategoria_costo"] or "").strip().upper()
        tipo = (r["tipo_gasto"]        or "").strip().upper()
        fallback_map[f"{cat}|{sub}|{tipo}"] = int(r["id"])

    # Detalles etiquetados
    rows = con.execute("""
        SELECT
            d.descripcion,
            d.categoria,
            d.subcategoria,
            d.tipo_gasto,
            d.catalogo_costo_id,
            COALESCE(doc.razon_social,   '') AS razon_social,
            COALESCE(doc.giro,           '') AS giro,
            COALESCE(doc.fecha_emision,  '') AS fecha_emision
        FROM detalle d
        LEFT JOIN documentos doc ON doc.id_doc = d.id_doc
        WHERE d.categoria IS NOT NULL
          AND d.categoria NOT IN ('', 'SIN_CLASIFICAR')
          AND d.descripcion IS NOT NULL
          AND TRIM(d.descripcion) != ''
    """).fetchall()
    con.close()

    texts:  List[str] = []
    labels: List[int] = []

    for r in rows:
        sub  = _TIPO_NORM.get((r["subcategoria"] or "").strip().upper(),
                              (r["subcategoria"] or "").strip().upper())
        tipo = _TIPO_NORM.get((r["tipo_gasto"]   or "").strip().upper(),
                              (r["tipo_gasto"]   or "").strip().upper())

        label_id = _resolve_label(
            catalogo_by_id, fallback_map,
            r["catalogo_costo_id"],
            r["categoria"], sub, tipo,
        )
        if label_id is None:
            continue

        text = build_feature_text(
            descripcion   = r["descripcion"],
            razon_social  = r["razon_social"],
            giro          = r["giro"],
            fecha_emision = r["fecha_emision"],
        )
        texts.append(text)
        labels.append(label_id)

    return texts, labels, catalogo_by_id


# =============================================================================
# ENTRENAMIENTO
# =============================================================================

def _make_pipeline(calibrate: bool = True, cv_folds: int = 3) -> Pipeline:
    """Construye el pipeline TF-IDF (char + word) + LinearSVC."""
    vec = FeatureUnion([
        ("char", TfidfVectorizer(
            analyzer    = "char_wb",
            ngram_range = (2, 4),
            max_features= 40_000,
            sublinear_tf= True,
            min_df      = 1,
        )),
        ("word", TfidfVectorizer(
            analyzer    = "word",
            ngram_range = (1, 2),
            max_features= 20_000,
            sublinear_tf= True,
            min_df      = 1,
        )),
    ])
    base_svc = LinearSVC(C=1.5, max_iter=4000, class_weight="balanced")
    clf = CalibratedClassifierCV(base_svc, cv=cv_folds, method="isotonic") if calibrate else base_svc
    return Pipeline([("vec", vec), ("clf", clf)])


def train_and_save(
    db_path: str,
    model_path: str = str(MODEL_PATH_DEFAULT),
) -> Dict[str, Any]:
    """
    Entrena el modelo local con los datos de la DB y lo guarda.
    Retorna estadísticas del entrenamiento.
    """
    print("[TRAIN] Cargando datos de entrenamiento...")
    texts, labels, catalogo_by_id = load_training_data(db_path)

    if len(texts) < 20:
        return {"ok": False, "error": f"Datos insuficientes: {len(texts)} filas"}

    # Remap clases raras → OTRO de su categoría
    label_counts = Counter(labels)
    fallback_map: Dict[str, int] = {}
    for cid, row in catalogo_by_id.items():
        cat = (row.get("categoria_costo") or "").strip().upper()
        key = f"{cat}|OTRO|OTRO"
        fallback_map[key] = cid

    # Primer remap: clases con < MIN_SAMPLES_PER_CLASS → OTRO de su categoría
    remapped: List[int] = []
    for text, lbl in zip(texts, labels):
        if label_counts[lbl] < MIN_SAMPLES_PER_CLASS:
            row = catalogo_by_id.get(lbl, {})
            cat = (row.get("categoria_costo") or "SIN_CLASIFICAR").strip().upper()
            lbl = fallback_map.get(f"{cat}|OTRO|OTRO", lbl)
        remapped.append(lbl)

    # Segundo remap: eliminar clases que siguen con < 3 ejemplos (calibración requiere ≥ 3)
    remap2_counts = Counter(remapped)
    final_texts:  List[str] = []
    final_labels: List[int] = []
    for text, lbl in zip(texts, remapped):
        if remap2_counts[lbl] >= 3:
            final_texts.append(text)
            final_labels.append(lbl)

    if not final_texts:
        return {"ok": False, "error": "No quedan clases con ≥ 3 ejemplos tras el filtrado"}

    n_classes   = len(set(final_labels))
    min_cls_cnt = min(Counter(final_labels).values())
    cv_folds    = min(3, min_cls_cnt)
    print(f"[TRAIN] {len(final_texts)} filas | {n_classes} clases | cv_folds={cv_folds}")

    # Cross-val rápida sin calibración
    cv_accuracy: Optional[float] = None
    try:
        base_pipe = _make_pipeline(calibrate=False)
        scores = cross_val_score(
            base_pipe, final_texts, final_labels,
            cv=cv_folds, scoring="accuracy",
        )
        cv_accuracy = float(scores.mean())
        print(f"[TRAIN] Cross-val accuracy ({cv_folds}-fold): {cv_accuracy:.2%}")
    except Exception as e:
        print(f"[TRAIN] Cross-val omitido: {e}")

    # Entrenamiento final con calibración para predict_proba
    print("[TRAIN] Entrenando modelo final con calibración...")
    pipeline = _make_pipeline(calibrate=True, cv_folds=cv_folds)
    pipeline.fit(final_texts, final_labels)

    payload = {
        "pipeline":       pipeline,
        "catalogo_by_id": catalogo_by_id,
        "label_set":      sorted(set(final_labels)),
        "cv_accuracy":    cv_accuracy,
    }
    os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(payload, f, protocol=4)

    print(f"[TRAIN] Modelo guardado en: {model_path}")
    return {
        "ok":          True,
        "n_samples":   len(final_texts),
        "n_classes":   n_classes,
        "cv_accuracy": cv_accuracy,
        "model_path":  model_path,
    }


# =============================================================================
# CLASIFICADOR (inferencia)
# =============================================================================

class LocalClassifier:
    """
    Clasificador local cargado desde pickle.
    Si el archivo no existe, se entrena automáticamente.
    """

    def __init__(
        self,
        model_path: str,
        db_path: str,
        min_confidence: float = MIN_CONFIDENCE,
    ) -> None:
        self.model_path     = model_path
        self.db_path        = db_path
        self.min_confidence = min_confidence
        self._pipeline:       Optional[Pipeline]       = None
        self._catalogo_by_id: Dict[int, Dict]          = {}
        self._loaded = False

    # ------------------------------------------------------------------
    def _load(self) -> bool:
        if self._loaded:
            return True
        if not os.path.exists(self.model_path):
            print(f"[ML] Modelo no encontrado en {self.model_path}. Entrenando...")
            result = train_and_save(self.db_path, self.model_path)
            if not result.get("ok"):
                print(f"[ML] Error al entrenar: {result.get('error')}")
                return False
        try:
            with open(self.model_path, "rb") as f:
                payload = pickle.load(f)
            self._pipeline       = payload["pipeline"]
            self._catalogo_by_id = payload["catalogo_by_id"]
            self._loaded = True
            return True
        except Exception as e:
            print(f"[ML] Error al cargar modelo: {e}")
            return False

    # ------------------------------------------------------------------
    def predict(
        self,
        descripcion:  str,
        razon_social: str = "",
        giro:         str = "",
        fecha_emision: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Predice el catalogo_costo_id de una línea de detalle.

        Returns dict con claves:
          ok, catalogo_costo_id, categoria, subcategoria, tipo_gasto,
          confianza (0-100), needs_review (0/1),
          origen_clasificacion, motivo_clasificacion
        """
        if not self._load():
            return {"ok": False, "error": "model_not_loaded"}

        text = build_feature_text(descripcion, razon_social, giro, fecha_emision)

        try:
            proba       = self._pipeline.predict_proba([text])[0]   # type: ignore[union-attr]
            classes     = self._pipeline.classes_                    # type: ignore[union-attr]
            top_idx     = int(np.argmax(proba))
            predicted_id= int(classes[top_idx])
            confidence  = float(proba[top_idx])
        except Exception as e:
            return {"ok": False, "error": f"predict_error:{e}"}

        if confidence < self.min_confidence:
            return {
                "ok":      False,
                "error":   "low_confidence",
                "confianza": round(confidence * 100),
            }

        row = self._catalogo_by_id.get(predicted_id)
        if not row:
            return {"ok": False, "error": "predicted_id_not_in_catalog"}

        conf_pct    = round(confidence * 100)
        needs_review= 0 if confidence >= HIGH_CONFIDENCE else 1

        # Token temporal para el motivo (ayuda a auditar)
        temp_info = build_temporal_tokens(fecha_emision, normalize_text(descripcion))
        motivo    = f"modelo_local:conf={conf_pct}"
        if temp_info:
            motivo += f":{temp_info}"

        return {
            "ok":                   True,
            "catalogo_costo_id":    predicted_id,
            "categoria":            (row.get("categoria_costo")   or "").strip(),
            "subcategoria":         (row.get("subcategoria_costo") or "").strip(),
            "tipo_gasto":           (row.get("tipo_gasto")        or "").strip(),
            "confianza":            conf_pct,
            "needs_review":         needs_review,
            "origen_clasificacion": "ML_LOCAL",
            "motivo_clasificacion": motivo,
        }

    # ------------------------------------------------------------------
    def retrain(self) -> Dict[str, Any]:
        """Re-entrena el modelo con datos actualizados de la DB."""
        self._loaded   = False
        self._pipeline = None
        result = train_and_save(self.db_path, self.model_path)
        if result.get("ok"):
            self._load()
        return result


# =============================================================================
# SINGLETON (lazy)
# =============================================================================
_clf_instance: Optional[LocalClassifier] = None


def get_classifier(
    db_path: str,
    model_path: Optional[str] = None,
) -> LocalClassifier:
    """Retorna la instancia global del clasificador (carga lazy)."""
    global _clf_instance
    mp = model_path or str(MODEL_PATH_DEFAULT)
    if _clf_instance is None or _clf_instance.db_path != db_path:
        _clf_instance = LocalClassifier(model_path=mp, db_path=db_path)
    return _clf_instance


# =============================================================================
# CLI – entrenamiento y pruebas rápidas
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Entrenar clasificador local DTE")
    parser.add_argument("--db",    default=None, help="Ruta a DteRecibidos_db.db")
    parser.add_argument("--model", default=None, help="Ruta de salida del modelo .pkl")
    parser.add_argument("--test",  action="store_true", help="Ejecutar casos de prueba")
    args = parser.parse_args()

    # Resolver DB desde config.env si no se pasa
    if not args.db:
        env_path = BASE_DIR / "data" / "config.env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                if line.startswith("DB_PATH_DTE_RECIBIDOS"):
                    val = line.split("=", 1)[1].strip().strip("'\"")
                    args.db = val
                    break

    if not args.db:
        print("[ERROR] Especifica --db o configura DB_PATH_DTE_RECIBIDOS en config.env")
        sys.exit(1)

    mp     = args.model or str(MODEL_PATH_DEFAULT)
    result = train_and_save(args.db, mp)
    print(f"\n[RESULTADO] {result}")

    if args.test and result.get("ok"):
        clf = LocalClassifier(model_path=mp, db_path=args.db)
        test_cases = [
            # (descripcion, razon_social, giro, fecha_emision)
            ("HERBICIDA PANTERA GOLD 1L",        "COPEVAL",            "INSUMOS AGRICOLAS",         None),
            ("COPLA 16 X 16MM IMPORT TAVILT",    "OLIVOS SPA",         "PROY INST RIEGO",           None),
            ("TAG AUTOPISTA PEAJE",               "",                   "",                          "2024-07-15"),
            ("TRACTOR CON CHOFER",                "JUAN RAMON MORALES", "AGRICULTOR TRANSPORTE",     "2024-11-20"),
            ("COMISION MANTENCION CUENTA",        "BANCO BCI",          "BANCO",                     None),
            ("MANO OBRA COSECHA",                 "CONTRATISTA",        "SERVICIOS AGRICOLAS",       "2024-11-05"),
            ("MANO OBRA",                         "CONTRATISTA",        "SERVICIOS AGRICOLAS",       "2024-07-10"),
            ("COLACION CUADRILLA NOVIEMBRE",      "PROVEEDOR",          "",                          "2024-11-01"),
            ("SERVICIO PODA",                     "JUAN MORALES",       "AGRICULTOR",                "2024-06-15"),
            ("FACTURA HONORARIOS CONTABILIDAD",   "ESTUDIO CONTABLE",   "CONTABILIDAD ASESORIA",     None),
        ]
        print("\n[TEST] Predicciones:")
        print(f"  {'Descripcion':<40} {'Fecha':<12}   {'Categoria':<22} {'Sub':<25} {'Tipo':<22} {'Conf':>5}")
        print("  " + "-" * 130)
        for desc, prov, gir, fecha in test_cases:
            pred = clf.predict(desc, prov, gir, fecha)
            if pred.get("ok"):
                nr_flag = " [R]" if pred["needs_review"] else ""
                print(
                    f"  {desc:<40} {str(fecha or ''):<12}   "
                    f"{pred['categoria']:<22} {pred['subcategoria']:<25} "
                    f"{pred['tipo_gasto']:<22} {pred['confianza']:>4}%{nr_flag}"
                )
            else:
                print(
                    f"  {desc:<40} {str(fecha or ''):<12}   "
                    f"FALLO ({pred.get('error')}, conf={pred.get('confianza', '?')}%)"
                )
