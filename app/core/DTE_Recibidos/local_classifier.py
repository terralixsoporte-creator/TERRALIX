"""
Clasificador local TF-IDF + LinearSVC para líneas de detalle DTE.
Sin dependencia de OpenAI. Se entrena con datos históricos de SQLite.

Estrategia:
  1. Predicción jerárquica:
     - Primero agrega probabilidades por CATEGORIA (6 clases → alta accuracy)
     - Luego dentro de la mejor categoría, elige el catalogo_costo_id específico
  2. Features ponderados:
     - descripcion: señal principal (char n-grams + word n-grams)
     - razon_social + giro: repetidos 3x (señal más predictiva para categoría)
     - temporada agrícola: tokens desde fecha_emision y descripción

Uso:
  python local_classifier.py               # entrena y guarda modelo
  python local_classifier.py --test        # entrena + prueba con ejemplos
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
BASE_DIR = _THIS_DIR.parents[2]
MODEL_PATH_DEFAULT = BASE_DIR / "data" / "classifier_dte.pkl"

# =============================================================================
# CONSTANTES TEMPORALES (agricultura chilena – cítricos)
# =============================================================================
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
    "PEAJE":              "PEAJES",
    "SERVICIOS_AGRICOLA": "SERVICIOS_AGRICOLAS",
}

# Umbrales
MIN_CONFIDENCE: float = 0.25       # mínimo para aceptar predicción (0–1)
HIGH_CONFIDENCE: float = 0.60      # por encima → needs_review=0
MIN_SAMPLES_PER_CLASS: int = 2     # clases con menos → remap a OTRO
PROVIDER_REPEAT: int = 3           # cuántas veces repetir tokens de proveedor
TEMPORAL_REPEAT: int = 2           # cuántas veces repetir tokens temporales


# =============================================================================
# FUNCIONES DE FEATURES
# =============================================================================

def normalize_text(value: str) -> str:
    """Normaliza texto: quita tildes, mayúsculas, solo alfanumérico."""
    txt = unicodedata.normalize("NFKD", str(value or ""))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.upper()
    txt = re.sub(r"[^A-Z0-9]+", " ", txt)
    return " ".join(txt.split())


def _parse_month_from_date(fecha: Optional[str]) -> Optional[int]:
    raw = (fecha or "").strip()
    if not raw:
        return None
    from datetime import datetime as _dt
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y",
                "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return _dt.strptime(raw, fmt).month
        except Exception:
            pass
    m = re.search(r"(20\d{2})[-/](\d{1,2})[-/](\d{1,2})", raw)
    if m:
        mm = int(m.group(2))
        return mm if 1 <= mm <= 12 else None
    return None


def _extract_month_from_description(desc_normalized: str) -> Optional[int]:
    tokens = desc_normalized.split()
    for word in tokens:
        if word in _MONTH_NAMES:
            return _MONTH_NAMES[word]
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
    Construye el texto de features con ponderación por repetición.
    Proveedor y giro se repiten PROVIDER_REPEAT veces para darles más peso.
    """
    desc_n  = normalize_text(descripcion)
    prov_n  = normalize_text(razon_social)
    giro_n  = normalize_text(giro)
    temp_tk = build_temporal_tokens(fecha_emision, desc_n)

    parts = [desc_n]
    # Repetir proveedor y giro para darles más peso en TF-IDF
    for _ in range(PROVIDER_REPEAT):
        if prov_n:
            parts.append(f"PRV {prov_n}")
        if giro_n:
            parts.append(f"GIR {giro_n}")
    # Repetir tokens temporales
    for _ in range(TEMPORAL_REPEAT):
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
    for key in (f"{cat}|{sub}|{tipo}", f"{cat}|{sub}|OTRO", f"{cat}|OTRO|OTRO"):
        if key in fallback_map:
            return fallback_map[key]
    return None


def load_training_data(
    db_path: str,
) -> Tuple[List[str], List[int], Dict[int, Dict]]:
    """
    Carga datos de entrenamiento desde SQLite.
    Filtra labels de baja calidad (IA con confianza=0).
    """
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row

    cat_rows = con.execute(
        "SELECT id, categoria_costo, subcategoria_costo, tipo_gasto "
        "FROM catalogo_costos"
    ).fetchall()
    catalogo_by_id: Dict[int, Dict] = {int(r["id"]): dict(r) for r in cat_rows}

    fallback_map: Dict[str, int] = {}
    for r in cat_rows:
        cat  = (r["categoria_costo"]    or "").strip().upper()
        sub  = (r["subcategoria_costo"] or "").strip().upper()
        tipo = (r["tipo_gasto"]         or "").strip().upper()
        fallback_map[f"{cat}|{sub}|{tipo}"] = int(r["id"])

    # Filtrar: excluir IA con confianza=0 (labels ruidosos)
    rows = con.execute("""
        SELECT
            d.descripcion,
            d.categoria,
            d.subcategoria,
            d.tipo_gasto,
            d.catalogo_costo_id,
            d.confianza_categoria,
            d.origen_clasificacion,
            COALESCE(doc.razon_social,   '') AS razon_social,
            COALESCE(doc.giro,           '') AS giro,
            COALESCE(doc.fecha_emision,  '') AS fecha_emision
        FROM detalle d
        LEFT JOIN documentos doc ON doc.id_doc = d.id_doc
        WHERE d.categoria IS NOT NULL
          AND d.categoria NOT IN ('', 'SIN_CLASIFICAR')
          AND d.descripcion IS NOT NULL
          AND TRIM(d.descripcion) != ''
          AND NOT (
              d.origen_clasificacion = 'IA'
              AND (d.confianza_categoria IS NULL OR d.confianza_categoria = 0)
          )
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
            r["catalogo_costo_id"], r["categoria"], sub, tipo,
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
    vec = FeatureUnion([
        ("char", TfidfVectorizer(
            analyzer="char_wb", ngram_range=(2, 4),
            max_features=50_000, sublinear_tf=True, min_df=1,
        )),
        ("word", TfidfVectorizer(
            analyzer="word", ngram_range=(1, 2),
            max_features=25_000, sublinear_tf=True, min_df=1,
        )),
    ])
    base_svc = LinearSVC(C=2.0, max_iter=5000, class_weight="balanced")
    clf = (CalibratedClassifierCV(base_svc, cv=cv_folds, method="sigmoid")
           if calibrate else base_svc)
    return Pipeline([("vec", vec), ("clf", clf)])


def train_and_save(
    db_path: str,
    model_path: str = str(MODEL_PATH_DEFAULT),
) -> Dict[str, Any]:
    print("[TRAIN] Cargando datos de entrenamiento...")
    texts, labels, catalogo_by_id = load_training_data(db_path)

    if len(texts) < 20:
        return {"ok": False, "error": f"Datos insuficientes: {len(texts)} filas"}

    # Mapa catalogo_id → categoria para predicción jerárquica
    id_to_cat: Dict[int, str] = {}
    for cid, row in catalogo_by_id.items():
        id_to_cat[cid] = (row.get("categoria_costo") or "SIN_CLASIFICAR").strip().upper()

    # Remap clases raras → OTRO de su categoría
    label_counts = Counter(labels)
    otro_map: Dict[str, int] = {}
    for cid, row in catalogo_by_id.items():
        cat = id_to_cat[cid]
        key = f"{cat}|OTRO|OTRO"
        otro_map[key] = cid

    remapped: List[int] = []
    for lbl in labels:
        if label_counts[lbl] < MIN_SAMPLES_PER_CLASS:
            cat = id_to_cat.get(lbl, "SIN_CLASIFICAR")
            lbl = otro_map.get(f"{cat}|OTRO|OTRO", lbl)
        remapped.append(lbl)

    # Filtrar clases con < 3 ejemplos (necesario para calibración)
    remap_counts = Counter(remapped)
    final_texts:  List[str] = []
    final_labels: List[int] = []
    for text, lbl in zip(texts, remapped):
        if remap_counts[lbl] >= 3:
            final_texts.append(text)
            final_labels.append(lbl)

    if not final_texts:
        return {"ok": False, "error": "No quedan clases con >= 3 ejemplos"}

    n_classes   = len(set(final_labels))
    min_cls_cnt = min(Counter(final_labels).values())
    cv_folds    = min(3, min_cls_cnt)
    print(f"[TRAIN] {len(final_texts)} filas | {n_classes} clases | cv_folds={cv_folds}")

    # --- También preparar datos para clasificador de categoría ---
    cat_labels = [id_to_cat.get(lbl, "SIN_CLASIFICAR") for lbl in final_labels]
    n_cat_classes = len(set(cat_labels))
    print(f"[TRAIN] Categorías únicas: {n_cat_classes}")

    # Cross-val del clasificador de categoría
    cat_cv_accuracy: Optional[float] = None
    try:
        cat_pipe = _make_pipeline(calibrate=False)
        scores = cross_val_score(cat_pipe, final_texts, cat_labels, cv=cv_folds, scoring="accuracy")
        cat_cv_accuracy = float(scores.mean())
        print(f"[TRAIN] Cross-val CATEGORIA ({cv_folds}-fold): {cat_cv_accuracy:.2%}")
    except Exception as e:
        print(f"[TRAIN] Cross-val categoria omitido: {e}")

    # Cross-val del clasificador completo
    full_cv_accuracy: Optional[float] = None
    try:
        full_pipe = _make_pipeline(calibrate=False)
        scores = cross_val_score(full_pipe, final_texts, final_labels, cv=cv_folds, scoring="accuracy")
        full_cv_accuracy = float(scores.mean())
        print(f"[TRAIN] Cross-val COMPLETO ({cv_folds}-fold): {full_cv_accuracy:.2%}")
    except Exception as e:
        print(f"[TRAIN] Cross-val completo omitido: {e}")

    # Entrenar modelo final con calibración
    print("[TRAIN] Entrenando modelo final...")
    pipeline = _make_pipeline(calibrate=True, cv_folds=cv_folds)
    pipeline.fit(final_texts, final_labels)

    payload = {
        "pipeline":       pipeline,
        "catalogo_by_id": catalogo_by_id,
        "id_to_cat":      id_to_cat,
        "label_set":      sorted(set(final_labels)),
        "cat_cv_accuracy": cat_cv_accuracy,
        "full_cv_accuracy": full_cv_accuracy,
    }
    os.makedirs(os.path.dirname(model_path) or ".", exist_ok=True)
    with open(model_path, "wb") as f:
        pickle.dump(payload, f, protocol=4)

    print(f"[TRAIN] Modelo guardado en: {model_path}")
    return {
        "ok":              True,
        "n_samples":       len(final_texts),
        "n_classes":       n_classes,
        "n_categories":    n_cat_classes,
        "cat_cv_accuracy": cat_cv_accuracy,
        "full_cv_accuracy": full_cv_accuracy,
        "model_path":      model_path,
    }


# =============================================================================
# CLASIFICADOR (inferencia jerárquica)
# =============================================================================

class LocalClassifier:
    """
    Clasificador local con predicción jerárquica:
    1) Agrega probabilidades por categoría → elige la mejor
    2) Dentro de esa categoría → elige el mejor catalogo_costo_id
    """

    def __init__(self, model_path: str, db_path: str,
                 min_confidence: float = MIN_CONFIDENCE) -> None:
        self.model_path     = model_path
        self.db_path        = db_path
        self.min_confidence = min_confidence
        self._pipeline:       Optional[Pipeline]  = None
        self._catalogo_by_id: Dict[int, Dict]     = {}
        self._id_to_cat:      Dict[int, str]       = {}
        self._loaded = False

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
            self._id_to_cat      = payload.get("id_to_cat", {})
            # Rebuild id_to_cat si no viene en el pickle
            if not self._id_to_cat:
                for cid, row in self._catalogo_by_id.items():
                    self._id_to_cat[cid] = (
                        row.get("categoria_costo") or "SIN_CLASIFICAR"
                    ).strip().upper()
            self._loaded = True
            return True
        except Exception as e:
            print(f"[ML] Error al cargar modelo: {e}")
            return False

    def predict(
        self,
        descripcion:   str,
        razon_social:  str = "",
        giro:          str = "",
        fecha_emision: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Predicción jerárquica:
        1. Obtiene probabilidades para cada catalogo_costo_id
        2. Agrega por categoría → elige la mejor categoría
        3. Dentro de esa categoría → elige el mejor ID específico
        """
        if not self._load():
            return {"ok": False, "error": "model_not_loaded"}

        text = build_feature_text(descripcion, razon_social, giro, fecha_emision)

        try:
            proba   = self._pipeline.predict_proba([text])[0]
            classes = self._pipeline.classes_
        except Exception as e:
            return {"ok": False, "error": f"predict_error:{e}"}

        # --- Paso 1: Agregar probabilidades por categoría ---
        cat_proba: Dict[str, float] = {}
        for idx, prob in enumerate(proba):
            cid = int(classes[idx])
            cat = self._id_to_cat.get(cid, "SIN_CLASIFICAR")
            cat_proba[cat] = cat_proba.get(cat, 0.0) + float(prob)

        best_cat = max(cat_proba, key=cat_proba.get)  # type: ignore
        cat_confidence = cat_proba[best_cat]

        # --- Paso 2: Dentro de la mejor categoría, elegir el mejor ID ---
        best_id:   Optional[int]   = None
        best_prob: float           = -1.0
        cat_total: float           = 0.0

        for idx, prob in enumerate(proba):
            cid = int(classes[idx])
            if self._id_to_cat.get(cid) == best_cat:
                cat_total += float(prob)
                if float(prob) > best_prob:
                    best_prob = float(prob)
                    best_id   = cid

        # Confianza dentro de la categoría (normalizada)
        sub_confidence = best_prob / cat_total if cat_total > 0 else 0.0

        # Confianza compuesta: categoría * subcategoría
        combined_confidence = cat_confidence * sub_confidence

        if cat_confidence < self.min_confidence:
            return {
                "ok":      False,
                "error":   "low_confidence",
                "confianza": round(cat_confidence * 100),
            }

        if best_id is None:
            return {"ok": False, "error": "no_id_in_category"}

        row = self._catalogo_by_id.get(best_id)
        if not row:
            return {"ok": False, "error": "predicted_id_not_in_catalog"}

        cat_pct = round(cat_confidence * 100)
        sub_pct = round(sub_confidence * 100)
        needs_review = 0 if cat_confidence >= HIGH_CONFIDENCE else 1

        # Motivo detallado para auditoría
        temp_info = build_temporal_tokens(fecha_emision, normalize_text(descripcion))
        motivo = f"modelo_local:cat={cat_pct}%:sub={sub_pct}%"
        if temp_info:
            motivo += f":{temp_info}"

        return {
            "ok":                   True,
            "catalogo_costo_id":    best_id,
            "categoria":            (row.get("categoria_costo")    or "").strip(),
            "subcategoria":         (row.get("subcategoria_costo") or "").strip(),
            "tipo_gasto":           (row.get("tipo_gasto")         or "").strip(),
            "confianza":            cat_pct,
            "confianza_sub":        sub_pct,
            "needs_review":         needs_review,
            "origen_clasificacion": "ML_LOCAL",
            "motivo_clasificacion": motivo,
        }

    def retrain(self) -> Dict[str, Any]:
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
    global _clf_instance
    mp = model_path or str(MODEL_PATH_DEFAULT)
    if _clf_instance is None or _clf_instance.db_path != db_path:
        _clf_instance = LocalClassifier(model_path=mp, db_path=db_path)
    return _clf_instance


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Entrenar clasificador local DTE")
    parser.add_argument("--db",    default=None, help="Ruta a DteRecibidos_db.db")
    parser.add_argument("--model", default=None, help="Ruta de salida del modelo .pkl")
    parser.add_argument("--test",  action="store_true", help="Ejecutar casos de prueba")
    args = parser.parse_args()

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
        print(f"\n[TEST] Predicciones (umbral confianza: {MIN_CONFIDENCE*100:.0f}%):")
        print(f"  {'Descripcion':<40} {'Fecha':<12} {'Categoria':<22} {'Sub':<25} {'Tipo':<20} {'Cat%':>4} {'Sub%':>4}")
        print("  " + "-" * 135)
        for desc, prov, gir, fecha in test_cases:
            pred = clf.predict(desc, prov, gir, fecha)
            if pred.get("ok"):
                nr = " [R]" if pred["needs_review"] else ""
                print(
                    f"  {desc:<40} {str(fecha or ''):<12} "
                    f"{pred['categoria']:<22} {pred['subcategoria']:<25} "
                    f"{pred['tipo_gasto']:<20} {pred['confianza']:>3}% {pred.get('confianza_sub',0):>3}%{nr}"
                )
            else:
                print(
                    f"  {desc:<40} {str(fecha or ''):<12} "
                    f"FALLO ({pred.get('error')}, conf={pred.get('confianza', '?')}%)"
                )
