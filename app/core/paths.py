"""
Resolvedor centralizado de rutas para Terralix ERP.

Regla:
  - Archivos de SOLO LECTURA (imágenes, PDF manual, modelo inicial, config template)
    → empaquetados por PyInstaller en _internal/data/  (no se modifican)
  - Archivos ESCRIBIBLES (config.env, modelo ML, storage_state, logs, estado)
    → %APPDATA%\\Terralix ERP  (cuando frozen/instalado)
    → <raiz_proyecto>/data      (cuando en desarrollo)

Lógica de config.env:
  - Si no existe en AppData → se copia el bundled completo
  - Si existe pero le faltan claves → se agregan desde el bundled (merge)
  - Las claves que el usuario ya tiene (rutas, etc.) NO se tocan
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path


# ──────────────────────────────────────────────────────────────
# Directorios base
# ──────────────────────────────────────────────────────────────

def _user_data_dir() -> Path:
    """
    Carpeta escribible del usuario:
      - Frozen  : %APPDATA%\\Terralix ERP
      - Dev     : <raíz proyecto>/data
    """
    if getattr(sys, "frozen", False):
        appdata = os.environ.get("APPDATA") or str(Path.home())
        return Path(appdata) / "Terralix ERP"
    # Desarrollo: app/core/paths.py → parents[0]=app/core, [1]=app, [2]=raíz
    return Path(__file__).resolve().parents[2] / "data"


def _bundled_data_dir() -> Path:
    """
    Carpeta de datos de solo lectura empaquetados por PyInstaller.
    En desarrollo coincide con _user_data_dir().
    """
    if getattr(sys, "frozen", False):
        try:
            return Path(sys._MEIPASS) / "data"  # type: ignore[attr-defined]
        except Exception:
            return Path(sys.executable).parent / "_internal" / "data"
    return Path(__file__).resolve().parents[2] / "data"


# ──────────────────────────────────────────────────────────────
# Merge de config.env
# ──────────────────────────────────────────────────────────────

def _parse_env_file(path: Path) -> dict:
    """
    Lee un archivo .env y devuelve {KEY: 'linea_original'}.
    Conserva líneas de comentario por separado.
    """
    keys = {}
    if not path.exists():
        return keys
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            keys[key] = line  # guardamos la línea completa
    return keys


def _merge_env_files(user_path: Path, bundled_path: Path) -> None:
    """
    Agrega al config.env del usuario las claves que existen en el bundled
    pero faltan en el del usuario. No modifica claves ya existentes.
    """
    if not bundled_path.exists():
        return

    user_keys   = _parse_env_file(user_path)
    bundled_raw = bundled_path.read_text(encoding="utf-8", errors="replace")

    lines_to_add = []
    for line in bundled_raw.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key not in user_keys:
                lines_to_add.append(line)

    if lines_to_add:
        with user_path.open("a", encoding="utf-8") as f:
            f.write("\n# --- claves agregadas automáticamente ---\n")
            for l in lines_to_add:
                f.write(l + "\n")


# ──────────────────────────────────────────────────────────────
# API pública
# ──────────────────────────────────────────────────────────────

def get_user_data_dir() -> Path:
    """Devuelve (y crea si no existe) la carpeta de datos del usuario."""
    d = _user_data_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_env_path() -> Path:
    """
    Ruta al config.env escribible.

    Comportamiento:
      1. Si NO existe en AppData → copia el bundled completo (primer arranque limpio)
      2. Si existe pero le FALTAN claves → hace merge agregando las que faltan
         (cubre el caso de instalaciones previas con config incompleto)
      3. Si existe y está completo → no lo toca
    """
    env_path    = get_user_data_dir() / "config.env"
    bundled     = _bundled_data_dir() / "config.env"

    if not env_path.exists() or env_path.stat().st_size == 0:
        # Primer arranque: copiar bundled completo
        if bundled.exists() and bundled.stat().st_size > 0:
            shutil.copy2(str(bundled), str(env_path))
        else:
            env_path.touch()
    else:
        # Ya existe: agregar solo las claves faltantes
        _merge_env_files(env_path, bundled)

    return env_path


def get_storage_state_path() -> Path:
    """Ruta al storage_state.json de Playwright (siempre escribible)."""
    return get_user_data_dir() / "storage_state.json"


def get_state_path() -> Path:
    """Ruta al estado del verificador semanal automático."""
    return get_user_data_dir() / "auto_weekly_dte_check_state.json"


def get_logs_dir() -> Path:
    """Carpeta de logs (siempre escribible, se crea si no existe)."""
    d = get_user_data_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d


def get_model_path() -> Path:
    """
    Ruta al modelo ML classifier_dte.pkl (siempre escribible).
    Primer arranque: copia el modelo inicial bundled a AppData.
    """
    model_path = get_user_data_dir() / "classifier_dte.pkl"

    if not model_path.exists():
        bundled = _bundled_data_dir() / "classifier_dte.pkl"
        if bundled.exists():
            shutil.copy2(str(bundled), str(model_path))

    return model_path
