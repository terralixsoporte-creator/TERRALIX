# -*- mode: python ; coding: utf-8 -*-
"""
PyInstaller spec for Terralix ERP.
Usage:  pyinstaller TERRALIX.spec
"""

import os
import sys
from pathlib import Path
from PyInstaller.utils.hooks import collect_all, collect_submodules, collect_data_files

block_cipher = None

# ── paths ──────────────────────────────────────────────────────────────
ROOT     = os.path.abspath(".")
APP_DIR  = os.path.join(ROOT, "app")
ASSETS_DIR = os.path.join(APP_DIR, "assets")
CORE_DIR   = os.path.join(APP_DIR, "core")
DATA_DIR   = os.path.join(ROOT, "data")
ICON       = os.path.join(ROOT, "Terralix_Logo.ico")

# Virtual-env site-packages
VENV_SP = os.path.join(ROOT, "terr", "Lib", "site-packages")

# Playwright browsers (source candidates)
PW_BROWSER_SOURCES = [
    os.path.join(VENV_SP, "playwright", "driver", "package", ".local-browsers"),
    os.path.join(os.environ.get("LOCALAPPDATA", ""), "ms-playwright"),
    os.path.join(ROOT, "ms-playwright"),
]
PW_BROWSERS_SRC = ""
for _candidate in PW_BROWSER_SOURCES:
    if _candidate and os.path.isdir(_candidate):
        PW_BROWSERS_SRC = _candidate
        break


# ── collect_all para paquetes que PyInstaller no detecta sólo ─────────
dotenv_d,    dotenv_b,    dotenv_h    = collect_all("dotenv")
openai_d,    openai_b,    openai_h    = collect_all("openai")
sklearn_d,   sklearn_b,   sklearn_h   = collect_all("sklearn")
scipy_d,     scipy_b,     scipy_h     = collect_all("scipy")
requests_d,  requests_b,  requests_h  = collect_all("requests")
fitz_d,      fitz_b,      fitz_h      = collect_all("fitz")
PIL_d,       PIL_b,       PIL_h       = collect_all("PIL")
sqla_d,      sqla_b,      sqla_h      = collect_all("sqlalchemy")
# supabase-py reemplazado por requests directo — solo necesitamos certifi/httpx para SSL
# supabase_d,  supabase_b,  supabase_h  = collect_all("supabase")
keyring_d,   keyring_b,   keyring_h   = collect_all("keyring")
openpyxl_d,  openpyxl_b,  openpyxl_h  = collect_all("openpyxl")
certifi_d,   certifi_b,   certifi_h   = collect_all("certifi")
httpx_d,     httpx_b,     httpx_h     = collect_all("httpx")

# ── data / assets to bundle ───────────────────────────────────────────
datas = [
    # Imágenes y assets de la GUI
    (os.path.join(ASSETS_DIR, "imgs"),   os.path.join("app", "assets", "imgs")),
    (os.path.join(ASSETS_DIR, "update"), os.path.join("app", "assets", "update")),

    # Código fuente de la app (necesario para que Python lo importe en runtime)
    (os.path.join(APP_DIR, "core"), os.path.join("app", "core")),
    (os.path.join(APP_DIR, "gui"),  os.path.join("app", "gui")),

    # Modelo ML (12 MB) — CRÍTICO para local_classifier.py
    (os.path.join(DATA_DIR, "classifier_dte.pkl"), "data"),

    # Manual PDF
    (os.path.join(DATA_DIR, "Manual_Terralix_ERP.pdf"), "data"),

    # Config template — se copia a AppData en el primer arranque
    # Contiene SUPABASE_URL, SUPABASE_ANON_KEY, OPENAI_API_KEY, etc.
    (os.path.join(DATA_DIR, "config.env"), "data"),

    # Datos de colecciones externas
    *dotenv_d,
    *openai_d,
    *sklearn_d,
    *scipy_d,
    *requests_d,
    *fitz_d,
    *PIL_d,
    *sqla_d,
    *keyring_d,
    *openpyxl_d,
    *certifi_d,
    *httpx_d,
]

# __init__.py vacío para que app/ sea un paquete reconocible
app_init = os.path.join(APP_DIR, "__init__.py")
if os.path.isfile(app_init):
    datas.append((app_init, "app"))

core_init = os.path.join(CORE_DIR, "__init__.py")
if os.path.isfile(core_init):
    datas.append((core_init, os.path.join("app", "core")))

# auth.py está en app/core/ (no en assets/)
auth_file = os.path.join(CORE_DIR, "auth.py")
if os.path.isfile(auth_file):
    datas.append((auth_file, os.path.join("app", "core")))

# Playwright browsers -> empaquetar en la ruta exacta que espera Playwright frozen
if PW_BROWSERS_SRC:
    datas.append(
        (
            PW_BROWSERS_SRC,
            os.path.join("playwright", "driver", "package", ".local-browsers"),
        )
    )


# ── hidden imports ────────────────────────────────────────────────────
hiddenimports = [
    # ── Módulos de la app ──────────────────────────────────────────
    "app",
    "app.gui",
    "app.gui.LogIn_page",
    "app.gui.main_app",
    "app.gui.actualizar_base_de_datos",
    "app.gui.utils",
    "app.core",
    "app.core.paths",
    "app.core.auth",
    "app.core.DTE_Recibidos",
    "app.core.DTE_Recibidos.Scrap",
    "app.core.DTE_Recibidos.ai_reader",
    "app.core.DTE_Recibidos.categorizer",
    "app.core.DTE_Recibidos.dte_loader",
    "app.core.DTE_Recibidos.excel_sync",
    "app.core.DTE_Recibidos.local_classifier",
    "app.core.DTE_Recibidos.pipeline_guard",
    "app.core.DTE_Recibidos.weekly_background_checker",
    "app.assets",
    "app.assets.update",
    "app.assets.update.__version__",

    # ── python-dotenv ──────────────────────────────────────────────
    "dotenv",
    "dotenv.main",
    "dotenv.parser",
    "dotenv.variables",
    *dotenv_h,

    # ── Tkinter / GUI ──────────────────────────────────────────────
    "tkinter",
    "tkinter.ttk",
    "tkinter.messagebox",
    "tkinter.filedialog",
    "tkinter.simpledialog",
    "PIL",
    "PIL.Image",
    "PIL.ImageTk",
    "PIL._tkinter_finder",
    *PIL_h,

    # ── SQLAlchemy ─────────────────────────────────────────────────
    "sqlalchemy",
    "sqlalchemy.orm",
    "sqlalchemy.orm.session",
    "sqlalchemy.engine",
    "sqlalchemy.dialects",
    "sqlalchemy.dialects.sqlite",
    "sqlalchemy.sql",
    "sqlalchemy.pool",
    *sqla_h,

    # ── requests ───────────────────────────────────────────────────
    "requests",
    "requests.adapters",
    "requests.auth",
    "requests.cookies",
    "requests.exceptions",
    "urllib3",
    "certifi",
    "charset_normalizer",
    "idna",
    *requests_h,

    # ── PyMuPDF (fitz) ─────────────────────────────────────────────
    "fitz",
    *fitz_h,

    # ── scikit-learn ───────────────────────────────────────────────
    "sklearn",
    "sklearn.ensemble",
    "sklearn.ensemble._forest",
    "sklearn.ensemble._gb",
    "sklearn.tree",
    "sklearn.tree._classes",
    "sklearn.pipeline",
    "sklearn.preprocessing",
    "sklearn.feature_extraction",
    "sklearn.feature_extraction.text",
    "sklearn.linear_model",
    "sklearn.utils",
    "sklearn.utils._bunch",
    "joblib",
    "threadpoolctl",
    "numpy",
    *sklearn_h,
    "scipy",
    *scipy_h,

    # ── OpenAI ─────────────────────────────────────────────────────
    "openai",
    "openai.types",
    "openai._client",
    "openai._streaming",
    "httpx",
    "httpcore",
    "anyio",
    "sniffio",
    *openai_h,

    # ── Keyring (Windows Credential Manager) ──────────────────────
    "keyring",
    "keyring.backends",
    "keyring.backends.Windows",
    "keyring.backends.fail",
    *keyring_h,

    # ── openpyxl (Excel sync) ──────────────────────────────────────
    "openpyxl",
    "openpyxl.styles",
    "openpyxl.utils",
    "openpyxl.worksheet.datavalidation",
    *openpyxl_h,

    # ── Playwright ─────────────────────────────────────────────────
    "playwright",
    "playwright.sync_api",
    "playwright.async_api",
    *collect_submodules("playwright"),

    # ── stdlib que PyInstaller a veces omite ───────────────────────
    "pathlib",
    "json",
    "re",
    "datetime",
    "threading",
    "subprocess",
    "logging",
    "logging.handlers",
    "zipfile",
    "shutil",
    "tempfile",
    "csv",
    "io",
    "os.path",
    "email",
    "email.mime",
    "email.mime.multipart",
    "email.mime.text",
]


# ── Analysis ──────────────────────────────────────────────────────────
a = Analysis(
    ["TERRALIX.py"],
    pathex=[ROOT, VENV_SP],          # <-- venv en el path
    binaries=[*dotenv_b, *openai_b, *sklearn_b, *scipy_b, *requests_b, *fitz_b, *PIL_b, *sqla_b,
              *keyring_b, *openpyxl_b, *certifi_b, *httpx_b],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=["matplotlib", "pandas", "notebook", "IPython",
              "easyocr", "cv2", "ultralytics", "transformers"],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="TERRALIX",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,          # App final sin ventana de consola
    disable_windowed_traceback=False,
    argv_emulation=False,
    icon=ICON,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name="TERRALIX",
)
