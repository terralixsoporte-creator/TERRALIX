# TERRALIX.spec - build local/offline (sin TUFUP)

import os
import sys
from pathlib import Path

from PyInstaller.building.build_main import Analysis, COLLECT, EXE, PYZ
from PyInstaller.building.datastruct import Tree
from PyInstaller.utils.hooks import collect_data_files, collect_submodules

BASE_DIR = Path.cwd()

# Prefer project venv packages (terr) when building with a global interpreter.
VENV_SITE = BASE_DIR / "terr" / "Lib" / "site-packages"
if VENV_SITE.exists():
    sys.path.insert(0, str(VENV_SITE))

datas = []

cfg = BASE_DIR / "data" / "config.env"
if cfg.exists():
    datas.append((str(cfg), "data"))

stjson = BASE_DIR / "data" / "storage_state.json"
if stjson.exists():
    datas.append((str(stjson), "data"))

assets_dir = BASE_DIR / "app" / "assets"
if assets_dir.exists():
    for p in assets_dir.rglob("*"):
        if p.is_file():
            rel = p.relative_to(BASE_DIR)
            datas.append((str(p), str(rel.parent)))

yolo = BASE_DIR / "app" / "core" / "DTE_Recibidos" / "YOLOv11Model.pt"
if yolo.exists():
    datas.append((str(yolo), "app/core/DTE_Recibidos"))

try:
    datas += collect_data_files("playwright")
except Exception:
    pass

browsers_tree = []
local = os.environ.get("LOCALAPPDATA", "")
if local:
    ms_playwright = Path(local) / "ms-playwright"
    if ms_playwright.exists():
        browsers_tree = [Tree(str(ms_playwright), prefix="ms-playwright")]

hiddenimports = []
for pkg in ("fitz", "playwright", "dotenv", "openai", "PIL", "sqlalchemy", "requests"):
    try:
        hiddenimports += collect_submodules(pkg)
    except Exception:
        pass
hiddenimports = sorted(set(hiddenimports))

a = Analysis(
    ["TERRALIX.py"],
    pathex=[str(BASE_DIR), str(VENV_SITE)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=["hooks"],
    hooksconfig={},
    runtime_hooks=["hooks/runtime_playwright_browsers_path.py"],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="TERRALIX",
    console=False,
    icon=str(BASE_DIR / "Terralix_Logo.ico") if (BASE_DIR / "Terralix_Logo.ico").exists() else None,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    *browsers_tree,
    strip=False,
    upx=False,
    name="TERRALIX",
)


