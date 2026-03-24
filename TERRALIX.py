# Script Control:
# - Role: App entry point and GUI bootstrap.
# - Track file: docs/SCRIPT_CONTROL.md
import os
import sys
from pathlib import Path

from dotenv import load_dotenv, set_key
import tkinter as tk
from tkinter import messagebox, filedialog


if getattr(sys, "frozen", False):
    candidates = [
        Path(getattr(sys, "_MEIPASS", Path.cwd())) / "_internal" / "ms-playwright",
        Path(getattr(sys, "_MEIPASS", Path.cwd())) / "ms-playwright",
        Path(getattr(sys, "_MEIPASS", Path.cwd())) / "_internal" / "_internal" / "ms-playwright",
    ]
    for p in candidates:
        if p.exists():
            os.environ["PLAYWRIGHT_BROWSERS_PATH"] = str(p)
            break


BASE_DIR = Path(__file__).resolve().parent


def resource_path(relative_path: str) -> str:
    try:
        base_path = sys._MEIPASS  # type: ignore[attr-defined]
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


DATA_PATH = BASE_DIR / "data"
ENV_PATH = DATA_PATH / "config.env"
DATA_PATH.mkdir(parents=True, exist_ok=True)
ENV_PATH.touch(exist_ok=True)
load_dotenv(resource_path(str(ENV_PATH)))


try:
    from app.assets.update.__version__ import __version__ as APP_VERSION
except Exception:
    APP_VERSION = "0.0.0"


def _clean_env_path(value: str) -> str:
    return (value or "").strip().strip('"').strip("'")


def _ensure_first_run_paths() -> bool:
    """
    En primera ejecucion (o cuando falta config), pide:
    - carpeta de PDFs
    - carpeta para DB (se guarda como DteRecibidos_db.db)
    """
    pdf_dir = _clean_env_path(os.getenv("RUTA_PDF_DTE_RECIBIDOS", ""))
    db_path = _clean_env_path(os.getenv("DB_PATH_DTE_RECIBIDOS", ""))

    need_pdf = not pdf_dir or not os.path.isdir(pdf_dir)
    db_parent = os.path.dirname(db_path) if db_path else ""
    need_db = not db_path or not db_parent or not os.path.isdir(db_parent)

    if not need_pdf and not need_db:
        return True

    chooser = tk.Tk()
    chooser.withdraw()
    try:
        chooser.attributes("-topmost", True)
    except Exception:
        pass

    try:
        messagebox.showinfo(
            "Configuracion inicial",
            "Para la primera ejecucion debes configurar la carpeta de PDFs y la ubicacion de la base de datos.",
            parent=chooser,
        )

        if need_pdf:
            picked_pdf_dir = filedialog.askdirectory(
                parent=chooser,
                title="Selecciona la carpeta donde se guardaran los PDF",
                initialdir=str(DATA_PATH),
            )
            if not picked_pdf_dir:
                messagebox.showerror("Configuracion incompleta", "Debes seleccionar la carpeta de PDF para continuar.", parent=chooser)
                return False
            pdf_dir = picked_pdf_dir
            set_key(str(ENV_PATH), "RUTA_PDF_DTE_RECIBIDOS", pdf_dir)
            os.environ["RUTA_PDF_DTE_RECIBIDOS"] = pdf_dir

        if need_db:
            picked_db_dir = filedialog.askdirectory(
                parent=chooser,
                title="Selecciona la carpeta donde se guardara la base de datos",
                initialdir=str(DATA_PATH),
            )
            if not picked_db_dir:
                messagebox.showerror("Configuracion incompleta", "Debes seleccionar la carpeta de base de datos para continuar.", parent=chooser)
                return False
            db_path = os.path.join(picked_db_dir, "DteRecibidos_db.db")
            set_key(str(ENV_PATH), "DB_PATH_DTE_RECIBIDOS", db_path)
            os.environ["DB_PATH_DTE_RECIBIDOS"] = db_path

        try:
            if pdf_dir:
                os.makedirs(pdf_dir, exist_ok=True)
            if db_path:
                os.makedirs(os.path.dirname(db_path), exist_ok=True)
        except Exception:
            pass

        messagebox.showinfo(
            "Configuracion guardada",
            f"PDF: {pdf_dir}\nDB: {db_path}",
            parent=chooser,
        )
        return True
    finally:
        chooser.destroy()


def check_for_updates_on_demand(parent_window=None):
    """Updater deshabilitado en build local/offline."""
    def _show():
        messagebox.showinfo(
            "Actualizaciones",
            f"Version local v{APP_VERSION}. Actualizaciones automaticas deshabilitadas.",
        )

    if parent_window is not None:
        try:
            parent_window.after(0, _show)
            return
        except Exception:
            pass
    _show()


def launch_gui():
    from app.gui.LogIn_page import open_login
    open_login()


def main():
    if not _ensure_first_run_paths():
        return
    launch_gui()


if __name__ == "__main__":
    main()
