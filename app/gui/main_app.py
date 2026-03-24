# Script Control:
# - Role: Main window/tab shell after login.
# - Track file: docs/SCRIPT_CONTROL.md
from tkinter import Tk
from tkinter import ttk
import tkinter as tk
from pathlib import Path
import sys
import os

from TERRALIX import check_for_updates_on_demand
from app.core.DTE_Recibidos.weekly_background_checker import start_weekly_checker

# === BASE PATH (raiz del proyecto TERRALIX) ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))


def resource_path(relative_path: str) -> str:
    """Obtiene ruta absoluta compatible con PyInstaller y entorno de desarrollo."""
    try:
        base_path = sys._MEIPASS  # type: ignore[attr-defined]
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


ASSETS_PATH = BASE_DIR / "app/assets/imgs"


def relative_to_assets(path: str) -> Path:
    """Devuelve la ruta completa de un recurso dentro de /assets."""
    return resource_path(ASSETS_PATH / Path(path))


from app.gui.actualizar_base_de_datos import create_update_tab


def open_main_app(login_window: Tk | None):
    """Convierte la ventana de login en la ventana principal con solo la pestaña de actualizacion."""
    root = login_window if login_window is not None else Tk()

    for child in root.winfo_children():
        try:
            child.destroy()
        except Exception:
            pass

    root.geometry("917x500")
    root.configure(bg="#FFFEFF")
    root.resizable(False, False)
    root.title("Terralix ERP")

    try:
        root.iconbitmap(relative_to_assets("Terralix_Logo.ico"))
    except Exception as e:
        print(f"[WARN] No se encontro el icono .ico: {e}")

    header_bg = "#0F6645"
    app_bg = "#FFFEFF"

    style = ttk.Style()
    try:
        style.theme_use("default")
    except Exception:
        pass

    style.configure("TNotebook", background=header_bg, borderwidth=0, tabmargins=[0, 0, 0, 0])
    style.configure("TNotebook.Tab", padding=[8, 2], font=("Segoe UI", 9), background=header_bg, foreground="#FFFFFF")
    style.map(
        "TNotebook.Tab",
        background=[("selected", app_bg), ("active", header_bg)],
        foreground=[("selected", header_bg), ("active", "#FFFFFF")],
    )

    notebook = ttk.Notebook(root)
    notebook.pack(fill="both", expand=True)

    menubar = tk.Menu(root)
    opciones_menu = tk.Menu(menubar, tearoff=0)
    opciones_menu.add_command(
        label="Buscar actualizaciones",
        command=lambda: check_for_updates_on_demand(root),
    )
    menubar.add_cascade(label="Opciones", menu=opciones_menu)
    root.config(menu=menubar)

    actualizar_tab = create_update_tab(notebook)
    notebook.add(actualizar_tab, text="Actualizar")
    notebook.select(actualizar_tab)

    try:
        started = start_weekly_checker()
        if started:
            print("[INFO] Chequeo semanal de DTE en segundo plano habilitado.")
    except Exception as e:
        print(f"[WARN] No se pudo iniciar el chequeo semanal en segundo plano: {e}")

    if login_window is None:
        root.mainloop()

