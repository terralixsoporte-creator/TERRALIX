# Script Control:
# - Role: Main window/tab shell after login.
# - Track file: docs/SCRIPT_CONTROL.md
from tkinter import Tk
from tkinter import ttk
import tkinter as tk
from pathlib import Path
import sys
import os

from app.core.DTE_Recibidos.weekly_background_checker import start_weekly_checker
from app.core.paths import get_logs_dir

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


def _log_main(message: str) -> None:
    """Registra eventos de la ventana principal sin usar consola."""
    try:
        log_path = get_logs_dir() / "main_app.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(message + "\n")
    except Exception:
        pass


def _open_manual():
    """Abre el manual de usuario .docx con la aplicacion predeterminada."""
    manual_path = BASE_DIR / "data" / "Manual_Terralix_ERP.pdf"
    if manual_path.is_file():
        os.startfile(str(manual_path))
    else:
        from tkinter import messagebox
        messagebox.showwarning(
            "Manual no encontrado",
            f"No se encontro el manual en:\n{manual_path}",
        )


def open_main_app(login_window: Tk | None):
    """Convierte la ventana de login en la ventana principal de Terralix."""
    from app.gui.actualizar_base_de_datos import create_update_tab
    from app.gui.aplicaciones_tab import create_applications_tab
    from app.gui.inventario_tab import create_inventory_tab

    root = login_window if login_window is not None else Tk()

    for child in root.winfo_children():
        try:
            child.destroy()
        except Exception:
            pass

    root.geometry("1100x650")
    root.configure(bg="#FFFEFF")
    root.minsize(917, 500)
    root.resizable(True, True)
    root.title("Terralix ERP")

    try:
        root.iconbitmap(relative_to_assets("Terralix_Logo.ico"))
    except Exception as e:
        _log_main(f"[WARN] No se encontro el icono .ico: {e}")

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

    actualizar_tab = create_update_tab(notebook)
    notebook.add(actualizar_tab, text="Actualizar")
    inventario_tab = create_inventory_tab(notebook)
    notebook.add(inventario_tab, text="Inventario")
    aplicaciones_tab = create_applications_tab(notebook)
    notebook.add(aplicaciones_tab, text="Aplicaciones")
    notebook.select(actualizar_tab)

    menubar = tk.Menu(root)

    excel_menu = tk.Menu(menubar, tearoff=0)
    excel_menu.add_command(
        label="Exportar Excel",
        command=lambda: actualizar_tab.run_export_excel(),
    )
    excel_menu.add_command(
        label="Importar Excel",
        command=lambda: actualizar_tab.run_import_excel(),
    )
    menubar.add_cascade(label="Excel", menu=excel_menu)

    inventario_menu = tk.Menu(menubar, tearoff=0)
    inventario_menu.add_command(
        label="Refrescar Inventario",
        command=lambda: inventario_tab.run_inventory_refresh(),
    )
    menubar.add_cascade(label="Inventario", menu=inventario_menu)

    aplicaciones_menu = tk.Menu(menubar, tearoff=0)
    aplicaciones_menu.add_command(
        label="Refrescar Aplicaciones",
        command=lambda: aplicaciones_tab.run_applications_refresh(),
    )
    menubar.add_cascade(label="Aplicaciones", menu=aplicaciones_menu)

    opciones_menu = tk.Menu(menubar, tearoff=0)
    opciones_menu.add_command(
        label="Configurar Rutas (PDF / Base de Datos)",
        command=lambda: actualizar_tab.run_change_paths(),
    )
    menubar.add_cascade(label="Opciones", menu=opciones_menu)

    ayuda_menu = tk.Menu(menubar, tearoff=0)
    ayuda_menu.add_command(
        label="Manual de usuario",
        command=lambda: _open_manual(),
    )
    menubar.add_cascade(label="Ayuda", menu=ayuda_menu)
    root.config(menu=menubar)

    try:
        started = start_weekly_checker()
        if started:
            _log_main("[INFO] Chequeo semanal de DTE en segundo plano habilitado.")
    except Exception as e:
        _log_main(f"[WARN] No se pudo iniciar el chequeo semanal en segundo plano: {e}")

    if login_window is None:
        root.mainloop()
