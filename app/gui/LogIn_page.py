# Script Control:
# - Role: Login window and access validation.
# - Track file: docs/SCRIPT_CONTROL.md
from pathlib import Path
from tkinter import Tk, Toplevel, Canvas, Entry, Button, PhotoImage, messagebox
from PIL import Image, ImageTk
import os
from dotenv import load_dotenv, set_key
from pathlib import Path
import sys

# === BASE PATH (raÃ­z del proyecto TERRALIX) ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # gui â†’ app â†’ TERRALIX
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# === FUNCIONES DE RUTAS ===
def resource_path(relative_path: str) -> str:
    """Obtiene ruta absoluta compatible con PyInstaller y entorno de desarrollo."""
    try:
        base_path = sys._MEIPASS  # carpeta temporal creada por PyInstaller
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)

def relative_to_assets(path: str) -> Path:
    """Devuelve la ruta completa de un recurso dentro de /assets."""
    return resource_path(ASSETS_PATH / Path(path))

# === CONFIGURACIÃ“N DE RUTAS CLAVE ===
DATA_PATH = BASE_DIR / "data"
ENV_PATH = DATA_PATH / "config.env"
ASSETS_PATH = BASE_DIR / "app/assets/imgs"  # âœ… tu carpeta real de imÃ¡genes

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv(resource_path(str(ENV_PATH)))

CLAVE = os.getenv("CLAVE_LOGIN")  # valor por defecto de respaldo

def open_login(parent_window=None):

    # --- Determinar si crear ventana raÃ­z o secundaria ---
    if parent_window:
        window = Toplevel(parent_window)
    else:
        window = Tk()

    # --- CONFIGURACIÃ“N DE VENTANA ---
    window.geometry("917x421")
    window.configure(bg="#FFFEFF")
    window.resizable(False, False)
    window.title("Terralix - Ingreso")

    # --- Ãcono ---
    icon_path = relative_to_assets("Terralix_Logo.ico")
    try:
        window.iconbitmap(icon_path)
    except Exception as e:
        print(f"âš ï¸ No se encontrÃ³ el icono .ico: {e}")

    # --- CANVAS PRINCIPAL ---
    canvas = Canvas(
        window,
        bg="#FFFEFF",
        height=421,
        width=917,
        bd=0,
        highlightthickness=0,
        relief="ridge"
    )
    canvas.place(x=0, y=0)

    # --- CAJA BLANCA IZQUIERDA ---
    canvas.create_rectangle(40.0, 25.0, 411.0, 396.0, fill="#FFFFFF", outline="")

    # --- LOGO ---
    logo_path = relative_to_assets("Terralix_logo.png")
    logo_img = Image.open(logo_path).resize((480, 480), Image.LANCZOS)
    logo_image = ImageTk.PhotoImage(logo_img)
    canvas.create_image(225, 220, image=logo_image)

    # --- CAMPO DE CONTRASEÑA ---
    entry_img_path = relative_to_assets("entry_contraseña.png")
    if not os.path.exists(entry_img_path):
        # Fallback por compatibilidad con nombres dañados por encoding.
        entry_img_path = relative_to_assets("entry_contraseña.png")
    entry_image_1 = PhotoImage(file=entry_img_path)
    canvas.create_image(665.0, 184.0, image=entry_image_1)

    entry_1 = Entry(
        bd=0,
        bg="#DADADA",
        fg="#000716",
        highlightthickness=0,
        show="*"
    )
    entry_1.place(x=464.0, y=167.0, width=402.0, height=32.0)

    canvas.create_text(
        459.0,
        118.0,
        anchor="nw",
        text="CONTRASEÃ‘A:",
        fill="#0F6645",
        font=("AnekLatin ExtraBold", 32 * -1)
    )

    # --- FUNCIÃ“N PARA VALIDAR Y ABRIR DASHBOARD ---
    def continuar():
        password = entry_1.get().strip()

        if password != CLAVE:
            entry_1.delete(0, 'end')
            entry_1.insert(0, "")
            messagebox.showerror("Acceso denegado", "âŒ ContraseÃ±a incorrecta.")
            return

        # Abrir la ventana principal con pestaÃ±as en la misma ventana
        from app.gui.main_app import open_main_app
        open_main_app(window)

    def salir():
        try:
            window.destroy()
        except Exception:
            window.quit()

    # --- BOTONES ---
    button_image_1 = PhotoImage(file=relative_to_assets("Salir.png"))
    button_1 = Button(
        image=button_image_1,
        borderwidth=0,
        highlightthickness=0,
        command=salir,  # BotÃ³n "Salir"
        relief="flat"
    )
    button_1.place(x=459.0, y=262.0, width=145.0, height=41.0)

    button_image_2 = PhotoImage(file=relative_to_assets("Continuar.png"))
    button_2 = Button(
        image=button_image_2,
        borderwidth=0,
        highlightthickness=0,
        command=continuar,  # BotÃ³n "Continuar"
        relief="flat"
    )
    button_2.place(x=726.0, y=262.0, width=145.0, height=41.0)

    # --- ATAJOS DE TECLADO ---
    window.bind("<Return>", lambda _e: continuar())
    window.bind("<KP_Enter>", lambda _e: continuar())
    window.bind("<Escape>", lambda _e: salir())
    entry_1.focus_set()

    # --- REFERENCIAS PARA EVITAR QUE SE LIBEREN IMÃGENES ---
    window.logo_ref = logo_image
    window.entry_ref = entry_image_1
    window.btn1_ref = button_image_1
    window.btn2_ref = button_image_2

    # --- BUCLE PRINCIPAL ---
    window.mainloop()


if __name__ == "__main__":
    open_login()

