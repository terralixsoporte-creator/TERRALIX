# Script Control:
# - Role: Login window with Supabase authentication.
# - Track file: docs/SCRIPT_CONTROL.md
from pathlib import Path
from tkinter import Tk, Toplevel, Canvas, Entry, Button, PhotoImage, Label, Checkbutton, IntVar, messagebox
from PIL import Image, ImageTk
import os
from dotenv import load_dotenv
import sys
import threading

# === BASE PATH (raiz del proyecto TERRALIX) ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))


# === FUNCIONES DE RUTAS ===
def resource_path(relative_path: str) -> str:
    """Obtiene ruta absoluta compatible con PyInstaller y entorno de desarrollo."""
    try:
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")
    return os.path.join(base_path, relative_path)


def relative_to_assets(path: str) -> Path:
    """Devuelve la ruta completa de un recurso dentro de /assets."""
    return resource_path(ASSETS_PATH / Path(path))


# === CONFIGURACION DE RUTAS CLAVE ===
DATA_PATH = BASE_DIR / "data"
ENV_PATH = DATA_PATH / "config.env"
ASSETS_PATH = BASE_DIR / "app/assets/imgs"

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv(resource_path(str(ENV_PATH)))


def open_login(parent_window=None):

    # --- Ventana raiz o secundaria ---
    if parent_window:
        window = Toplevel(parent_window)
    else:
        window = Tk()

    # --- CONFIGURACION DE VENTANA ---
    window.geometry("917x480")
    window.configure(bg="#FFFEFF")
    window.resizable(False, False)
    window.title("Terralix - Ingreso")

    # --- Icono ---
    icon_path = relative_to_assets("Terralix_Logo.ico")
    try:
        window.iconbitmap(icon_path)
    except Exception:
        pass

    # --- CANVAS PRINCIPAL ---
    canvas = Canvas(
        window,
        bg="#FFFEFF",
        height=480,
        width=917,
        bd=0,
        highlightthickness=0,
        relief="ridge",
    )
    canvas.place(x=0, y=0)

    # --- CAJA BLANCA IZQUIERDA ---
    canvas.create_rectangle(40.0, 25.0, 411.0, 455.0, fill="#FFFFFF", outline="")

    # --- LOGO ---
    logo_path = relative_to_assets("Terralix_logo.png")
    try:
        logo_img = Image.open(logo_path).resize((480, 480), Image.LANCZOS)
        logo_image = ImageTk.PhotoImage(logo_img)
        canvas.create_image(225, 240, image=logo_image)
        window.logo_ref = logo_image
    except Exception:
        pass

    # --- TITULO ---
    FONT_LABEL = ("Segoe UI Semibold", 13)
    FONT_LINK = ("Segoe UI", 10, "underline")
    GREEN = "#0F6645"
    ENTRY_BG = "#DADADA"
    BG = "#FFFEFF"

    # --- CAMPO EMAIL ---
    canvas.create_text(459.0, 115.0, anchor="nw", text="EMAIL:", fill=GREEN, font=FONT_LABEL)
    entry_email = Entry(window, bd=0, bg=ENTRY_BG, fg="#000716", highlightthickness=0, font=("Segoe UI", 12))
    entry_email.place(x=459.0, y=142.0, width=402.0, height=32.0)

    # --- CAMPO PASSWORD ---
    canvas.create_text(459.0, 190.0, anchor="nw", text="CONTRASENA:", fill=GREEN, font=FONT_LABEL)
    entry_pass = Entry(window, bd=0, bg=ENTRY_BG, fg="#000716", highlightthickness=0, show="*", font=("Segoe UI", 12))
    entry_pass.place(x=459.0, y=217.0, width=402.0, height=32.0)

    # --- CHECKBOX RECORDARME ---
    var_remember = IntVar(value=0)
    chk_remember = Checkbutton(
        window, text="Recordar credenciales", variable=var_remember,
        bg=BG, activebackground=BG, font=("Segoe UI", 10), fg="#333333",
        selectcolor="#FFFFFF",
    )
    chk_remember.place(x=456.0, y=255.0)

    # --- PRE-RELLENAR CREDENCIALES GUARDADAS ---
    try:
        from app.core.auth import load_credentials
        saved_email, saved_pass = load_credentials()
        if saved_email:
            entry_email.insert(0, saved_email)
            if saved_pass:
                entry_pass.insert(0, saved_pass)
            var_remember.set(1)
            entry_pass.focus_set()
    except Exception:
        entry_email.focus_set()

    # --- LABEL DE STATUS ---
    lbl_status = Label(window, text="", bg=BG, fg="#CC0000", font=("Segoe UI", 10))
    lbl_status.place(x=459.0, y=282.0, width=402.0, height=20.0)

    # --- FUNCIONES ---
    def _set_status(msg: str, color: str = "#CC0000"):
        lbl_status.config(text=msg, fg=color)

    def _set_buttons_state(state: str):
        btn_continuar.config(state=state)
        btn_salir.config(state=state)

    def continuar():
        email = entry_email.get().strip()
        password = entry_pass.get().strip()

        if not email:
            _set_status("Ingresa tu email.")
            entry_email.focus_set()
            return
        if not password:
            _set_status("Ingresa tu contrasena.")
            entry_pass.focus_set()
            return

        _set_status("Conectando...", color="#666666")
        _set_buttons_state("disabled")

        def _do_login():
            from app.core.auth import sign_in

            result = sign_in(email, password)

            def _handle_result():
                if result["ok"]:
                    _set_status("")
                    # Guardar o limpiar credenciales segun checkbox
                    try:
                        if var_remember.get():
                            from app.core.auth import save_credentials
                            save_credentials(email, password)
                        else:
                            from app.core.auth import clear_credentials
                            clear_credentials()
                    except Exception:
                        pass
                    from app.gui.main_app import open_main_app
                    open_main_app(window)
                else:
                    _set_status(result["error"])
                    _set_buttons_state("normal")
                    entry_pass.delete(0, "end")
                    entry_pass.focus_set()

            window.after(0, _handle_result)

        threading.Thread(target=_do_login, daemon=True).start()

    def salir():
        try:
            window.destroy()
        except Exception:
            window.quit()

    def _show_new_password_dialog(access_token: str):
        """Muestra dialogo para ingresar nueva contrasena."""
        from tkinter import Toplevel, Label as Lbl, Entry as Ent, Button as Btn

        dlg = Toplevel(window)
        dlg.title("Nueva contrasena")
        dlg.geometry("380x200")
        dlg.resizable(False, False)
        dlg.configure(bg=BG)
        dlg.grab_set()

        try:
            dlg.iconbitmap(relative_to_assets("Terralix_Logo.ico"))
        except Exception:
            pass

        Lbl(dlg, text="Ingresa tu nueva contrasena:", bg=BG, fg=GREEN,
            font=("Segoe UI Semibold", 12)).pack(pady=(20, 5))

        ent_new = Ent(dlg, show="*", font=("Segoe UI", 12), width=30)
        ent_new.pack(pady=5)

        Lbl(dlg, text="Confirmar contrasena:", bg=BG, fg=GREEN,
            font=("Segoe UI Semibold", 12)).pack(pady=(5, 5))

        ent_confirm = Ent(dlg, show="*", font=("Segoe UI", 12), width=30)
        ent_confirm.pack(pady=5)

        def _apply():
            pw1 = ent_new.get().strip()
            pw2 = ent_confirm.get().strip()
            if not pw1:
                messagebox.showwarning("Error", "Ingresa una contrasena.", parent=dlg)
                return
            if len(pw1) < 6:
                messagebox.showwarning("Error", "La contrasena debe tener al menos 6 caracteres.", parent=dlg)
                return
            if pw1 != pw2:
                messagebox.showwarning("Error", "Las contrasenas no coinciden.", parent=dlg)
                return

            from app.core.auth import update_password_with_token
            result = update_password_with_token(access_token, pw1)
            if result["ok"]:
                messagebox.showinfo("Listo", "Contrasena actualizada correctamente.\n\nYa puedes iniciar sesion.", parent=dlg)
                dlg.destroy()
                entry_pass.delete(0, "end")
                entry_pass.focus_set()
                _set_status("Contrasena cambiada. Inicia sesion.", color=GREEN)
            else:
                messagebox.showerror("Error", f"No se pudo cambiar la contrasena:\n{result['error']}", parent=dlg)

        Btn(dlg, text="Cambiar contrasena", bg=GREEN, fg="white",
            activebackground="#0A4D33", activeforeground="white",
            font=("Segoe UI Semibold", 11), command=_apply).pack(pady=15)

        ent_new.focus_set()
        dlg.bind("<Return>", lambda _: _apply())

    def olvidar_contrasena():
        email = entry_email.get().strip()
        if not email:
            _set_status("Escribe tu email arriba primero.")
            entry_email.focus_set()
            return

        _set_status("Enviando email de recuperacion...", color="#666666")

        def _do_reset():
            from app.core.auth import reset_password, start_reset_server

            # Iniciar servidor local para capturar el token del link
            def _on_token(token):
                window.after(0, lambda: _show_new_password_dialog(token))

            start_reset_server(_on_token)

            result = reset_password(email)

            def _handle():
                if result["ok"]:
                    _set_status("Email enviado. Revisa tu bandeja.", color=GREEN)
                    messagebox.showinfo(
                        "Email enviado",
                        f"Se envio un enlace de recuperacion a:\n{email}\n\n"
                        "1. Revisa tu bandeja de entrada (y spam)\n"
                        "2. Clickea el enlace del email\n"
                        "3. Se abrira una ventana para escribir tu nueva contrasena",
                    )
                else:
                    _set_status(f"Error: {result['error']}")

            window.after(0, _handle)

        threading.Thread(target=_do_reset, daemon=True).start()

    # --- BOTONES CON IMAGEN ---
    button_image_1 = PhotoImage(file=relative_to_assets("Salir.png"))
    btn_salir = Button(
        window, image=button_image_1,
        borderwidth=0, highlightthickness=0, relief="flat",
        command=salir,
    )
    btn_salir.place(x=459.0, y=315.0, width=145.0, height=41.0)

    button_image_2 = PhotoImage(file=relative_to_assets("Continuar.png"))
    btn_continuar = Button(
        window, image=button_image_2,
        borderwidth=0, highlightthickness=0, relief="flat",
        command=continuar,
    )
    btn_continuar.place(x=716.0, y=315.0, width=145.0, height=41.0)

    window.btn1_ref = button_image_1
    window.btn2_ref = button_image_2

    # --- LINK OLVIDE CONTRASENA (centrado) ---
    lbl_forgot = Label(
        window, text="Olvidaste tu contrasena?", bg=BG, fg="#1565C0",
        font=FONT_LINK, cursor="hand2",
    )
    lbl_forgot.place(x=459.0, y=370.0, width=402.0)
    lbl_forgot.bind("<Button-1>", lambda _: olvidar_contrasena())

    # --- ATAJOS DE TECLADO ---
    window.bind("<Return>", lambda _e: continuar())
    window.bind("<KP_Enter>", lambda _e: continuar())
    window.bind("<Escape>", lambda _e: salir())
    entry_email.focus_set()

    # --- BUCLE PRINCIPAL ---
    window.mainloop()


if __name__ == "__main__":
    open_login()
