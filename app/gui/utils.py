# Script Control:
# - Role: Shared GUI helper utilities.
# - Track file: docs/SCRIPT_CONTROL.md
from tkinter import messagebox

def confirmar_salida(window):
    """Muestra un mensaje de confirmaciÃ³n antes de cerrar la app."""
    respuesta = messagebox.askyesno("Salir de Terralix", "Â¿EstÃ¡s seguro de que deseas salir de Terralix?")
    if respuesta:
        try:
            window.destroy()
            if parent_window := window.master:
                parent_window.destroy()
        except Exception:
            pass
        return True
    return False

