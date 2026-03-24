# Script Control:
# - Role: Full update pipeline UI (download, AI read, categorization, reporting).
# - Track file: docs/SCRIPT_CONTROL.md
from tkinter import Toplevel, Canvas, Button, Text, Scrollbar, END, filedialog, Frame
from tkinter import ttk
from tkinter import messagebox
from pathlib import Path
from PIL import Image, ImageTk
from dotenv import load_dotenv, set_key
import io
import sys
import os
import threading
import time
import sqlite3
import subprocess

# === BASE PATH (raÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­z del proyecto TERRALIX) ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # gui ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ app ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ TERRALIX
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# === MODULOS CREADOS ===
from app.gui.utils import confirmar_salida  # ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã¢â‚¬Å“ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ ruta absoluta correcta
from app.core.DTE_Recibidos import ai_reader as AIR
from app.core.DTE_Recibidos import categorizer as CAT
from app.core.DTE_Recibidos.pipeline_guard import (
    acquire_pipeline_lock,
    release_pipeline_lock,
)

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

# === CONFIGURACIÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN DE RUTAS CLAVE ===
DATA_PATH = BASE_DIR / "data"
ENV_PATH = DATA_PATH / "config.env"
ASSETS_PATH = BASE_DIR / "app/assets/imgs"  # ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã¢â‚¬Å“ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ tu carpeta real de imÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¡genes

# === CARGAR VARIABLES DE ENTORNO ===
load_dotenv(resource_path(str(ENV_PATH)))

# --- REDIRECCIÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN DE PRINTS A LA CONSOLA DE TKINTER ---
class ConsoleRedirect(io.StringIO):
    """Redirige los print() a la consola del widget Text."""
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def write(self, msg):
        try:
            self.text_widget.insert(END, msg)
            self.text_widget.see(END)
        except Exception:
            pass

    def flush(self):
        pass

# --- TAB (Frame) DE ACTUALIZACIÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN ---
def create_update_tab(parent):
    """Crea y devuelve un Frame con la UI de actualizaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n, listo para aÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â±adirse a un Notebook."""
    frame = Frame(parent, bg="#FFFEFF", width=917, height=500)
    frame.pack_propagate(False)

    # --- CANVAS PRINCIPAL ---
    canvas = Canvas(
        frame,
        bg="#FFFEFF",
        height=500,
        width=917,
        bd=0,
        highlightthickness=0,
        relief="ridge"
    )
    canvas.place(x=0, y=0)

    # --- LOGO IZQUIERDA ---
    logo_path = relative_to_assets("Terralix_logo.png")
    try:
        logo_img = Image.open(logo_path).resize((400, 400), Image.LANCZOS)
        logo_image = ImageTk.PhotoImage(logo_img)
        canvas.create_image(200, 200, image=logo_image)
        frame.logo_ref = logo_image
    except Exception as e:
        print("ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã‚Â¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¯Ãƒâ€šÃ‚Â¸Ãƒâ€šÃ‚Â No se pudo cargar Terralix_logo.png:", e)

    # --- BOTÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN ACTUALIZAR BASE DE DATOS ---
    update_path = relative_to_assets("Actualizar.png")
    try:
        update_img = Image.open(update_path).resize((220, 50), Image.LANCZOS)
        update_photo = ImageTk.PhotoImage(update_img)
        frame.update_ref = update_photo
    except Exception as e:
        update_photo = None
        print("ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã‚Â¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¯Ãƒâ€šÃ‚Â¸Ãƒâ€šÃ‚Â No se pudo cargar Actualizar.png:", e)

    # --- TERMINAL DE SALIDA ---
    console = Text(frame, bg="#EAEAEA", fg="#000000", font=("Consolas", 10))
    console.place(x=460, y=80, width=420, height=370)

    scrollbar = Scrollbar(frame, command=console.yview)
    scrollbar.place(x=880, y=80, height=370)
    console.config(yscrollcommand=scrollbar.set)

    console_redirect = ConsoleRedirect(console)

    # --- PROGRESO ---
    lbl_descargas = ttk.Label(frame, text="Descargas: 0/?")
    lbl_descargas.place(x=460, y=460)
    pb_descargas = ttk.Progressbar(frame, mode="indeterminate", length=200)
    pb_descargas.place(x=560, y=460, width=320)

    lbl_lectura = ttk.Label(frame, text="Lectura PDF: 0/0")
    lbl_lectura.place(x=460, y=490)
    pb_lectura = ttk.Progressbar(frame, mode="determinate", length=200, maximum=100)
    pb_lectura.place(x=560, y=490, width=320)


    # --- LÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œGICA DE ACTUALIZACIÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN ---
    def run_update():
        original_stdout = sys.stdout
        sys.stdout = console_redirect

        RUTA_PDF = str(os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "")
        DB_PATH = str(os.getenv("DB_PATH_DTE_RECIBIDOS") or "")

        console.delete("1.0", END)

        # Si no hay carpeta PDF configurada
        if not RUTA_PDF or not os.path.exists(RUTA_PDF):
            print("No se ha configurado carpeta para guardar los PDF.\n")
            RUTA_PDF = filedialog.askdirectory(title="Selecciona carpeta para guardar tus PDF")
            if not RUTA_PDF:
                print("No se selecciono ninguna carpeta. Operacion cancelada.")
                sys.stdout = original_stdout
                return

            set_key(ENV_PATH, "RUTA_PDF_DTE_RECIBIDOS", RUTA_PDF)
            print(f"Carpeta guardada en config.env:\n{RUTA_PDF}\n")
        else:
            print(f"Usando carpeta configurada:\n{RUTA_PDF}\n")

        # Verificar base de datos
        if not DB_PATH or not os.path.exists(DB_PATH):
            print("No se ha configurado la ruta de la base de datos.\n")
            carpeta = filedialog.askdirectory(
                title="Selecciona la carpeta para guardar la base de datos"
            )

            if not carpeta:
                print("No se selecciono ninguna carpeta. Operacion cancelada.")
                sys.stdout = original_stdout
                return

            # Forzamos el nombre fijo
            DB_PATH = os.path.join(carpeta, "DteRecibidos_db.db")

            # Guardamos en el .env
            set_key(ENV_PATH, "DB_PATH_DTE_RECIBIDOS", DB_PATH)

            print(f"Ruta de base de datos guardada en config.env:\n{DB_PATH}\n")

        # Asegura que los procesos usen las rutas actuales en runtime
        os.environ["RUTA_PDF_DTE_RECIBIDOS"] = RUTA_PDF
        os.environ["DB_PATH_DTE_RECIBIDOS"] = DB_PATH

        print(
            """
Iniciando actualizacion de base de datos.
Secuencia: 1) Descarga SII -> 2) Lectura IA -> 3) Categorizacion
No cierres esta ventana hasta que termine.
"""
        )

        def _env_float(var_name: str, default: float) -> float:
            try:
                return float(os.getenv(var_name, str(default)))
            except Exception:
                return default

        def _generar_reporte_sin_clasificar(db_path: str) -> str | None:
            if not db_path or not os.path.isfile(db_path):
                print("[WARN] No se pudo generar reporte: DB no encontrada.")
                return None

            rows = []
            try:
                with sqlite3.connect(db_path) as con:
                    con.row_factory = sqlite3.Row
                    cur = con.cursor()

                    detalle_cols = {r[1] for r in cur.execute("PRAGMA table_info(detalle);").fetchall()}
                    documentos_cols = {r[1] for r in cur.execute("PRAGMA table_info(documentos);").fetchall()}
                    if not detalle_cols:
                        print("[WARN] No existe tabla 'detalle'; se omite reporte.")
                        return None

                    select_fields = [
                        "COALESCE(d.id_doc, '') AS id_doc",
                        "COALESCE(d.descripcion, '') AS descripcion" if "descripcion" in detalle_cols else "'' AS descripcion",
                        "COALESCE(d.codigo, '') AS codigo" if "codigo" in detalle_cols else "'' AS codigo",
                        "COALESCE(d.linea, 0) AS linea" if "linea" in detalle_cols else "0 AS linea",
                        "COALESCE(d.categoria, '') AS categoria" if "categoria" in detalle_cols else "'' AS categoria",
                        "COALESCE(d.subcategoria, '') AS subcategoria" if "subcategoria" in detalle_cols else "'' AS subcategoria",
                        "COALESCE(doc.razon_social, '') AS razon_social" if "razon_social" in documentos_cols else "'' AS razon_social",
                        "COALESCE(doc.giro, '') AS giro" if "giro" in documentos_cols else "'' AS giro",
                    ]

                    where_clauses = []
                    if "needs_review" in detalle_cols:
                        where_clauses.append("COALESCE(d.needs_review, 0) = 1")
                    if "categoria" in detalle_cols:
                        where_clauses.append(
                            "(d.categoria IS NULL OR TRIM(d.categoria) = '' OR UPPER(TRIM(d.categoria)) = 'SIN_CLASIFICAR')"
                        )

                    if not where_clauses:
                        print("[WARN] No hay campos de clasificacion en 'detalle'; se omite reporte.")
                        return None

                    where_sql = " OR ".join(where_clauses)
                    order_sql = "ORDER BY d.id_doc, d.linea" if "linea" in detalle_cols else "ORDER BY d.id_doc"

                    q = f"""
                        SELECT
                            {", ".join(select_fields)}
                        FROM detalle d
                        LEFT JOIN documentos doc ON doc.id_doc = d.id_doc
                        WHERE {where_sql}
                        {order_sql}
                    """
                    rows = cur.execute(q).fetchall()
            except Exception as e:
                print(f"[WARN] No se pudo consultar detalle sin clasificar: {e}")
                return None

            report_dir = DATA_PATH / "reportes"
            os.makedirs(report_dir, exist_ok=True)
            report_path = report_dir / f"reporte_sin_clasificar_{time.strftime('%Y%m%d_%H%M%S')}.txt"

            try:
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write("REPORTE DE DETALLES SIN CLASIFICAR\n")
                    f.write(f"Generado: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write("=" * 80 + "\n\n")

                    if not rows:
                        f.write("No se encontraron detalles sin clasificar.\n")
                    else:
                        current_doc = None
                        for r in rows:
                            id_doc = (r["id_doc"] or "").strip()
                            if id_doc != current_doc:
                                if current_doc is not None:
                                    f.write("\n")
                                f.write(f"id_doc: {id_doc or '(sin id_doc)'}\n")
                                f.write(f"razon_social: {(r['razon_social'] or '').strip() or '(vacio)'}\n")
                                f.write(f"giro: {(r['giro'] or '').strip() or '(vacio)'}\n")
                                f.write("detalle:\n")
                                current_doc = id_doc

                            linea = r["linea"]
                            codigo = (r["codigo"] or "").strip()
                            descripcion = (r["descripcion"] or "").strip() or "(sin descripcion)"

                            linea_txt = str(linea) if linea not in (None, "") else "-"
                            pref = f"  - linea {linea_txt}"
                            if codigo:
                                pref += f" | codigo: {codigo}"
                            f.write(pref + "\n")
                            f.write(f"    {descripcion}\n")
            except Exception as e:
                print(f"[WARN] No se pudo escribir reporte de sin clasificar: {e}")
                return None

            return str(report_path)

        def _abrir_reporte(path_reporte: str) -> None:
            if not path_reporte:
                return
            try:
                if hasattr(os, "startfile"):
                    os.startfile(path_reporte)  # type: ignore[attr-defined]
                else:
                    print(f"[INFO] Reporte generado en: {path_reporte}")
            except Exception as e:
                print(f"[WARN] No se pudo abrir el reporte automaticamente: {e}")

        def set_download_progress(done: int | None, total: int | None, extra: str = ""):
            def _update():
                if total and total > 0 and done is not None:
                    pb_descargas.stop()
                    pb_descargas.config(mode="determinate", maximum=total)
                    pb_descargas["value"] = done
                    lbl_descargas.config(text=f"Descargas: {done}/{total} {extra}")
                else:
                    if pb_descargas["mode"] != "indeterminate":
                        pb_descargas.config(mode="indeterminate")
                    if int(float(pb_descargas["value"])) == 0:
                        pb_descargas.start(10)
                    lbl_descargas.config(text=f"Descargas: {done or 0}/? {extra}")
            frame.after(0, _update)

        def finish_download_progress(extra: str = "(finalizado)"):
            def _update():
                pb_descargas.stop()
                pb_descargas.config(mode="determinate", maximum=1)
                pb_descargas["value"] = 1
                lbl_descargas.config(text=f"Descargas: completado {extra}")
            frame.after(0, _update)

        def set_read_progress(done: int, total: int):
            def _update():
                total_safe = max(total, 1)
                pct = int(done * 100 / total_safe)
                pb_lectura.config(mode="determinate", maximum=100)
                pb_lectura["value"] = pct
                lbl_lectura.config(text=f"Lectura PDF: {done}/{total}")
            frame.after(0, _update)

        def run_ai_reader_batch(ruta_pdf: str, db_path: str):
            print("\n[2/3] Iniciando lectura IA de PDFs...\n")
            targets = AIR._collect_target_files(file_arg=None, dir_arg=ruta_pdf)
            if not targets:
                print("No se encontraron PDFs para procesar con IA.")
                set_read_progress(0, 0)
                return

            db_path_resolved = db_path if (db_path and os.path.isfile(db_path)) else AIR._get_db_path()
            debug_mode = os.getenv("AI_DETALLE_DEBUG", "false").lower() == "true"
            ai_throttle_s = max(0.0, _env_float("GUI_AI_THROTTLE_SECONDS", 0.25))

            parsed_targets = []
            candidate_ids = []
            for pdf in targets:
                tipo_fn, rut_fn, folio_fn = AIR._infer_from_filename(pdf)
                id_doc = AIR.DL.build_id_doc(tipo_fn, rut_fn, folio_fn)
                parsed_targets.append((pdf, id_doc))
                candidate_ids.append(id_doc)

            existing_ids = set()
            preload_failed = False
            if candidate_ids and os.path.isfile(db_path_resolved):
                try:
                    with sqlite3.connect(db_path_resolved) as con:
                        cur = con.cursor()
                        # Evita el limite de variables de SQLite consultando por lotes.
                        chunk_size = 500
                        for i in range(0, len(candidate_ids), chunk_size):
                            chunk = candidate_ids[i:i + chunk_size]
                            placeholders = ",".join("?" for _ in chunk)
                            cur.execute(
                                f"SELECT id_doc FROM documentos WHERE id_doc IN ({placeholders})",
                                chunk,
                            )
                            existing_ids.update(row[0] for row in cur.fetchall())
                    print(f"[IA] Documentos ya existentes en DB (pre-carga): {len(existing_ids)}")
                except Exception as e:
                    preload_failed = True
                    print(f"[WARN] No se pudo pre-cargar IDs existentes desde DB: {e}")
                    print("[WARN] Se usara verificacion individual por documento.")

            total = len(parsed_targets)
            for idx, (pdf, id_doc) in enumerate(parsed_targets, start=1):

                already_exists = id_doc in existing_ids
                if not already_exists and preload_failed:
                    already_exists = AIR._doc_exists_in_db(id_doc, db_path_resolved)

                if already_exists:
                    print(f"[SKIP] Ya existe en DB -> {id_doc} [{idx}/{total}]")
                    set_read_progress(idx, total)
                    continue

                print(f"[IA] Leyendo ({idx}/{total}): {pdf}")
                print(f"[IA] id_doc: {id_doc}")

                max_retries = 4
                success = False

                for attempt in range(1, max_retries + 1):
                    if attempt > 1:
                        print(f"[IA] Reintentando ({attempt}/{max_retries})...")

                    res = AIR.read_one_pdf_with_ai(pdf, debug=debug_mode)
                    if res.get("ok"):
                        print(f"[OK] Guardado en DB: {res.get('doc_id')} - items: {len(res.get('items', []))}")
                        existing_ids.add(id_doc)
                        success = True
                        break

                    print(f"[ERROR] IA no pudo procesar {pdf}: {res.get('error')}")
                    if attempt < max_retries:
                        wait_s = 3 * attempt
                        print(f"[IA] Esperando {wait_s}s antes de reintentar...")
                        time.sleep(wait_s)

                if not success:
                    print(f"[WARN] No se pudo procesar despues de {max_retries} intentos -> {pdf}")

                set_read_progress(idx, total)
                if ai_throttle_s > 0:
                    time.sleep(ai_throttle_s)

            print("\n[OK] Lectura IA finalizada.\n")

        def run_categorizer_batch():
            gui_batch_sleep = max(0.0, _env_float("GUI_REVIEW_BATCH_SLEEP", 0.05))
            CAT.BATCH_SLEEP = gui_batch_sleep
            print(f"\n[3/3] Iniciando categorizacion contable (batch_sleep={gui_batch_sleep:.2f}s)...\n")
            try:
                CAT.main()
            except SystemExit as e:
                code = e.code if isinstance(e.code, int) else 1
                if code not in (0, None):
                    raise RuntimeError(f"categorizer finalizo con codigo {code}")
            print("\n[OK] Categorizacion finalizada.\n")

        def ejecutar_pipeline(ruta_pdf: str, db_path: str):
            lock_acquired = False
            try:
                lock_acquired = acquire_pipeline_lock(blocking=False)
                if not lock_acquired:
                    print("[WARN] Ya hay otro proceso DTE ejecutandose. Intenta nuevamente en unos minutos.")
                    return

                from app.core.DTE_Recibidos.Scrap import scrapear

                print("[1/3] Iniciando descarga desde SII...\n")

                def dl_cb(done: int | None, total: int | None, info: str = ""):
                    set_download_progress(done, total, info)

                descargas_incompletas = scrapear(ruta_pdf, progress_cb=dl_cb)
                finish_download_progress()

                if descargas_incompletas:
                    print("\n[WARN] Descargas incompletas detectadas. Se procesaran los PDFs disponibles.\n")
                else:
                    print("\n[OK] Descarga de DTE completada.\n")

                run_ai_reader_batch(ruta_pdf, db_path)
                run_categorizer_batch()
                reporte_path = _generar_reporte_sin_clasificar(db_path)
                if reporte_path:
                    print(f"[OK] Reporte de sin clasificar generado: {reporte_path}")
                    _abrir_reporte(reporte_path)
                print("\n[OK] Flujo completo terminado correctamente.\n")

            except ModuleNotFoundError:
                print("[ERROR] No se encontro un modulo requerido del pipeline (Scrap/AI/Categorizer).")
                print("Verifica dependencias instaladas y estructura del proyecto.")
            except Exception as e:
                print(f"[ERROR] Fallo durante la actualizacion:\n{e}")
            finally:
                if lock_acquired:
                    release_pipeline_lock()

                def _restore_ui():
                    try:
                        update_btn.config(state="normal")
                        ai_btn.config(state="normal")
                        repair_btn.config(state="normal")
                    except Exception:
                        pass
                    sys.stdout = original_stdout
                frame.after(0, _restore_ui)

        update_btn.config(state="disabled")

        pb_descargas.stop()
        pb_descargas.config(mode="indeterminate")
        pb_descargas["value"] = 0
        pb_lectura.config(mode="determinate", maximum=100)
        pb_lectura["value"] = 0
        lbl_descargas.config(text="Descargas: 0/?")
        lbl_lectura.config(text="Lectura PDF: 0/0")

        hilo_pipeline = threading.Thread(target=ejecutar_pipeline, args=(RUTA_PDF, DB_PATH), daemon=True)
        hilo_pipeline.start()

    # --- BOTON DE ACTUALIZAR ---
    update_btn = Button(
        frame,
        image=update_photo,
        borderwidth=0,
        highlightthickness=0,
        relief="flat",
        bg="#FFFEFF",
        activebackground="#FFFEFF",
        command=run_update
    )
    update_btn.place(x=100, y=340)
    def run_ai_manual():
        ruta_base = os.getenv("RUTA_PDF_DTE_RECIBIDOS") or ""
        start_file = filedialog.askopenfilename(
            title="Selecciona un PDF para leer con IA",
            initialdir=ruta_base if os.path.isdir(ruta_base) else None,
            filetypes=[("PDF", "*.pdf")]
        )
        if not start_file:
            print("ÃƒÆ’Ã‚Â¢Ãƒâ€šÃ‚ÂÃƒâ€¦Ã¢â‚¬â„¢ No seleccionaste PDF inicial.")
            return
        carpeta = os.path.dirname(start_file)
        archivos = sorted([f for f in os.listdir(carpeta) if f.lower().endswith(".pdf")])
        idx = archivos.index(os.path.basename(start_file)) if os.path.basename(start_file) in archivos else 0
        dbg = os.getenv("AI_DETALLE_DEBUG", "false").lower() == "true"
        while idx < len(archivos):
            pdf = os.path.join(carpeta, archivos[idx])
            print(f"\nÃƒÆ’Ã‚Â°Ãƒâ€¦Ã‚Â¸Ãƒâ€šÃ‚Â§Ãƒâ€šÃ‚Â  IA leyendo: {pdf}")
            res = AIR.read_one_pdf_with_ai(pdf, debug=dbg)
            if res.get("ok"):
                items = res.get("items", [])
                print(f"ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã¢â‚¬Å“ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ Guardado en BD {res.get('doc_id')} con {len(items)} ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­tems")
            else:
                print(f"ÃƒÆ’Ã‚Â¢Ãƒâ€šÃ‚ÂÃƒâ€¦Ã¢â‚¬â„¢ IA no pudo procesar {pdf}: {res.get('error')}")
            idx += 1
            if idx < len(archivos):
                if not messagebox.askyesno("Continuar", f"ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¿Procesar el siguiente PDF?\n\n{archivos[idx]}"):
                    break

    ai_btn = Button(
        frame,
        text="Leer con IA (manual)",
        borderwidth=0,
        highlightthickness=0,
        relief="flat",
        bg="#FFFEFF",
        activebackground="#FFFEFF",
        command=run_ai_manual
    )
    ai_btn.place(x=100, y=480, width=220, height=30)

    def _find_python_executable() -> str | None:
        candidates = [
            sys.executable,
            str((BASE_DIR / "terr" / "Scripts" / "python.exe").resolve()),
        ]
        for c in candidates:
            if c and os.path.isfile(c):
                return c
        return None

    def _run_command_stream(cmd: list[str]) -> int:
        try:
            print("[CMD] " + " ".join(cmd))
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            if p.stdout is not None:
                for line in p.stdout:
                    print(line, end="")
            return int(p.wait())
        except Exception as e:
            print(f"[ERROR] No se pudo ejecutar comando: {e}")
            return 1

    def run_repair_db():
        original_stdout = sys.stdout
        sys.stdout = console_redirect
        console.delete("1.0", END)

        RUTA_PDF = str(os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "")
        DB_PATH = str(os.getenv("DB_PATH_DTE_RECIBIDOS") or "")

        # Si no hay carpeta PDF configurada
        if not RUTA_PDF or not os.path.exists(RUTA_PDF):
            print("No se ha configurado carpeta para los PDF.\n")
            RUTA_PDF = filedialog.askdirectory(title="Selecciona carpeta donde estan tus PDF")
            if not RUTA_PDF:
                print("No se selecciono carpeta. Operacion cancelada.")
                sys.stdout = original_stdout
                return
            set_key(ENV_PATH, "RUTA_PDF_DTE_RECIBIDOS", RUTA_PDF)
            print(f"Carpeta PDF guardada en config.env:\n{RUTA_PDF}\n")

        # Verificar base de datos
        if not DB_PATH or not os.path.exists(DB_PATH):
            print("No se ha configurado la ruta de la base de datos.\n")
            carpeta = filedialog.askdirectory(
                title="Selecciona la carpeta donde esta/ira DteRecibidos_db.db"
            )
            if not carpeta:
                print("No se selecciono carpeta. Operacion cancelada.")
                sys.stdout = original_stdout
                return
            DB_PATH = os.path.join(carpeta, "DteRecibidos_db.db")
            set_key(ENV_PATH, "DB_PATH_DTE_RECIBIDOS", DB_PATH)
            print(f"Ruta DB guardada en config.env:\n{DB_PATH}\n")

        if not messagebox.askyesno(
            "Confirmar reparacion",
            (
                "Se releeran TODOS los PDF locales para reconstruir detalle en la DB.\n\n"
                "Tambien se pueden eliminar documentos cuyo PDF local no exista.\n"
                "Este proceso puede tardar varios minutos.\n\n"
                "Deseas continuar?"
            ),
        ):
            print("Reparacion cancelada por usuario.")
            sys.stdout = original_stdout
            return

        os.environ["RUTA_PDF_DTE_RECIBIDOS"] = RUTA_PDF
        os.environ["DB_PATH_DTE_RECIBIDOS"] = DB_PATH

        update_btn.config(state="disabled")
        ai_btn.config(state="disabled")
        repair_btn.config(state="disabled")

        pb_descargas.stop()
        pb_descargas.config(mode="indeterminate")
        pb_descargas["value"] = 0
        pb_descargas.start(10)
        lbl_descargas.config(text="Reparacion DB: en curso")

        pb_lectura.stop()
        pb_lectura.config(mode="indeterminate")
        pb_lectura["value"] = 0
        pb_lectura.start(10)
        lbl_lectura.config(text="Relectura PDF: en curso")

        def ejecutar_reparacion_local(ruta_pdf: str, db_path: str):
            lock_acquired = False
            try:
                lock_acquired = acquire_pipeline_lock(blocking=False)
                if not lock_acquired:
                    print("[WARN] Ya hay otro proceso DTE ejecutandose. Intenta nuevamente en unos minutos.")
                    return

                py = _find_python_executable()
                if not py:
                    raise RuntimeError("No se encontro ejecutable de Python.")

                repair_script = (BASE_DIR / "tools" / "releer_y_reparar_dte.py").resolve()
                audit_script = (BASE_DIR / "tools" / "auditar_consistencia_facturas.py").resolve()
                if not repair_script.is_file():
                    raise RuntimeError(f"No existe script de reparacion: {repair_script}")
                if not audit_script.is_file():
                    raise RuntimeError(f"No existe script de auditoria: {audit_script}")

                report_dir = DATA_PATH / "reportes"
                os.makedirs(report_dir, exist_ok=True)
                ts = time.strftime("%Y%m%d_%H%M%S")
                repair_report = report_dir / f"reparacion_gui_{ts}.txt"
                audit_report = report_dir / f"coherencia_post_reparacion_{ts}.txt"
                audit_csv = report_dir / f"coherencia_post_reparacion_{ts}.csv"

                print("\n[REPAIR] Iniciando reparacion local de DB (sin descargar desde SII)...\n")
                cmd_repair = [
                    py,
                    str(repair_script),
                    "--db", db_path,
                    "--all-docs",
                    "--delete-missing-local-pdf",
                    "--apply",
                    "--report", str(repair_report),
                ]
                rc = _run_command_stream(cmd_repair)
                if rc != 0:
                    raise RuntimeError(f"releer_y_reparar_dte finalizo con codigo {rc}")

                print("\n[REPAIR] Ejecutando auditoria de coherencia post-reparacion...\n")
                cmd_audit = [
                    py,
                    str(audit_script),
                    "--db", db_path,
                    "--check-description-vs-pdf",
                    "--report", str(audit_report),
                    "--csv", str(audit_csv),
                ]
                rc2 = _run_command_stream(cmd_audit)
                if rc2 != 0:
                    raise RuntimeError(f"auditar_consistencia_facturas finalizo con codigo {rc2}")

                print("\n[OK] Reparacion finalizada.\n")
                print(f"[OK] Reporte reparacion: {repair_report}")
                print(f"[OK] Reporte coherencia: {audit_report}")
                print(f"[OK] CSV coherencia (Excel): {audit_csv}")
                _abrir_reporte(str(repair_report))
                _abrir_reporte(str(audit_report))
            except Exception as e:
                print(f"[ERROR] Fallo durante reparacion de DB:\n{e}")
            finally:
                if lock_acquired:
                    release_pipeline_lock()

                def _restore_ui():
                    try:
                        pb_descargas.stop()
                        pb_descargas.config(mode="determinate", maximum=1)
                        pb_descargas["value"] = 1
                        lbl_descargas.config(text="Reparacion DB: finalizado")

                        pb_lectura.stop()
                        pb_lectura.config(mode="determinate", maximum=1)
                        pb_lectura["value"] = 1
                        lbl_lectura.config(text="Relectura PDF: finalizado")

                        update_btn.config(state="normal")
                        ai_btn.config(state="normal")
                        repair_btn.config(state="normal")
                    except Exception:
                        pass
                    sys.stdout = original_stdout
                frame.after(0, _restore_ui)

        hilo_repair = threading.Thread(
            target=ejecutar_reparacion_local,
            args=(RUTA_PDF, DB_PATH),
            daemon=True,
        )
        hilo_repair.start()

    repair_btn = Button(
        frame,
        text="Reparar lectura BD",
        borderwidth=0,
        highlightthickness=0,
        relief="flat",
        bg="#FFFEFF",
        activebackground="#FFFEFF",
        command=run_repair_db
    )
    repair_btn.place(x=100, y=440, width=220, height=30)

    return frame

# --- INTERFAZ DE ACTUALIZACIÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN COMO VENTANA (compatibilidad anterior) ---
def open_update_page(parent_window=None):
    window = Toplevel()
    window.geometry("917x500")
    window.configure(bg="#FFFEFF")
    window.resizable(False, False)
    window.title("Terralix - ActualizaciÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³n de Base de Datos")
    window.protocol("WM_DELETE_WINDOW", lambda: confirmar_salida(window))

    try:
        window.iconbitmap(relative_to_assets("Terralix_Logo.ico"))
    except Exception as e:
        print("ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã‚Â¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã‚Â¯Ãƒâ€šÃ‚Â¸Ãƒâ€šÃ‚Â No se encontrÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â³ el icono .ico:", e)

    tab = create_update_tab(window)
    tab.pack(fill="both", expand=True)

    window.mainloop()

if __name__ == "__main__":
    open_update_page()

