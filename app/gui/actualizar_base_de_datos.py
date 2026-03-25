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

# === BASE PATH (raÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â­z del proyecto TERRALIX) ===
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # gui ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ app ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ TERRALIX
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

# === MODULOS CREADOS ===
from app.gui.utils import confirmar_salida  # ÃƒÆ’Ã‚Â¢Ãƒâ€¦Ã¢â‚¬Å“ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ ruta absoluta correcta
# Importaciones pesadas de pipeline se hacen en demanda para no bloquear login.

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
        print("No se pudo cargar Terralix_logo.png:", e)

    # --- BOTÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN ACTUALIZAR BASE DE DATOS ---
    update_path = relative_to_assets("Actualizar.png")
    try:
        update_img = Image.open(update_path).resize((220, 50), Image.LANCZOS)
        update_photo = ImageTk.PhotoImage(update_img)
        frame.update_ref = update_photo
    except Exception as e:
        update_photo = None
        print("No se pudo cargar Actualizar.png:", e)

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
            from app.core.DTE_Recibidos import ai_reader as AIR

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
            from app.core.DTE_Recibidos import categorizer as CAT

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
            from app.core.DTE_Recibidos.pipeline_guard import (
                acquire_pipeline_lock,
                release_pipeline_lock,
            )

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

    # =================================================================
    # BOTONES NUEVOS: Excel sync + Configurar rutas
    # =================================================================

    def run_export_excel():
        """Exporta la DB a Excel para revision manual."""
        original_stdout = sys.stdout
        sys.stdout = console_redirect
        console.delete("1.0", END)

        DB_PATH = str(os.getenv("DB_PATH_DTE_RECIBIDOS") or "")
        if not DB_PATH or not os.path.isfile(DB_PATH):
            print("[ERROR] No se encontro la base de datos.")
            print("Configura la ruta en 'Configurar Rutas'.")
            sys.stdout = original_stdout
            return

        excel_dir = os.path.dirname(DB_PATH)
        excel_path = os.path.join(excel_dir, "DteRecibidos_revision.xlsx")

        def _do_export():
            from app.core.DTE_Recibidos.excel_sync import (
                export_to_excel,
                backfill_denormalized_columns,
            )

            try:
                print("[EXPORT] Rellenando columnas desnormalizadas...")
                n_fill = backfill_denormalized_columns(DB_PATH)
                if n_fill > 0:
                    print(f"[EXPORT] {n_fill} filas actualizadas con datos de documentos.")

                print("[EXPORT] Generando Excel...")
                result = export_to_excel(DB_PATH, excel_path)
                if result.get("ok"):
                    print(f"\n[OK] Excel generado exitosamente:")
                    print(f"  Ruta: {result['path']}")
                    print(f"  Filas detalle: {result['n_rows']}")
                    print(f"  Catalogo: {result['n_catalogo']} combinaciones")
                    print(f"  Documentos: {result['n_documentos']}")
                    print("\n  Filas AMARILLAS = necesitan revision")
                    print("  Filas ROJAS = sin clasificar")
                    print("\n  Edita las columnas 'categoria', 'subcategoria', 'tipo_gasto'")
                    print("  Luego usa 'Importar Excel' para aplicar los cambios.\n")

                    # Abrir Excel automaticamente
                    try:
                        if hasattr(os, "startfile"):
                            os.startfile(excel_path)
                    except Exception:
                        pass
                else:
                    print(f"[ERROR] {result.get('error', 'Error desconocido')}")
            except Exception as e:
                print(f"[ERROR] Fallo al exportar: {e}")
            finally:
                sys.stdout = original_stdout

        threading.Thread(target=_do_export, daemon=True).start()

    def run_import_excel():
        """Importa cambios del Excel, actualiza DB y reentrena modelo."""
        original_stdout = sys.stdout
        sys.stdout = console_redirect
        console.delete("1.0", END)

        DB_PATH = str(os.getenv("DB_PATH_DTE_RECIBIDOS") or "")
        if not DB_PATH or not os.path.isfile(DB_PATH):
            print("[ERROR] No se encontro la base de datos.")
            sys.stdout = original_stdout
            return

        excel_dir = os.path.dirname(DB_PATH)
        default_excel = os.path.join(excel_dir, "DteRecibidos_revision.xlsx")

        excel_path = filedialog.askopenfilename(
            title="Selecciona el Excel con las correcciones",
            initialdir=excel_dir,
            initialfile="DteRecibidos_revision.xlsx",
            filetypes=[("Excel", "*.xlsx")],
        )
        if not excel_path:
            print("No se selecciono archivo. Operacion cancelada.")
            sys.stdout = original_stdout
            return

        def _do_import():
            from app.core.DTE_Recibidos.excel_sync import sync_and_retrain

            try:
                print(f"[IMPORT] Leyendo: {excel_path}")
                print("[IMPORT] Detectando cambios...\n")

                result = sync_and_retrain(excel_path, DB_PATH)

                if not result.get("ok"):
                    print(f"[ERROR] {result.get('error', 'Error desconocido')}")
                    return

                n = result["n_changed"]
                if n == 0:
                    print("[OK] No se detectaron cambios en el Excel.")
                    print("Las categorias coinciden con la base de datos.")
                    return

                print(f"[OK] {n} filas actualizadas en la base de datos:\n")
                for ch in result.get("changes", [])[:20]:
                    print(f"  {ch['id_det']}: {ch['categoria']} > {ch['subcategoria']} > {ch['tipo_gasto']}")
                if n > 20:
                    print(f"  ... y {n - 20} mas.")

                for w in result.get("warnings", []):
                    print(f"  [WARN] {w}")

                if result.get("retrained"):
                    tr = result.get("train_result", {})
                    print(f"\n[ML] Modelo reentrenado:")
                    print(f"  Muestras: {tr.get('n_samples', '?')}")
                    print(f"  Clases: {tr.get('n_classes', '?')}")
                    acc = tr.get('cat_cv_accuracy')
                    if acc:
                        print(f"  Accuracy categorias: {acc:.1%}")
                    print("\n[OK] El modelo ahora aprende de tus correcciones.")
                else:
                    print("\n[WARN] No se pudo reentrenar el modelo.")

            except Exception as e:
                print(f"[ERROR] Fallo al importar: {e}")
            finally:
                sys.stdout = original_stdout

        threading.Thread(target=_do_import, daemon=True).start()

    def run_change_paths():
        """Permite cambiar las rutas de PDF y base de datos."""
        original_stdout = sys.stdout
        sys.stdout = console_redirect
        console.delete("1.0", END)

        current_pdf = str(os.getenv("RUTA_PDF_DTE_RECIBIDOS") or "(no configurado)")
        current_db = str(os.getenv("DB_PATH_DTE_RECIBIDOS") or "(no configurado)")

        print("=== CONFIGURACION DE RUTAS ===\n")
        print(f"Carpeta PDF actual: {current_pdf}")
        print(f"Base de datos actual: {current_db}\n")

        # Pedir nueva carpeta PDF
        change_pdf = messagebox.askyesno(
            "Cambiar carpeta PDF",
            f"Carpeta PDF actual:\n{current_pdf}\n\nDeseas cambiarla?",
        )
        if change_pdf:
            new_pdf = filedialog.askdirectory(
                title="Selecciona nueva carpeta para PDFs",
                initialdir=current_pdf if os.path.isdir(current_pdf) else None,
            )
            if new_pdf:
                set_key(str(ENV_PATH), "RUTA_PDF_DTE_RECIBIDOS", new_pdf)
                os.environ["RUTA_PDF_DTE_RECIBIDOS"] = new_pdf
                print(f"[OK] Carpeta PDF actualizada: {new_pdf}")
            else:
                print("[INFO] Carpeta PDF sin cambios.")

        # Pedir nueva ruta DB
        change_db = messagebox.askyesno(
            "Cambiar base de datos",
            f"Base de datos actual:\n{current_db}\n\nDeseas cambiarla?",
        )
        if change_db:
            new_db_dir = filedialog.askdirectory(
                title="Selecciona carpeta para la base de datos",
                initialdir=os.path.dirname(current_db) if os.path.isdir(os.path.dirname(current_db)) else None,
            )
            if new_db_dir:
                new_db = os.path.join(new_db_dir, "DteRecibidos_db.db")
                set_key(str(ENV_PATH), "DB_PATH_DTE_RECIBIDOS", new_db)
                os.environ["DB_PATH_DTE_RECIBIDOS"] = new_db
                print(f"[OK] Base de datos actualizada: {new_db}")
            else:
                print("[INFO] Base de datos sin cambios.")

        print("\n=== Rutas actuales ===")
        print(f"  PDF: {os.getenv('RUTA_PDF_DTE_RECIBIDOS', '(no configurado)')}")
        print(f"  DB:  {os.getenv('DB_PATH_DTE_RECIBIDOS', '(no configurado)')}")
        sys.stdout = original_stdout

    # Expose functions for menu bar access
    frame.run_export_excel = run_export_excel
    frame.run_import_excel = run_import_excel
    frame.run_change_paths = run_change_paths

    return frame

# --- INTERFAZ DE ACTUALIZACIÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…â€œN COMO VENTANA (compatibilidad anterior) ---
def open_update_page(parent_window=None):
    window = Toplevel()
    window.geometry("917x500")
    window.configure(bg="#FFFEFF")
    window.resizable(False, False)
    window.title("Terralix - Actualizacion de Base de Datos")
    window.protocol("WM_DELETE_WINDOW", lambda: confirmar_salida(window))

    try:
        window.iconbitmap(relative_to_assets("Terralix_Logo.ico"))
    except Exception as e:
        print("No se encontro el icono .ico:", e)

    tab = create_update_tab(window)
    tab.pack(fill="both", expand=True)

    window.mainloop()

if __name__ == "__main__":
    open_update_page()
