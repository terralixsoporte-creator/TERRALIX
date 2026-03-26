"""
Generador del Manual de Usuario de Terralix ERP.
Ejecutar con: terr/Scripts/python.exe build_manual.py
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    PageBreak, HRFlowable, KeepTogether
)
from reportlab.platypus.tableofcontents import TableOfContents
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfgen import canvas as pdfcanvas
from reportlab.platypus import BaseDocTemplate, Frame, PageTemplate

OUTPUT_PATH = "data/Manual_Terralix_ERP.pdf"

# ── Colores corporativos ──────────────────────────────────────────────────────
GREEN        = colors.HexColor("#0F6645")
GREEN_LIGHT  = colors.HexColor("#E8F5EE")
GREEN_MED    = colors.HexColor("#B2D8C4")
GRAY_LIGHT   = colors.HexColor("#F5F5F5")
GRAY_MED     = colors.HexColor("#D9D9D9")
GRAY_DARK    = colors.HexColor("#555555")
WHITE        = colors.white
BLACK        = colors.black
YELLOW       = colors.HexColor("#FFF9C4")
RED_LIGHT    = colors.HexColor("#FFEBEE")
BLUE_LIGHT   = colors.HexColor("#E3F2FD")

PAGE_W, PAGE_H = A4
MARGIN = 2 * cm

# ── Estilos ───────────────────────────────────────────────────────────────────
styles = getSampleStyleSheet()

style_title_page = ParagraphStyle(
    "TitlePage", parent=styles["Title"],
    fontName="Helvetica-Bold", fontSize=28,
    textColor=GREEN, alignment=TA_CENTER, spaceAfter=10
)
style_subtitle = ParagraphStyle(
    "Subtitle", parent=styles["Normal"],
    fontName="Helvetica", fontSize=13,
    textColor=GRAY_DARK, alignment=TA_CENTER, spaceAfter=6
)
style_h1 = ParagraphStyle(
    "H1", parent=styles["Heading1"],
    fontName="Helvetica-Bold", fontSize=15,
    textColor=WHITE, spaceAfter=6, spaceBefore=18,
    backColor=GREEN, borderPad=6, leading=20
)
style_h2 = ParagraphStyle(
    "H2", parent=styles["Heading2"],
    fontName="Helvetica-Bold", fontSize=12,
    textColor=GREEN, spaceAfter=4, spaceBefore=12,
    borderColor=GREEN, borderWidth=0, borderPad=0
)
style_h3 = ParagraphStyle(
    "H3", parent=styles["Heading3"],
    fontName="Helvetica-BoldOblique", fontSize=11,
    textColor=GRAY_DARK, spaceAfter=3, spaceBefore=8
)
style_body = ParagraphStyle(
    "Body", parent=styles["Normal"],
    fontName="Helvetica", fontSize=10,
    textColor=BLACK, spaceAfter=5, leading=14,
    alignment=TA_JUSTIFY
)
style_body_left = ParagraphStyle(
    "BodyLeft", parent=style_body,
    alignment=TA_LEFT
)
style_bullet = ParagraphStyle(
    "Bullet", parent=styles["Normal"],
    fontName="Helvetica", fontSize=10,
    textColor=BLACK, spaceAfter=3, leading=13,
    leftIndent=16, firstLineIndent=-10
)
style_code = ParagraphStyle(
    "Code", parent=styles["Code"],
    fontName="Courier", fontSize=9,
    textColor=colors.HexColor("#333333"),
    backColor=GRAY_LIGHT, borderPad=4, spaceAfter=4,
    leading=13
)
style_note = ParagraphStyle(
    "Note", parent=styles["Normal"],
    fontName="Helvetica-Oblique", fontSize=9,
    textColor=GRAY_DARK, backColor=YELLOW,
    borderPad=5, spaceAfter=6, leading=13
)
style_warning = ParagraphStyle(
    "Warning", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=10,
    textColor=colors.HexColor("#B71C1C"), backColor=RED_LIGHT,
    borderPad=5, spaceAfter=6, leading=13
)
style_tip = ParagraphStyle(
    "Tip", parent=styles["Normal"],
    fontName="Helvetica-Oblique", fontSize=9,
    textColor=colors.HexColor("#0D47A1"), backColor=BLUE_LIGHT,
    borderPad=5, spaceAfter=6, leading=13
)
style_toc_h1 = ParagraphStyle(
    "TOCH1", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=11,
    textColor=GREEN, spaceBefore=4, spaceAfter=2, leftIndent=0
)
style_toc_h2 = ParagraphStyle(
    "TOCH2", parent=styles["Normal"],
    fontName="Helvetica", fontSize=10,
    textColor=BLACK, spaceBefore=1, spaceAfter=1, leftIndent=14
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def h1(text):
    return Paragraph(f"  {text}", style_h1)

def h2(text):
    return Paragraph(text, style_h2)

def h3(text):
    return Paragraph(text, style_h3)

def body(text):
    return Paragraph(text, style_body)

def body_left(text):
    return Paragraph(text, style_body_left)

def bullet(text):
    return Paragraph(f"\u2022  {text}", style_bullet)

def note(text):
    return Paragraph(f"<b>Nota:</b>  {text}", style_note)

def warning(text):
    return Paragraph(f"<b>Advertencia:</b>  {text}", style_warning)

def tip(text):
    return Paragraph(f"<b>Consejo:</b>  {text}", style_tip)

def space(h=0.25):
    return Spacer(1, h * cm)

def hr():
    return HRFlowable(width="100%", thickness=1, color=GREEN_MED, spaceAfter=4)

def simple_table(headers, rows, col_widths=None):
    """Genera tabla con encabezado verde y filas alternas."""
    data = [[Paragraph(f"<b>{h}</b>", ParagraphStyle("TH", fontName="Helvetica-Bold",
             fontSize=9, textColor=WHITE, alignment=TA_CENTER)) for h in headers]]
    for i, row in enumerate(rows):
        data.append([Paragraph(str(cell), ParagraphStyle("TD", fontName="Helvetica",
                     fontSize=9, textColor=BLACK, leading=12)) for cell in row])

    avail_w = PAGE_W - 2 * MARGIN
    if col_widths is None:
        col_widths = [avail_w / len(headers)] * len(headers)

    ts = TableStyle([
        ("BACKGROUND",  (0, 0), (-1, 0),  GREEN),
        ("TEXTCOLOR",   (0, 0), (-1, 0),  WHITE),
        ("ALIGN",       (0, 0), (-1, -1), "LEFT"),
        ("VALIGN",      (0, 0), (-1, -1), "MIDDLE"),
        ("FONTNAME",    (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",    (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, GRAY_LIGHT]),
        ("GRID",        (0, 0), (-1, -1), 0.4, GRAY_MED),
        ("TOPPADDING",  (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING",(0, 0), (-1, -1), 5),
    ])
    t = Table(data, colWidths=col_widths)
    t.setStyle(ts)
    return t

# ── Numeración de páginas ─────────────────────────────────────────────────────

def _add_page_number(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(GRAY_DARK)
    page_num = canvas.getPageNumber()
    canvas.drawString(MARGIN, 1.2 * cm, "Terralix ERP — Manual de Usuario")
    canvas.drawRightString(PAGE_W - MARGIN, 1.2 * cm, f"Pagina {page_num}")
    canvas.setStrokeColor(GREEN_MED)
    canvas.setLineWidth(0.5)
    canvas.line(MARGIN, 1.5 * cm, PAGE_W - MARGIN, 1.5 * cm)
    canvas.restoreState()


# ── Documento ─────────────────────────────────────────────────────────────────
doc = SimpleDocTemplate(
    OUTPUT_PATH,
    pagesize=A4,
    leftMargin=MARGIN, rightMargin=MARGIN,
    topMargin=2 * cm, bottomMargin=2 * cm,
    title="Manual de Usuario Terralix ERP",
    author="Terralix ERP",
    subject="Documentacion de usuario v1.3.0",
)

story = []

# ══════════════════════════════════════════════════════════════════════════════
# PORTADA
# ══════════════════════════════════════════════════════════════════════════════
story.append(Spacer(1, 3 * cm))
story.append(Paragraph("TERRALIX ERP", style_title_page))
story.append(Spacer(1, 0.5 * cm))
story.append(Paragraph("Manual de Usuario", style_subtitle))
story.append(Spacer(1, 0.3 * cm))
story.append(Paragraph("Version 1.3.0  |  Marzo 2026", style_subtitle))
story.append(Spacer(1, 0.3 * cm))
story.append(Paragraph("Agricola Las Tipuanas SPA", style_subtitle))
story.append(Spacer(1, 2 * cm))

cover_table = Table(
    [[Paragraph(
        "Gestion automatizada de Documentos Tributarios Electronicos (DTE) "
        "para la agricultura chilena. Descarga, lectura IA, clasificacion contable, "
        "inventario de insumos y planificacion de aplicaciones de campo.",
        ParagraphStyle("Cover", fontName="Helvetica", fontSize=11,
                       textColor=GRAY_DARK, alignment=TA_CENTER, leading=16)
    )]],
    colWidths=[PAGE_W - 4 * cm]
)
cover_table.setStyle(TableStyle([
    ("BACKGROUND",   (0, 0), (-1, -1), GREEN_LIGHT),
    ("ALIGN",        (0, 0), (-1, -1), "CENTER"),
    ("TOPPADDING",   (0, 0), (-1, -1), 20),
    ("BOTTOMPADDING",(0, 0), (-1, -1), 20),
    ("LEFTPADDING",  (0, 0), (-1, -1), 20),
    ("RIGHTPADDING", (0, 0), (-1, -1), 20),
    ("ROUNDEDCORNERS", (0, 0), (-1, -1), [8, 8, 8, 8]),
]))
story.append(cover_table)
story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════════════════
# INDICE
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("Contenido"))
story.append(space(0.3))

toc_items = [
    ("1. Introduccion", False),
    ("2. Requisitos del sistema", False),
    ("3. Instalacion y primera ejecucion", False),
    ("   3.1 Instalacion", True),
    ("   3.2 Primera ejecucion", True),
    ("4. Inicio de sesion", False),
    ("   4.1 Iniciar sesion con email y contrasena", True),
    ("   4.2 Recordar credenciales", True),
    ("   4.3 Recuperacion de contrasena", True),
    ("5. Pantalla principal", False),
    ("   5.1 Pestana Actualizar", True),
    ("   5.2 Pestana Inventario", True),
    ("   5.3 Pestana Aplicaciones", True),
    ("   5.4 Barra de menu superior", True),
    ("6. Actualizar Base de Datos (pipeline completo)", False),
    ("   Etapa 1: Descarga desde SII", True),
    ("   Etapa 2: Lectura IA de PDFs", True),
    ("   Etapa 3: Categorizacion contable", True),
    ("   Etapa 4: Backfill de codigos (INSUMOS_AGRICOLAS)", True),
    ("   Etapa 5: Backfill de unidades", True),
    ("   Etapa 6: Sincronizacion automatica de inventario", True),
    ("7. Exportar Excel para revision", False),
    ("8. Correccion manual en Excel", False),
    ("9. Importar Excel y reentrenar modelo", False),
    ("10. Modulo de Inventario", False),
    ("    10.1 Vista de stock actual", True),
    ("    10.2 Registrar uso manual", True),
    ("    10.3 Edicion directa de celdas", True),
    ("    10.4 Sincronizar desde DTE", True),
    ("11. Modulo de Aplicaciones de campo", False),
    ("    11.1 Crear una nueva aplicacion", True),
    ("    11.2 Agregar productos quimicos", True),
    ("    11.3 Calendario mensual", True),
    ("    11.4 Gestionar aplicaciones existentes", True),
    ("12. Chequeo semanal automatico en segundo plano", False),
    ("13. Configurar rutas", False),
    ("14. Conexion con Power BI / Excel externo", False),
    ("15. Estructura de la base de datos", False),
    ("16. Catalogo de costos", False),
    ("17. Preguntas frecuentes", False),
]

for text, is_sub in toc_items:
    st = style_toc_h2 if is_sub else style_toc_h1
    story.append(Paragraph(text, st))

story.append(PageBreak())

# ══════════════════════════════════════════════════════════════════════════════
# 1. INTRODUCCION
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("1. Introduccion"))
story.append(body(
    "Terralix ERP es una aplicacion de escritorio desarrollada en Python para la gestion "
    "automatizada de Documentos Tributarios Electronicos (DTE) recibidos por "
    "Agricola Las Tipuanas SPA. Combina descarga automatica desde el SII, lectura "
    "inteligente de PDFs con GPT-4o, clasificacion contable con Machine Learning local "
    "y un modulo completo de inventario de insumos agricolas."
))
story.append(space())
story.append(body("La aplicacion cubre el ciclo completo de gestion de facturas y boletas:"))
story.append(bullet("Descarga automatica desde el Servicio de Impuestos Internos (SII)"))
story.append(bullet("Lectura inteligente de PDFs mediante vision artificial (GPT-4o)"))
story.append(bullet("Categorizacion contable automatica con modelo de Machine Learning local"))
story.append(bullet("Completado automatico de codigos de insumos y unidades de medida"))
story.append(bullet("Sincronizacion de inventario teorico desde los DTE comprados"))
story.append(bullet("Exportacion a Excel para revision manual y correccion"))
story.append(bullet("Aprendizaje continuo: las correcciones manuales reentrenan el modelo"))
story.append(bullet("Modulo de aplicaciones de campo con calendario mensual"))
story.append(bullet("Conexion directa con Power BI y Excel para analisis y reportes"))
story.append(bullet("Chequeo semanal automatico en segundo plano"))
story.append(space())
story.append(note(
    "El objetivo es que con un solo click se actualice toda la base de datos, "
    "permitiendote enfocarte en el analisis y la gestion, no en la carga manual de datos."
))

# ══════════════════════════════════════════════════════════════════════════════
# 2. REQUISITOS
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("2. Requisitos del sistema"))
story.append(simple_table(
    ["Componente", "Requisito"],
    [
        ["Sistema operativo", "Windows 10 / 11 (64 bits)"],
        ["Python", "3.11 o superior (incluido en el instalador)"],
        ["RAM", "4 GB minimo, 8 GB recomendado"],
        ["Disco", "500 MB para la app + espacio para PDFs"],
        ["Internet", "Requerido para descarga SII y lectura IA"],
        ["Navegador", "Chromium (instalado automaticamente por Playwright)"],
        ["Excel", "Microsoft Excel o LibreOffice Calc (para revision)"],
        ["Cuenta Terralix", "Email y contrasena proporcionados por el administrador"],
    ],
    col_widths=[7 * cm, 9.7 * cm]
))

# ══════════════════════════════════════════════════════════════════════════════
# 3. INSTALACION
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("3. Instalacion y primera ejecucion"))
story.append(h2("3.1 Instalacion"))
story.append(body(
    "Descarga el instalador TERRALIX_Setup.exe y ejecutalo con doble clic. "
    "El asistente instalara la aplicacion en <b>C:\\Program Files\\Terralix ERP\\</b> "
    "y creara un acceso directo en el escritorio y el menu Inicio."
))
story.append(space(0.2))
story.append(note(
    "Los datos del usuario (base de datos, PDFs, config.env, logs) se guardan en "
    "%APPDATA%\\Terralix ERP\\ y no se eliminan al desinstalar la aplicacion."
))

story.append(h2("3.2 Primera ejecucion"))
story.append(body(
    "En la primera ejecucion la aplicacion mostrara un asistente de configuracion "
    "inicial que te pedira definir dos rutas esenciales:"
))
story.append(space(0.1))
story.append(simple_table(
    ["Ruta", "Descripcion", "Ejemplo"],
    [
        ["Carpeta de PDFs", "Donde se guardaran los PDF descargados del SII", "C:\\Users\\...\\DTE"],
        ["Carpeta de base de datos", "Donde se creara DteRecibidos_db.db", "C:\\Users\\...\\Base"],
    ],
    col_widths=[4.5 * cm, 8 * cm, 4.2 * cm]
))
story.append(space(0.2))
story.append(body(
    "Estas rutas se guardan en config.env y pueden cambiarse posteriormente desde "
    "<b>Opciones > Configurar Rutas (PDF / Base de Datos)</b>."
))

# ══════════════════════════════════════════════════════════════════════════════
# 4. INICIO DE SESION
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("4. Inicio de sesion"))
story.append(body(
    "Al abrir Terralix ERP aparece la ventana de ingreso. El sistema utiliza "
    "autenticacion segura a traves de Supabase con email y contrasena."
))

story.append(h2("4.1 Iniciar sesion con email y contrasena"))
story.append(bullet("Escribe tu <b>email</b> en el campo correspondiente."))
story.append(bullet("Escribe tu <b>contrasena</b>."))
story.append(bullet("Presiona el boton <b>Continuar</b> o la tecla <b>Enter</b>."))
story.append(bullet("Mientras se verifica, aparecera el mensaje <i>Conectando...</i>"))
story.append(bullet("Si las credenciales son incorrectas, se muestra un mensaje de error en rojo."))
story.append(space(0.2))
story.append(note(
    "Las credenciales (email y contrasena) son asignadas por el administrador "
    "de Terralix. Si no las tienes, contacta al equipo de soporte."
))

story.append(h2("4.2 Recordar credenciales"))
story.append(body(
    "Marca la casilla <b>'Recordar credenciales'</b> antes de iniciar sesion. "
    "La proxima vez que abras la app, el email y la contrasena se completaran "
    "automaticamente. Las credenciales se guardan de forma segura en el "
    "Administrador de credenciales de Windows (no en texto plano)."
))

story.append(h2("4.3 Recuperacion de contrasena"))
story.append(body(
    "Si olvidaste tu contrasena, usa el enlace <b>'Olvidaste tu contrasena?'</b>:"
))
story.append(bullet("1. Escribe tu email en el campo correspondiente."))
story.append(bullet("2. Haz clic en el enlace azul '?Olvidaste tu contrasena?'"))
story.append(bullet("3. Revisa tu bandeja de entrada (y la carpeta spam)."))
story.append(bullet("4. Haz clic en el enlace del email de recuperacion."))
story.append(bullet("5. Se abrira automaticamente una ventana para escribir tu nueva contrasena."))
story.append(bullet("6. Ingresa y confirma la nueva contrasena (minimo 6 caracteres)."))
story.append(space(0.2))
story.append(simple_table(
    ["Atajo", "Accion"],
    [
        ["Enter / KP_Enter", "Iniciar sesion (equivale a presionar Continuar)"],
        ["Escape", "Salir de la aplicacion"],
    ],
    col_widths=[5 * cm, 11.7 * cm]
))

# ══════════════════════════════════════════════════════════════════════════════
# 5. PANTALLA PRINCIPAL
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("5. Pantalla principal"))
story.append(body(
    "Tras iniciar sesion, la ventana de login se transforma en la aplicacion principal. "
    "La pantalla es <b>redimensionable</b> y se adapta automaticamente al tamano de la ventana. "
    "El diseno utiliza un panel con tres pestanas en la parte superior."
))

story.append(h2("5.1 Pestana Actualizar"))
story.append(body(
    "Es la pestana principal. Contiene a la izquierda el logo de Terralix y el boton "
    "<b>Actualizar</b> que ejecuta el pipeline completo de 6 etapas. "
    "A la derecha hay una consola de texto que muestra el progreso en tiempo real "
    "y dos barras de progreso: una para las descargas y otra para la lectura de PDFs."
))

story.append(h2("5.2 Pestana Inventario"))
story.append(body(
    "Modulo de gestion de inventario de insumos agricolas. Muestra el stock actual "
    "de cada producto, permite registrar usos manuales y editar informacion del catalogo. "
    "Ver seccion 10 para el detalle completo."
))

story.append(h2("5.3 Pestana Aplicaciones"))
story.append(body(
    "Modulo de planificacion de aplicaciones de campo (tratamientos fitosanitarios, "
    "fertilizaciones, etc.). Incluye un calendario mensual y la posibilidad de "
    "registrar los productos quimicos utilizados en cada aplicacion. "
    "Ver seccion 11 para el detalle completo."
))

story.append(h2("5.4 Barra de menu superior"))
story.append(simple_table(
    ["Menu", "Opcion", "Funcion"],
    [
        ["Excel", "Exportar Excel", "Genera DteRecibidos_revision.xlsx con todos los datos para revision"],
        ["Excel", "Importar Excel", "Lee correcciones del Excel, actualiza DB y reentrena el modelo ML"],
        ["Inventario", "Refrescar Inventario", "Sincroniza el inventario desde los DTE (equivale al boton Refrescar del tab)"],
        ["Aplicaciones", "Refrescar Aplicaciones", "Recarga el calendario y la lista de aplicaciones de campo"],
        ["Opciones", "Configurar Rutas (PDF / Base de Datos)", "Permite cambiar la carpeta de PDFs y la ruta de la base de datos"],
        ["Ayuda", "Manual de usuario", "Abre este manual en formato PDF"],
    ],
    col_widths=[3.2 * cm, 5.5 * cm, 8 * cm]
))

# ══════════════════════════════════════════════════════════════════════════════
# 6. PIPELINE
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("6. Actualizar Base de Datos (pipeline completo)"))
story.append(body(
    "El boton <b>Actualizar</b> ejecuta las 6 etapas del pipeline de forma secuencial. "
    "Cada etapa se muestra en la consola con el formato [N/6]. "
    "No cierres la ventana hasta que aparezca el mensaje "
    "<i>'Flujo completo terminado correctamente.'</i>"
))
story.append(space(0.2))
story.append(warning(
    "Si otro proceso del pipeline ya esta en ejecucion (por ejemplo el chequeo "
    "semanal automatico), el boton Actualizar mostrara un aviso y no iniciara "
    "hasta que el proceso anterior termine."
))

story.append(h2("Etapa 1: Descarga desde SII"))
story.append(body(
    "Se conecta automaticamente al Servicio de Impuestos Internos usando las credenciales "
    "configuradas en config.env (RUT y CLAVE). Un navegador Chromium invisible navega "
    "por el portal del SII, pagina por pagina, y descarga los PDF de facturas y boletas "
    "recibidas que aun no estan en la carpeta local."
))
story.append(bullet("La barra <i>Descargas</i> muestra el progreso (N / total)."))
story.append(bullet("Si ya existe el PDF, se omite para evitar duplicados."))
story.append(bullet("Si la descarga falla, se continua con los archivos disponibles."))

story.append(h2("Etapa 2: Lectura IA de PDFs"))
story.append(body(
    "Cada PDF nuevo se convierte a imagenes y se envia a GPT-4o (vision) que extrae "
    "la informacion estructurada: emisor, folio, fecha, montos y el detalle linea por "
    "linea (descripcion, cantidad, precio unitario, monto). Los datos se guardan en "
    "las tablas <b>documentos</b> y <b>detalle</b>."
))
story.append(bullet("La barra <i>Lectura PDF</i> muestra el porcentaje completado."))
story.append(bullet("Si un PDF falla, se reintenta hasta 4 veces con espera entre reintentos."))
story.append(bullet("Los documentos ya existentes en la DB se omiten automaticamente."))

story.append(h2("Etapa 3: Categorizacion contable"))
story.append(body(
    "Cada linea de detalle se clasifica con un sistema de 3 capas:"
))
story.append(simple_table(
    ["Capa", "Descripcion", "Confianza tipica"],
    [
        ["1 - Reglas locales", "Patrones de proveedor/descripcion (ej: autopista → PEAJES)", "90-95%"],
        ["2 - Mantenedor proveedores", "Si el proveedor ya fue clasificado con alta confianza, se reutiliza", "92%"],
        ["3 - Modelo ML local", "TF-IDF + LinearSVC entrenado con datos historicos. Sin internet, sin costo API.", "Variable"],
    ],
    col_widths=[4 * cm, 10 * cm, 2.7 * cm]
))
story.append(space(0.2))
story.append(body(
    "Al finalizar se genera un reporte de detalles sin clasificar (needs_review=1 o "
    "categoria='SIN_CLASIFICAR')."
))

story.append(h2("Etapa 4: Backfill de codigos (INSUMOS_AGRICOLAS)"))
story.append(body(
    "Despues de clasificar, vuelve a leer los PDFs de la categoria INSUMOS_AGRICOLAS "
    "para extraer el codigo del producto (ej: codigo de agroquimico o fertilizante). "
    "Esta informacion es critica para el modulo de inventario. Solo procesa los "
    "documentos que aun no tienen codigo asignado."
))

story.append(h2("Etapa 5: Backfill de unidades"))
story.append(body(
    "Completa la columna <i>unidad</i> (litros, kg, saco, etc.) para los INSUMOS_AGRICOLAS "
    "que no la tienen, usando los datos de debug JSON guardados localmente durante la "
    "lectura IA. No requiere llamadas a la API de OpenAI."
))

story.append(h2("Etapa 6: Sincronizacion automatica de inventario"))
story.append(body(
    "Sincroniza el inventario teorico de insumos a partir de los datos de detalle. "
    "Solo considera documentos tipo <b>Factura</b> y productos con codigo asignado "
    "(ignora codigo='-'). Actualiza el catalogo de insumos y los movimientos de entrada "
    "en el modulo de Inventario."
))

# ══════════════════════════════════════════════════════════════════════════════
# 7. EXPORTAR EXCEL
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("7. Exportar Excel para revision"))
story.append(body(
    "Ve al menu <b>Excel > Exportar Excel</b>. La aplicacion genera el archivo "
    "<b>DteRecibidos_revision.xlsx</b> en la misma carpeta de la base de datos."
))
story.append(space(0.2))
story.append(simple_table(
    ["Hoja", "Contenido", "Editable"],
    [
        ["detalle", "Todas las lineas de detalle con su clasificacion", "Si (columnas de categoria)"],
        ["catalogo", "Las combinaciones validas de categoria/subcategoria/tipo_gasto", "No (solo referencia)"],
        ["documentos", "Datos de los documentos: emisor, fecha, montos", "No (solo referencia)"],
        ["stock", "Stock actual de insumos por codigo", "No (solo referencia)"],
        ["entradas", "Movimientos de entrada de insumos desde facturas", "No (solo referencia)"],
    ],
    col_widths=[3 * cm, 9.5 * cm, 4.2 * cm]
))
story.append(space(0.3))

story.append(h2("Codificacion de colores"))
story.append(simple_table(
    ["Color", "Significado"],
    [
        ["Amarillo", "Filas marcadas como 'needs_review' - el modelo no esta seguro de la clasificacion"],
        ["Rojo claro", "Filas SIN_CLASIFICAR - no se pudo determinar la categoria automaticamente"],
        ["Verde", "Filas corregidas manualmente (origen=MANUAL) - ya validadas por el usuario"],
        ["Gris (texto)", "Columnas de solo lectura (no editables)"],
    ],
    col_widths=[3.5 * cm, 13.2 * cm]
))
story.append(space(0.2))
story.append(body(
    "La columna 'categoria' tiene un <b>dropdown</b> con las categorias validas del catalogo."
))

# ══════════════════════════════════════════════════════════════════════════════
# 8. CORRECCION MANUAL
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("8. Correccion manual en Excel"))
story.append(body("Para corregir clasificaciones erroneas sigue estos pasos:"))
story.append(bullet("<b>Paso 1:</b> Abre el Excel generado (DteRecibidos_revision.xlsx)."))
story.append(bullet("<b>Paso 2:</b> Usa los filtros para mostrar solo filas con needs_review=1 o categoria='SIN_CLASIFICAR'."))
story.append(bullet("<b>Paso 3:</b> Consulta la hoja 'catalogo' para ver las combinaciones validas."))
story.append(bullet("<b>Paso 4:</b> Modifica las columnas 'categoria', 'subcategoria' y 'tipo_gasto' segun corresponda."))
story.append(bullet("<b>Paso 5:</b> Guarda el archivo Excel (Ctrl+S)."))
story.append(bullet("<b>Paso 6:</b> Vuelve a Terralix y presiona Excel > Importar Excel."))
story.append(space(0.3))

story.append(h2("Consejos para categorizar"))
story.append(tip("Revisa la descripcion del item junto con el proveedor (razon_social) y su giro."))
story.append(tip("La fecha de emision ayuda a distinguir gastos de cosecha (nov-dic) vs trabajos de campo (resto del ano)."))
story.append(tip("Si no sabes la subcategoria exacta, usa 'OTRO' como tipo_gasto."))
story.append(tip("Cada correccion manual mejora el modelo ML para futuras clasificaciones."))

# ══════════════════════════════════════════════════════════════════════════════
# 9. IMPORTAR Y REENTRENAR
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("9. Importar Excel y reentrenar modelo"))
story.append(body("Ve al menu <b>Excel > Importar Excel</b>. La aplicacion:"))
story.append(bullet("1. Te pide seleccionar el archivo Excel con las correcciones."))
story.append(bullet("2. Compara cada fila con la base de datos original (usa un hash interno para detectar cambios)."))
story.append(bullet("3. Solo actualiza las filas que realmente cambiaron."))
story.append(bullet("4. Marca las filas corregidas con origen='MANUAL' y confianza=100%."))
story.append(bullet("5. Valida que las combinaciones categoria/subcategoria/tipo_gasto existan en el catalogo."))
story.append(bullet("6. Reentrena automaticamente el modelo ML con los datos corregidos."))
story.append(space(0.2))
story.append(note(
    "Aprendizaje continuo: cada vez que importas correcciones, el modelo aprende "
    "de tus decisiones. Con el tiempo, la categorizacion automatica sera cada vez "
    "mas precisa y necesitaras hacer menos correcciones manuales. Con ~200 correcciones "
    "manuales veras una mejora significativa."
))

# ══════════════════════════════════════════════════════════════════════════════
# 10. INVENTARIO
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("10. Modulo de Inventario"))
story.append(body(
    "El modulo de inventario permite gestionar el stock de insumos agricolas "
    "(fertilizantes, agroquimicos, herbicidas, etc.). El stock se construye "
    "automaticamente a partir de las facturas compradas (entradas) y se descuenta "
    "con los usos registrados manualmente."
))

story.append(h2("10.1 Vista de stock actual"))
story.append(body(
    "El panel derecho 'Stock actual' muestra una tabla con todos los productos del catalogo:"
))
story.append(simple_table(
    ["Columna", "Descripcion"],
    [
        ["Codigo", "Codigo unico del producto (ej: HERBICIDA-X)"],
        ["Descripcion", "Nombre estandarizado del producto"],
        ["Unidad", "Unidad de medida (L, kg, saco, etc.)"],
        ["Stock actual", "Cantidad disponible (entradas - salidas). En rojo si es negativo."],
        ["Ultima modificacion", "Fecha del ultimo movimiento registrado"],
        ["Tipo", "Tipo del ultimo movimiento (COMPRA, USO_MANUAL, AJUSTE, etc.)"],
    ],
    col_widths=[3.5 * cm, 13.2 * cm]
))
story.append(space(0.2))
story.append(body(
    "Usa el campo <b>Buscar</b> para filtrar productos por codigo o descripcion. "
    "Presiona Enter despues de escribir para aplicar el filtro."
))

story.append(h2("10.2 Registrar uso manual"))
story.append(body(
    "Usa el panel izquierdo 'Registrar uso' para descontar del stock:"
))
story.append(bullet("Selecciona el <b>producto</b> desde el desplegable (codigo | descripcion [unidad])."))
story.append(bullet("Ingresa la <b>cantidad</b> usada (usa punto o coma como separador decimal)."))
story.append(bullet("Confirma o ajusta la <b>fecha</b> (formato YYYY-MM-DD, por defecto hoy)."))
story.append(bullet("Agrega una <b>observacion</b> opcional (ej: 'Aplicacion lote norte')."))
story.append(bullet("Presiona <b>Registrar uso</b>."))
story.append(space(0.2))
story.append(note(
    "Si el uso deja el stock negativo, la aplicacion mostrara una advertencia "
    "y te preguntara si deseas guardar igualmente."
))

story.append(h2("10.3 Edicion directa de celdas"))
story.append(body(
    "Haz <b>doble clic</b> sobre cualquier celda editable de la tabla de stock "
    "para modificarla directamente. Columnas editables:"
))
story.append(bullet("Descripcion: renombrar el producto en el catalogo."))
story.append(bullet("Unidad: corregir la unidad de medida."))
story.append(bullet("Stock actual: ajuste manual del stock (crea un movimiento tipo AJUSTE_MANUAL)."))
story.append(bullet("Ultima modificacion: corregir la fecha de ultimo movimiento."))
story.append(bullet("Tipo: corregir el tipo del ultimo movimiento."))
story.append(space(0.2))
story.append(tip("La columna 'Codigo' NO es editable para mantener la integridad referencial con los DTE."))

story.append(h2("10.4 Sincronizar desde DTE"))
story.append(body(
    "El boton <b>Refrescar</b> (o menu Inventario > Refrescar Inventario) re-sincroniza "
    "el catalogo y los movimientos de entrada desde los datos de detalle de facturas. "
    "Esta operacion es segura y se puede ejecutar en cualquier momento. "
    "Equivale a la Etapa 6 del pipeline de actualizacion."
))

# ══════════════════════════════════════════════════════════════════════════════
# 11. APLICACIONES DE CAMPO
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("11. Modulo de Aplicaciones de campo"))
story.append(body(
    "El modulo de Aplicaciones permite planificar y registrar los tratamientos "
    "fitosanitarios, fertilizaciones y otras labores que impliquen el uso de "
    "productos quimicos. Cada aplicacion registrada descuenta automaticamente "
    "los productos del inventario."
))

story.append(h2("11.1 Crear una nueva aplicacion"))
story.append(body("En el panel izquierdo 'Nueva aplicacion de campo':"))
story.append(bullet("<b>Titulo:</b> nombre descriptivo de la aplicacion (ej: 'Fungicida lote A')."))
story.append(bullet("<b>Fecha programada:</b> en formato YYYY-MM-DD."))
story.append(bullet("<b>Estado inicial:</b> PROGRAMADA (puede cambiarse a EJECUTADA despues)."))
story.append(bullet("<b>Descripcion:</b> notas o instrucciones adicionales."))

story.append(h2("11.2 Agregar productos quimicos"))
story.append(body("En la seccion 'Producto quimico' del formulario:"))
story.append(bullet("Selecciona el <b>producto</b> desde el desplegable (muestra solo productos del catalogo de inventario)."))
story.append(bullet("Ingresa la <b>cantidad</b> a usar."))
story.append(bullet("Agrega una <b>observacion</b> opcional."))
story.append(bullet("Presiona <b>Cargar producto</b> para agregarlo a la lista."))
story.append(bullet("Repite para cada producto necesario en la aplicacion."))
story.append(bullet("Para eliminar un producto de la lista, seleccionalo y presiona <b>Quitar seleccionado</b>."))
story.append(bullet("Cuando estes listo, presiona <b>Cargar</b> para guardar la aplicacion."))
story.append(space(0.2))
story.append(note(
    "Al guardar la aplicacion como EJECUTADA, los productos se descontaran "
    "automaticamente del inventario."
))

story.append(h2("11.3 Calendario mensual"))
story.append(body(
    "El panel derecho 'Calendario y gestion' muestra un calendario del mes actual. "
    "Los dias con aplicaciones programadas aparecen resaltados."
))
story.append(bullet("Usa los botones <b><</b> y <b>></b> para navegar entre meses."))
story.append(bullet("El boton <b>Hoy</b> regresa al mes actual."))
story.append(bullet("El filtro <b>Estado</b> permite ver solo PROGRAMADAS, solo EJECUTADAS o TODOS."))
story.append(bullet("Presiona <b>Refrescar</b> para recargar el calendario desde la base de datos."))

story.append(h2("11.4 Gestionar aplicaciones existentes"))
story.append(body(
    "Al hacer clic en un dia del calendario que tiene aplicaciones, se mostraran "
    "en la lista inferior. Desde ahi puedes:"
))
story.append(bullet("Editar una aplicacion existente (carga sus datos en el formulario izquierdo)."))
story.append(bullet("Marcar una aplicacion como EJECUTADA."))
story.append(bullet("Eliminar una aplicacion."))

# ══════════════════════════════════════════════════════════════════════════════
# 12. CHEQUEO SEMANAL
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("12. Chequeo semanal automatico en segundo plano"))
story.append(body(
    "Cuando abres Terralix ERP, se inicia automaticamente un proceso en segundo plano "
    "que verifica si hay nuevos DTE en el SII cada cierto numero de dias (por defecto 7 dias). "
    "Si detecta que ha pasado mas tiempo del intervalo configurado desde la ultima descarga, "
    "ejecuta el pipeline completo de forma silenciosa."
))
story.append(space(0.2))
story.append(simple_table(
    ["Variable en config.env", "Descripcion", "Valor por defecto"],
    [
        ["AUTO_WEEKLY_DTE_CHECK_ENABLED", "Habilita o deshabilita el chequeo automatico", "true"],
        ["AUTO_WEEKLY_DTE_CHECK_INTERVAL_DAYS", "Dias entre chequeos automaticos (1-30)", "7"],
        ["AUTO_WEEKLY_DTE_CHECK_RETRY_HOURS", "Horas antes de reintentar si fallo (1-48)", "6"],
        ["AUTO_WEEKLY_DTE_CHECK_POLL_MINUTES", "Frecuencia de verificacion del temporizador (5-1440)", "30"],
    ],
    col_widths=[6.8 * cm, 6.5 * cm, 3.4 * cm]
))
story.append(space(0.2))
story.append(note(
    "El chequeo automatico usa el mismo pipeline de 6 etapas que el boton Actualizar. "
    "Si el pipeline ya esta en uso (ej: el usuario presiono Actualizar), el chequeo "
    "automatico esperara hasta que termine. Los logs se guardan en "
    "%APPDATA%\\Terralix ERP\\logs\\auto_weekly_dte_check.log"
))

# ══════════════════════════════════════════════════════════════════════════════
# 13. CONFIGURAR RUTAS
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("13. Configurar rutas"))
story.append(body(
    "Disponible desde el menu <b>Opciones > Configurar Rutas (PDF / Base de Datos)</b>. "
    "Permite cambiar las rutas configuradas en el primer arranque:"
))
story.append(simple_table(
    ["Ruta", "Descripcion", "Ejemplo"],
    [
        ["Carpeta PDF", "Donde se guardan los PDFs descargados del SII", "C:\\Users\\...\\DTE"],
        ["Base de datos", "Carpeta donde esta DteRecibidos_db.db", "C:\\Users\\...\\Base"],
    ],
    col_widths=[3.5 * cm, 8.5 * cm, 4.7 * cm]
))
story.append(space(0.2))
story.append(body(
    "Los cambios se guardan inmediatamente en config.env y aplican en la siguiente operacion."
))

# ══════════════════════════════════════════════════════════════════════════════
# 14. POWER BI / EXCEL
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("14. Conexion con Power BI / Excel externo"))

story.append(h2("14.1 Conectar Excel a la base de datos"))
story.append(bullet("Abre Excel y ve a <b>Datos > Obtener datos > De otras fuentes > De ODBC</b>."))
story.append(bullet("Si no tienes el driver ODBC de SQLite, descargalo desde sqliteodbc.ch"))
story.append(bullet("Alternativa simple: <b>Datos > Obtener datos > Desde un libro</b> (abre el Excel exportado)."))
story.append(bullet("Selecciona las tablas que necesitas (detalle, documentos, catalogo, stock, entradas)."))
story.append(bullet("Crea una tabla dinamica con los datos importados."))

story.append(h2("14.2 Conectar Power BI"))
story.append(bullet("Abre Power BI Desktop."))
story.append(bullet("Inicio > Obtener datos > Mas > Base de datos SQLite."))
story.append(bullet("Navega a la ruta de DteRecibidos_db.db."))
story.append(bullet("Selecciona las tablas: detalle, documentos, catalogo_costos, insumos_catalogo, insumos_movimientos."))
story.append(bullet("Crea las relaciones: detalle.id_doc = documentos.id_doc y detalle.catalogo_costo_id = catalogo_costos.id"))
story.append(bullet("Para inventario: insumos_movimientos.codigo = insumos_catalogo.codigo"))

story.append(h2("14.3 Metricas sugeridas para dashboards"))
story.append(bullet("Gasto total por categoria y mes"))
story.append(bullet("Top 10 proveedores por monto"))
story.append(bullet("Distribucion de gastos: cosecha vs campo vs administracion"))
story.append(bullet("Tendencia mensual de gastos"))
story.append(bullet("Detalles sin clasificar pendientes de revision"))
story.append(bullet("Comparativo interanual por categoria"))
story.append(bullet("Stock actual de insumos vs consumo mensual"))
story.append(bullet("Aplicaciones de campo ejecutadas vs programadas por mes"))

# ══════════════════════════════════════════════════════════════════════════════
# 15. ESTRUCTURA BASE DE DATOS
# ══════════════════════════════════════════════════════════════════════════════
story.append(PageBreak())
story.append(h1("15. Estructura de la base de datos"))
story.append(body(
    "La base de datos SQLite (DteRecibidos_db.db) contiene las siguientes tablas:"
))

story.append(h2("15.1 Tabla documentos"))
story.append(body("Contiene un registro por cada factura o boleta recibida."))
story.append(simple_table(
    ["Columna", "Tipo", "Descripcion"],
    [
        ["id_doc", "TEXT (PK)", "Identificador unico: TIPO_RUT_FOLIO"],
        ["tipo_doc", "TEXT", "Tipo de documento (33=factura, 34=exenta, etc.)"],
        ["folio", "TEXT", "Numero de folio del documento"],
        ["rut_emisor", "TEXT", "RUT del proveedor"],
        ["razon_social", "TEXT", "Nombre del proveedor"],
        ["giro", "TEXT", "Giro comercial del proveedor"],
        ["fecha_emision", "DATE", "Fecha de emision del documento"],
        ["monto_total", "REAL", "Monto total del documento"],
        ["IVA", "REAL", "Monto del IVA"],
        ["ruta_pdf", "TEXT", "Ruta al archivo PDF local"],
    ],
    col_widths=[4.5 * cm, 3.5 * cm, 8.7 * cm]
))

story.append(h2("15.2 Tabla detalle"))
story.append(body("Contiene las lineas de cada documento (items facturados)."))
story.append(simple_table(
    ["Columna", "Tipo", "Descripcion"],
    [
        ["id_det", "TEXT (PK)", "Identificador unico de la linea"],
        ["id_doc", "TEXT (FK)", "Referencia al documento padre"],
        ["linea", "INTEGER", "Numero de linea dentro del documento"],
        ["codigo", "TEXT", "Codigo del producto (clave para inventario)"],
        ["descripcion", "TEXT", "Descripcion del producto/servicio"],
        ["unidad", "TEXT", "Unidad de medida (L, kg, saco, etc.)"],
        ["cantidad", "REAL", "Cantidad"],
        ["precio_unitario", "REAL", "Precio por unidad"],
        ["monto_item", "REAL", "Monto total de la linea"],
        ["categoria", "TEXT", "Categoria de costo asignada"],
        ["subcategoria", "TEXT", "Subcategoria de costo"],
        ["tipo_gasto", "TEXT", "Tipo especifico de gasto"],
        ["catalogo_costo_id", "INTEGER", "ID del catalogo de costos"],
        ["confianza_categoria", "INTEGER", "Nivel de confianza (0-100)"],
        ["needs_review", "INTEGER", "1=necesita revision, 0=ok"],
        ["origen_clasificacion", "TEXT", "Quien clasifico: REGLA_RS, ML_LOCAL, MANUAL"],
    ],
    col_widths=[4.5 * cm, 3.5 * cm, 8.7 * cm]
))

story.append(h2("15.3 Tabla catalogo_costos"))
story.append(body("Define las combinaciones validas de clasificacion contable."))
story.append(simple_table(
    ["Columna", "Tipo", "Descripcion"],
    [
        ["id", "INTEGER (PK)", "Identificador unico"],
        ["categoria_costo", "TEXT", "Categoria principal (ej: COSECHA)"],
        ["subcategoria_costo", "TEXT", "Subcategoria (ej: MANO_OBRA_COSECHA)"],
        ["tipo_gasto", "TEXT", "Tipo especifico (ej: MANO_OBRA)"],
    ],
    col_widths=[4.5 * cm, 3.5 * cm, 8.7 * cm]
))

story.append(h2("15.4 Tablas de inventario"))
story.append(body(
    "Las siguientes tablas gestionan el inventario de insumos agricolas:"
))
story.append(simple_table(
    ["Tabla", "Descripcion"],
    [
        ["insumos_catalogo", "Catalogo maestro de productos: codigo, descripcion_estandar, unidad_base, stock_actual"],
        ["insumos_movimientos", "Historico de movimientos: codigo, fecha, cantidad, tipo (COMPRA/USO_MANUAL/AJUSTE), observacion"],
        ["aplicaciones_campo", "Registro de aplicaciones: titulo, fecha, estado, descripcion"],
        ["aplicaciones_productos", "Productos quimicos de cada aplicacion: id_aplicacion, codigo, cantidad"],
    ],
    col_widths=[5 * cm, 11.7 * cm]
))

# ══════════════════════════════════════════════════════════════════════════════
# 16. CATALOGO DE COSTOS
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("16. Catalogo de costos"))
story.append(body("El sistema utiliza 7 categorias principales para clasificar los gastos:"))
story.append(simple_table(
    ["Categoria", "Descripcion", "Ejemplos"],
    [
        ["ADMINISTRACION", "Gastos de oficina, personal, servicios basicos, TI", "Contabilidad, sueldos, software, internet"],
        ["COSECHA", "Todo lo relacionado con la cosecha de cerezas", "Mano obra cosecha, bins, flete, packing, abejas"],
        ["ENERGIA", "Consumo electrico y servicios de luz", "Cuentas CGE, consumo electrico"],
        ["GASTOS_FINANCIEROS", "Bancos, creditos, seguros, arriendos financieros", "Comisiones bancarias, intereses, seguros"],
        ["INSUMOS_AGRICOLAS", "Fertilizantes, agroquimicos, herbicidas", "Fertilizantes, fungicidas, herbicidas, insecticidas"],
        ["MANTENCION", "Vehiculos, riego, infraestructura, cercos", "Combustible, peajes, TAG, filtros riego, reparaciones"],
        ["TRABAJOS_CAMPO", "Labores agricolas, poda, servicios, mano obra", "Poda, tractorista, aplicacion herbicida, jornales"],
    ],
    col_widths=[4 * cm, 7 * cm, 5.7 * cm]
))
story.append(space(0.3))

story.append(h2("Temporalidad de cosecha"))
story.append(body(
    "Para cerezas en Chile, la cosecha ocurre principalmente entre noviembre y diciembre. "
    "El modelo ML considera la fecha de emision para distinguir entre gastos de cosecha "
    "y trabajos de campo regulares:"
))
story.append(simple_table(
    ["Meses", "Temporada", "Efecto en clasificacion"],
    [
        ["Enero - Febrero", "Postcosecha", "Limpieza, mantencion postcosecha"],
        ["Marzo - Mayo", "Otono", "Preparacion terreno, poda"],
        ["Junio - Agosto", "Invierno", "Poda invernal, mantencion riego"],
        ["Septiembre - Octubre", "Primavera", "Aplicaciones, floracion"],
        ["Noviembre - Diciembre", "Cosecha", "Mano obra cosecha, transporte, packing"],
    ],
    col_widths=[4.5 * cm, 4 * cm, 8.2 * cm]
))

# ══════════════════════════════════════════════════════════════════════════════
# 17. FAQ
# ══════════════════════════════════════════════════════════════════════════════
story.append(h1("17. Preguntas frecuentes"))

faqs = [
    ("La app no puede conectarse al SII",
     "Verifica que las credenciales en config.env (RUT, CLAVE) sean correctas. "
     "Asegurate de tener conexion a internet estable y que el portal del SII este disponible."),
    ("No puedo iniciar sesion / dice que las credenciales son incorrectas",
     "Verifica que estes usando el email correcto asignado por el administrador. "
     "Si olvidaste la contrasena, usa el enlace '?Olvidaste tu contrasena?' en la pantalla de ingreso."),
    ("Un PDF no se lee correctamente",
     "Ejecuta nuevamente 'Actualizar Base de Datos' para reprocesar los PDFs pendientes. "
     "Si persiste, el PDF puede estar danado o en formato no soportado."),
    ("La categorizacion esta equivocada",
     "Exporta a Excel, corrige la categoria manualmente y reimporta. "
     "El modelo aprendera de tu correccion para el futuro."),
    ("Quiero agregar una nueva categoria",
     "Agrega la nueva combinacion en la tabla catalogo_costos de la base de datos "
     "usando un editor SQLite (como DB Browser for SQLite)."),
    ("?Donde esta la base de datos?",
     "En la ruta configurada en config.env bajo DB_PATH_DTE_RECIBIDOS. "
     "Puedes verla y cambiarla desde 'Opciones > Configurar Rutas'."),
    ("?Puedo usar la app sin internet?",
     "La descarga del SII y la lectura IA requieren internet. "
     "La categorizacion ML, exportacion Excel, importacion e inventario funcionan offline."),
    ("?Como hago backup de mis datos?",
     "Copia el archivo DteRecibidos_db.db, la carpeta de PDFs y "
     "%APPDATA%\\Terralix ERP\\ a una ubicacion segura. "
     "Incluye classifier_dte.pkl para respaldar el modelo entrenado."),
    ("El modelo ML tiene baja confianza",
     "Esto es normal al inicio. A medida que corrijas mas filas y reimportes, "
     "el modelo mejorara progresivamente. Con ~200 correcciones manuales veras una mejora significativa."),
    ("El stock de un insumo esta incorrecto",
     "Puedes hacer doble clic en la celda 'Stock actual' de la tabla de inventario "
     "para ajustarlo manualmente. Esto crea un movimiento tipo AJUSTE_MANUAL para mantener el historial."),
    ("El chequeo automatico no funciona",
     "Verifica que AUTO_WEEKLY_DTE_CHECK_ENABLED=true en config.env. "
     "Revisa el archivo de log en %APPDATA%\\Terralix ERP\\logs\\auto_weekly_dte_check.log."),
    ("No veo las pestanas Inventario o Aplicaciones",
     "Asegurate de estar usando la version 1.3.0 o superior. "
     "Si la ventana es muy pequena, redimensionala para que aparezcan las tres pestanas."),
]

for q, a in faqs:
    story.append(KeepTogether([
        Paragraph(f"<b>P: {q}</b>",
                  ParagraphStyle("FAQ_Q", parent=style_body, textColor=GREEN, spaceAfter=3)),
        Paragraph(f"R: {a}",
                  ParagraphStyle("FAQ_A", parent=style_body, leftIndent=10, spaceAfter=10)),
    ]))

# ══════════════════════════════════════════════════════════════════════════════
# BUILD
# ══════════════════════════════════════════════════════════════════════════════
doc.build(story, onFirstPage=_add_page_number, onLaterPages=_add_page_number)
print(f"Manual generado: {OUTPUT_PATH}")
