"""Genera el manual de usuario de Terralix ERP como .docx"""
import os
from docx import Document
from docx.shared import Inches, Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn

OUT = os.path.join(os.path.dirname(__file__), "Manual_Terralix_ERP.docx")

doc = Document()

# --- Estilos globales ---
style = doc.styles["Normal"]
style.font.name = "Calibri"
style.font.size = Pt(11)
style.paragraph_format.space_after = Pt(6)

for level in range(1, 4):
    hs = doc.styles[f"Heading {level}"]
    hs.font.color.rgb = RGBColor(0x0F, 0x66, 0x45)  # verde Terralix
    hs.font.name = "Calibri"

# --- Funciones auxiliares ---
def add_table_row(table, cells, bold=False, bg=None):
    row = table.add_row()
    for i, text in enumerate(cells):
        cell = row.cells[i]
        cell.text = ""
        p = cell.paragraphs[0]
        run = p.add_run(str(text))
        run.font.size = Pt(10)
        run.font.name = "Calibri"
        if bold:
            run.bold = True
            run.font.color.rgb = RGBColor(0xFF, 0xFF, 0xFF)
        if bg:
            shading = cell._element.get_or_add_tcPr()
            sh = shading.makeelement(qn("w:shd"), {
                qn("w:val"): "clear",
                qn("w:color"): "auto",
                qn("w:fill"): bg,
            })
            shading.append(sh)


def make_table(headers, rows_data, col_widths=None):
    table = doc.add_table(rows=0, cols=len(headers))
    table.alignment = WD_TABLE_ALIGNMENT.CENTER
    table.style = "Light Grid Accent 1"
    add_table_row(table, headers, bold=True, bg="0F6645")
    for row_data in rows_data:
        add_table_row(table, row_data)
    if col_widths:
        for row in table.rows:
            for i, w in enumerate(col_widths):
                row.cells[i].width = Cm(w)
    return table


# =====================================================================
# PORTADA
# =====================================================================
p = doc.add_paragraph()
p.alignment = WD_ALIGN_PARAGRAPH.CENTER
p.space_before = Pt(120)
run = p.add_run("TERRALIX ERP")
run.font.size = Pt(36)
run.font.color.rgb = RGBColor(0x0F, 0x66, 0x45)
run.bold = True

p2 = doc.add_paragraph()
p2.alignment = WD_ALIGN_PARAGRAPH.CENTER
run2 = p2.add_run("Manual de Usuario")
run2.font.size = Pt(24)
run2.font.color.rgb = RGBColor(0x33, 0x33, 0x33)

p3 = doc.add_paragraph()
p3.alignment = WD_ALIGN_PARAGRAPH.CENTER
p3.space_before = Pt(40)
run3 = p3.add_run("Versi\u00f3n 1.3.0\nMarzo 2026")
run3.font.size = Pt(14)
run3.font.color.rgb = RGBColor(0x66, 0x66, 0x66)

p4 = doc.add_paragraph()
p4.alignment = WD_ALIGN_PARAGRAPH.CENTER
p4.space_before = Pt(20)
run4 = p4.add_run("Agr\u00edcola Las Tipuanas SPA")
run4.font.size = Pt(12)
run4.font.color.rgb = RGBColor(0x66, 0x66, 0x66)
run4.italic = True

doc.add_page_break()

# =====================================================================
# \u00cdNDICE
# =====================================================================
doc.add_heading("\u00cdndice", level=1)
toc_items = [
    "1. Introducci\u00f3n",
    "2. Requisitos del sistema",
    "3. Instalaci\u00f3n y primera ejecuci\u00f3n",
    "4. Inicio de sesi\u00f3n",
    "5. Pantalla principal",
    "6. Actualizar Base de Datos (pipeline completo)",
    "7. Exportar Excel para revisi\u00f3n",
    "8. Correcci\u00f3n manual en Excel",
    "9. Importar Excel y reentrenar modelo",
    "10. Configurar rutas",
    "11. Conexi\u00f3n con Power BI / Excel externo",
    "12. Estructura de la base de datos",
    "13. Cat\u00e1logo de costos",
    "14. Preguntas frecuentes",
]
for item in toc_items:
    p = doc.add_paragraph(item)
    p.paragraph_format.space_after = Pt(2)

doc.add_page_break()

# =====================================================================
# 1. INTRODUCCI\u00d3N
# =====================================================================
doc.add_heading("1. Introducci\u00f3n", level=1)
doc.add_paragraph(
    "Terralix ERP es una aplicaci\u00f3n de escritorio desarrollada en Python "
    "para la gesti\u00f3n automatizada de documentos tributarios electr\u00f3nicos (DTE) "
    "recibidos por Agr\u00edcola Las Tipuanas SPA."
)
doc.add_paragraph(
    "La aplicaci\u00f3n automatiza el ciclo completo de gesti\u00f3n de facturas y boletas:"
)
items = [
    "Descarga autom\u00e1tica desde el Servicio de Impuestos Internos (SII)",
    "Lectura inteligente de PDFs mediante visi\u00f3n artificial (GPT-4o)",
    "Categorizaci\u00f3n contable autom\u00e1tica con modelo de Machine Learning local",
    "Exportaci\u00f3n a Excel para revisi\u00f3n manual y correcci\u00f3n",
    "Aprendizaje continuo: las correcciones manuales reentrenan el modelo",
    "Conexi\u00f3n directa con Power BI y Excel para an\u00e1lisis y reportes",
]
for item in items:
    doc.add_paragraph(item, style="List Bullet")

doc.add_paragraph(
    "El objetivo es que con un solo clic se actualice toda la base de datos, "
    "permiti\u00e9ndote enfocarte en el an\u00e1lisis y la gesti\u00f3n, no en la carga manual de datos."
)

# =====================================================================
# 2. REQUISITOS
# =====================================================================
doc.add_heading("2. Requisitos del sistema", level=1)
make_table(
    ["Componente", "Requisito"],
    [
        ["Sistema operativo", "Windows 10 / 11 (64 bits)"],
        ["Python", "3.11 o superior (incluido en el instalador)"],
        ["RAM", "4 GB m\u00ednimo, 8 GB recomendado"],
        ["Disco", "500 MB para la app + espacio para PDFs"],
        ["Internet", "Requerido para descarga SII, lectura IA e inicio de sesi\u00f3n"],
        ["Navegador", "Chromium (instalado autom\u00e1ticamente por Playwright)"],
        ["Excel", "Microsoft Excel o LibreOffice Calc (para revisi\u00f3n)"],
    ],
    col_widths=[5, 12],
)

# =====================================================================
# 3. INSTALACI\u00d3N
# =====================================================================
doc.add_heading("3. Instalaci\u00f3n y primera ejecuci\u00f3n", level=1)

doc.add_heading("3.1 Instalaci\u00f3n", level=2)
doc.add_paragraph(
    "Descarga el instalador desde el repositorio o recibe el ejecutable directamente. "
    "Ejecuta TERRALIX.py o el acceso directo creado por el instalador."
)

doc.add_heading("3.2 Primera ejecuci\u00f3n", level=2)
doc.add_paragraph(
    "En la primera ejecuci\u00f3n, la aplicaci\u00f3n te pedir\u00e1 configurar dos rutas:"
)
items = [
    ("Carpeta de PDFs", "Donde se guardar\u00e1n los documentos PDF descargados del SII. "
     "Ejemplo: C:\\Users\\cleme\\Documents\\DTE"),
    ("Carpeta de Base de Datos", "Donde se crear\u00e1 el archivo DteRecibidos_db.db. "
     "Ejemplo: C:\\Users\\cleme\\Documents\\Base"),
]
for title, desc in items:
    p = doc.add_paragraph()
    run = p.add_run(f"{title}: ")
    run.bold = True
    p.add_run(desc)

doc.add_paragraph(
    "Estas rutas se guardan en data/config.env y pueden cambiarse "
    "posteriormente desde el men\u00fa Opciones > Configurar Rutas."
)

# =====================================================================
# 4. LOGIN
# =====================================================================
doc.add_heading("4. Inicio de sesi\u00f3n", level=1)
doc.add_paragraph(
    "Al abrir la aplicaci\u00f3n aparece la ventana de ingreso. "
    "Escribe tu email y contrase\u00f1a registrados, y presiona \u2018Continuar\u2019 o la tecla Enter."
)
doc.add_paragraph(
    "La autenticaci\u00f3n se realiza contra Supabase Auth (requiere conexi\u00f3n a internet). "
    "Solo los usuarios autorizados por el administrador pueden acceder."
)

doc.add_heading("4.1 Recordar credenciales", level=2)
doc.add_paragraph(
    "Si marcas la casilla \u2018Recordar credenciales\u2019, la aplicaci\u00f3n guardar\u00e1 tu email y "
    "contrase\u00f1a de forma segura en el Administrador de Credenciales de Windows (Windows Credential Manager). "
    "La pr\u00f3xima vez que abras la app, los campos se rellenar\u00e1n autom\u00e1ticamente."
)

doc.add_heading("4.2 Recuperar contrase\u00f1a", level=2)
doc.add_paragraph(
    "Si olvidaste tu contrase\u00f1a, escribe tu email y haz clic en "
    "\u2018\u00bfOlvidaste tu contrase\u00f1a?\u2019. Se enviar\u00e1 un enlace de recuperaci\u00f3n a tu correo. "
    "Al hacer clic en el enlace, se abrir\u00e1 una ventana en la aplicaci\u00f3n para establecer "
    "una nueva contrase\u00f1a."
)

p = doc.add_paragraph()
run = p.add_run("Atajos de teclado: ")
run.bold = True
p.add_run("Enter = Continuar | Escape = Salir")

# =====================================================================
# 5. PANTALLA PRINCIPAL
# =====================================================================
doc.add_heading("5. Pantalla principal", level=1)
doc.add_paragraph(
    "Tras iniciar sesi\u00f3n, ver\u00e1s la pantalla principal con los siguientes elementos:"
)

doc.add_heading("5.1 Panel izquierdo", level=2)
doc.add_paragraph(
    "Contiene el logo de Terralix y el bot\u00f3n principal \u2018Actualizar\u2019, "
    "que ejecuta el pipeline completo de descarga, lectura IA y categorizaci\u00f3n."
)

doc.add_heading("5.2 Panel derecho", level=2)
doc.add_paragraph(
    "Consola de salida que muestra el progreso de cada operaci\u00f3n en tiempo real. "
    "Debajo hay dos barras de progreso: una para descargas y otra para lectura de PDFs."
)

doc.add_heading("5.3 Barra de men\u00fa superior", level=2)
doc.add_paragraph(
    "La barra de men\u00fa contiene todas las funciones adicionales de la aplicaci\u00f3n:"
)
make_table(
    ["Men\u00fa", "Opci\u00f3n", "Funci\u00f3n"],
    [
        ["Excel", "Exportar Excel", "Genera un Excel con toda la base de datos para revisi\u00f3n manual"],
        ["Excel", "Importar Excel", "Lee las correcciones del Excel, actualiza la DB y reentrena el modelo ML"],
        ["Opciones", "Configurar Rutas", "Permite cambiar la carpeta de PDFs y la ruta de la base de datos"],
        ["Ayuda", "Manual de usuario", "Abre este manual de usuario"],
    ],
    col_widths=[3, 4, 10],
)

# =====================================================================
# 6. ACTUALIZAR BD
# =====================================================================
doc.add_heading("6. Actualizar Base de Datos", level=1)
doc.add_paragraph(
    "Este es el bot\u00f3n principal de la aplicaci\u00f3n. Con un solo clic ejecuta "
    "las 3 etapas del pipeline:"
)

doc.add_heading("Etapa 1: Descarga desde SII", level=2)
doc.add_paragraph(
    "Se conecta autom\u00e1ticamente al Servicio de Impuestos Internos usando "
    "las credenciales configuradas en config.env (RUT y CLAVE). "
    "Descarga los PDFs de facturas y boletas recibidas que a\u00fan no est\u00e9n en la carpeta local."
)
p = doc.add_paragraph()
run = p.add_run("Nota: ")
run.bold = True
p.add_run("No cierres la ventana durante este proceso. La descarga puede tomar varios minutos.")

doc.add_heading("Etapa 2: Lectura IA de PDFs", level=2)
doc.add_paragraph(
    "Cada PDF nuevo se env\u00eda a GPT-4o (visi\u00f3n) que extrae la informaci\u00f3n estructurada: "
    "emisor, folio, fecha, montos y el detalle l\u00ednea por l\u00ednea (descripci\u00f3n, cantidad, "
    "precio unitario, monto). Los datos se guardan en las tablas \u2018documentos\u2019 y \u2018detalle\u2019."
)

doc.add_heading("Etapa 3: Categorizaci\u00f3n contable", level=2)
doc.add_paragraph(
    "Cada l\u00ednea de detalle se clasifica autom\u00e1ticamente usando un sistema de 3 capas:"
)
items = [
    ("Capa 1 \u2013 Reglas locales", "Patrones de proveedor y descripci\u00f3n (ej.: si el proveedor es una autopista, "
     "la categor\u00eda es MANTENCI\u00d3N > VEH\u00cdCULOS > PEAJES). Confianza: 90-95%."),
    ("Capa 2 \u2013 Mantenedor de proveedores", "Si el proveedor ya fue clasificado antes con alta confianza, "
     "se reutiliza esa categor\u00eda. Confianza: 92%."),
    ("Capa 3 \u2013 Modelo ML local", "Un clasificador TF-IDF + LinearSVC entrenado con los datos hist\u00f3ricos. "
     "Usa la descripci\u00f3n, proveedor, giro y temporada agr\u00edcola para predecir la categor\u00eda. "
     "Funciona sin internet y sin costo de API."),
]
for title, desc in items:
    p = doc.add_paragraph()
    run = p.add_run(f"{title}: ")
    run.bold = True
    p.add_run(desc)

doc.add_paragraph(
    "Al finalizar, se genera un reporte de detalles sin clasificar que se abre autom\u00e1ticamente."
)

# =====================================================================
# 7. EXPORTAR EXCEL
# =====================================================================
doc.add_heading("7. Exportar Excel para revisi\u00f3n", level=1)
doc.add_paragraph(
    "Ve al men\u00fa Excel > Exportar Excel. La aplicaci\u00f3n genera un archivo "
    "DteRecibidos_revision.xlsx en la misma carpeta de la base de datos."
)

doc.add_heading("Estructura del Excel", level=2)
make_table(
    ["Hoja", "Contenido", "Editable"],
    [
        ["detalle", "Todas las l\u00edneas de detalle con su clasificaci\u00f3n", "S\u00ed (columnas de categor\u00eda)"],
        ["cat\u00e1logo", "Las 142 combinaciones v\u00e1lidas de categor\u00eda/subcategor\u00eda/tipo_gasto", "No (solo referencia)"],
        ["documentos", "Datos de los documentos: emisor, fecha, montos", "No (solo referencia)"],
    ],
    col_widths=[3, 10, 4],
)

doc.add_heading("Codificaci\u00f3n de colores", level=2)
make_table(
    ["Color", "Significado"],
    [
        ["Amarillo", "Filas marcadas como \u2018needs_review\u2019: el modelo no est\u00e1 seguro"],
        ["Rojo claro", "Filas SIN_CLASIFICAR: no se pudo determinar la categor\u00eda"],
        ["Gris (texto)", "Columnas de solo lectura (no editables)"],
    ],
    col_widths=[4, 13],
)

doc.add_paragraph(
    "La columna \u2018categor\u00eda\u2019 tiene un desplegable con las categor\u00edas v\u00e1lidas del cat\u00e1logo."
)

# =====================================================================
# 8. CORRECCI\u00d3N MANUAL
# =====================================================================
doc.add_heading("8. Correcci\u00f3n manual en Excel", level=1)
doc.add_paragraph("Para corregir clasificaciones err\u00f3neas:")

steps = [
    "Abre el Excel generado (DteRecibidos_revision.xlsx)",
    "Usa los filtros para mostrar solo filas con needs_review=1 o categor\u00eda=\u2018SIN_CLASIFICAR\u2019",
    "Consulta la hoja \u2018cat\u00e1logo\u2019 para ver las combinaciones v\u00e1lidas",
    "Modifica las columnas \u2018categor\u00eda\u2019, \u2018subcategor\u00eda\u2019 y \u2018tipo_gasto\u2019 seg\u00fan corresponda",
    "Guarda el archivo Excel (Ctrl+S)",
    "Vuelve a Terralix y ve a Excel > Importar Excel",
]
for i, step in enumerate(steps, 1):
    p = doc.add_paragraph()
    run = p.add_run(f"Paso {i}: ")
    run.bold = True
    p.add_run(step)

doc.add_heading("Consejos para categorizar", level=2)
items = [
    "Revisa la descripci\u00f3n del \u00edtem junto con el proveedor (raz\u00f3n social) y su giro",
    "La fecha de emisi\u00f3n ayuda a distinguir gastos de cosecha (nov-dic) vs. trabajos de campo (resto del a\u00f1o)",
    "Si no sabes la subcategor\u00eda exacta, usa \u2018OTRO\u2019 como tipo_gasto",
    "Cada correcci\u00f3n manual mejora el modelo ML para futuras clasificaciones",
]
for item in items:
    doc.add_paragraph(item, style="List Bullet")

# =====================================================================
# 9. IMPORTAR EXCEL
# =====================================================================
doc.add_heading("9. Importar Excel y reentrenar modelo", level=1)
doc.add_paragraph("Ve al men\u00fa Excel > Importar Excel. La aplicaci\u00f3n:")
steps = [
    "Te pide seleccionar el archivo Excel con las correcciones",
    "Compara cada fila con la base de datos original (usa un hash oculto para detectar cambios)",
    "Solo actualiza las filas que realmente cambiaron",
    "Marca las filas corregidas con origen=\u2018MANUAL\u2019 y confianza=100%",
    "Valida que las combinaciones categor\u00eda/subcategor\u00eda/tipo_gasto existan en el cat\u00e1logo",
    "Reentrena autom\u00e1ticamente el modelo ML con los datos corregidos",
]
for i, step in enumerate(steps, 1):
    doc.add_paragraph(f"{i}. {step}")

p = doc.add_paragraph()
run = p.add_run("Aprendizaje continuo: ")
run.bold = True
run.font.color.rgb = RGBColor(0x0F, 0x66, 0x45)
p.add_run(
    "Cada vez que importas correcciones, el modelo aprende de tus decisiones. "
    "Con el tiempo, la categorizaci\u00f3n autom\u00e1tica ser\u00e1 cada vez m\u00e1s precisa "
    "y necesitar\u00e1s hacer menos correcciones manuales."
)

# =====================================================================
# 10. CONFIGURAR RUTAS
# =====================================================================
doc.add_heading("10. Configurar rutas", level=1)
doc.add_paragraph(
    "Disponible desde el men\u00fa Opciones > Configurar Rutas. "
    "Permite cambiar:"
)
make_table(
    ["Ruta", "Descripci\u00f3n", "Ejemplo"],
    [
        ["Carpeta PDF", "Donde se guardan los PDFs descargados del SII", "C:\\Users\\...\\DTE"],
        ["Base de datos", "Carpeta donde est\u00e1 DteRecibidos_db.db", "C:\\Users\\...\\Base"],
    ],
    col_widths=[3, 8, 6],
)
doc.add_paragraph(
    "Los cambios se guardan inmediatamente en config.env y aplican en la siguiente operaci\u00f3n."
)

# =====================================================================
# 11. POWER BI / EXCEL
# =====================================================================
doc.add_heading("11. Conexi\u00f3n con Power BI / Excel externo", level=1)

doc.add_heading("11.1 Conectar Excel a la base de datos", level=2)
steps = [
    "Abre Excel y ve a Datos > Obtener datos > De otras fuentes > De ODBC",
    "Si no tienes el driver ODBC de SQLite, desc\u00e1rgalo desde sqliteodbc.ch",
    "Alternativa m\u00e1s simple: Datos > Obtener datos > Desde un libro (abre el Excel exportado)",
    "Selecciona las tablas que necesitas (detalle, documentos, cat\u00e1logo)",
    "Crea una tabla din\u00e1mica con los datos importados",
]
for i, step in enumerate(steps, 1):
    doc.add_paragraph(f"{i}. {step}")

doc.add_heading("11.2 Conectar Power BI", level=2)
steps = [
    "Abre Power BI Desktop",
    "Inicio > Obtener datos > M\u00e1s > Base de datos SQLite",
    "Navega a la ruta de DteRecibidos_db.db",
    "Selecciona las tablas: detalle, documentos, catalogo_costos",
    "Crea las relaciones: detalle.id_doc = documentos.id_doc y detalle.catalogo_costo_id = catalogo_costos.id",
    "Ahora puedes crear dashboards con todas las m\u00e9tricas de gastos",
]
for i, step in enumerate(steps, 1):
    doc.add_paragraph(f"{i}. {step}")

doc.add_heading("11.3 M\u00e9tricas sugeridas para dashboards", level=2)
items = [
    "Gasto total por categor\u00eda y mes",
    "Top 10 proveedores por monto",
    "Distribuci\u00f3n de gastos: cosecha vs. campo vs. administraci\u00f3n",
    "Tendencia mensual de gastos",
    "Detalles sin clasificar pendientes de revisi\u00f3n",
    "Comparativo interanual por categor\u00eda",
]
for item in items:
    doc.add_paragraph(item, style="List Bullet")

# =====================================================================
# 12. ESTRUCTURA DB
# =====================================================================
doc.add_heading("12. Estructura de la base de datos", level=1)
doc.add_paragraph(
    "La base de datos SQLite (DteRecibidos_db.db) contiene las siguientes tablas:"
)

doc.add_heading("12.1 Tabla documentos", level=2)
doc.add_paragraph("Contiene un registro por cada factura o boleta recibida.")
make_table(
    ["Columna", "Tipo", "Descripci\u00f3n"],
    [
        ["id_doc", "TEXT (PK)", "Identificador \u00fanico: TIPO_RUT_FOLIO"],
        ["tipo_doc", "TEXT", "Tipo de documento (33=factura, 34=exenta, etc.)"],
        ["folio", "TEXT", "N\u00famero de folio del documento"],
        ["rut_emisor", "TEXT", "RUT del proveedor"],
        ["razon_social", "TEXT", "Nombre del proveedor"],
        ["giro", "TEXT", "Giro comercial del proveedor"],
        ["fecha_emision", "DATE", "Fecha de emisi\u00f3n del documento"],
        ["monto_total", "REAL", "Monto total del documento"],
        ["IVA", "REAL", "Monto del IVA"],
        ["ruta_pdf", "TEXT", "Ruta al archivo PDF local"],
    ],
    col_widths=[3.5, 2.5, 11],
)

doc.add_heading("12.2 Tabla detalle", level=2)
doc.add_paragraph("Contiene las l\u00edneas de cada documento (\u00edtems facturados).")
make_table(
    ["Columna", "Tipo", "Descripci\u00f3n"],
    [
        ["id_det", "TEXT (PK)", "Identificador \u00fanico de la l\u00ednea"],
        ["id_doc", "TEXT (FK)", "Referencia al documento padre"],
        ["linea", "INTEGER", "N\u00famero de l\u00ednea dentro del documento"],
        ["descripcion", "TEXT", "Descripci\u00f3n del producto/servicio"],
        ["cantidad", "REAL", "Cantidad"],
        ["precio_unitario", "REAL", "Precio por unidad"],
        ["monto_item", "REAL", "Monto total de la l\u00ednea"],
        ["categoria", "TEXT", "Categor\u00eda de costo asignada"],
        ["subcategoria", "TEXT", "Subcategor\u00eda de costo"],
        ["tipo_gasto", "TEXT", "Tipo espec\u00edfico de gasto"],
        ["catalogo_costo_id", "INTEGER", "ID del cat\u00e1logo de costos"],
        ["confianza_categoria", "INTEGER", "Nivel de confianza (0\u2013100)"],
        ["needs_review", "INTEGER", "1=necesita revisi\u00f3n, 0=ok"],
        ["origen_clasificacion", "TEXT", "Quien clasific\u00f3: REGLA_RS, ML_LOCAL, MANUAL"],
        ["razon_social", "TEXT", "Nombre del proveedor (desnormalizado)"],
        ["giro", "TEXT", "Giro del proveedor (desnormalizado)"],
        ["fecha_emision", "TEXT", "Fecha de emisi\u00f3n (desnormalizado)"],
    ],
    col_widths=[4, 2.5, 10.5],
)

doc.add_heading("12.3 Tabla catalogo_costos", level=2)
doc.add_paragraph("Define las 142 combinaciones v\u00e1lidas de clasificaci\u00f3n.")
make_table(
    ["Columna", "Tipo", "Descripci\u00f3n"],
    [
        ["id", "INTEGER (PK)", "Identificador \u00fanico"],
        ["categoria_costo", "TEXT", "Categor\u00eda principal (ej.: COSECHA)"],
        ["subcategoria_costo", "TEXT", "Subcategor\u00eda (ej.: MANO_OBRA_COSECHA)"],
        ["tipo_gasto", "TEXT", "Tipo espec\u00edfico (ej.: MANO_OBRA)"],
    ],
    col_widths=[4, 2.5, 10.5],
)

# =====================================================================
# 13. CAT\u00c1LOGO
# =====================================================================
doc.add_heading("13. Cat\u00e1logo de costos", level=1)
doc.add_paragraph(
    "El sistema utiliza 7 categor\u00edas principales para clasificar los gastos:"
)
make_table(
    ["Categor\u00eda", "Descripci\u00f3n", "Ejemplos"],
    [
        ["ADMINISTRACI\u00d3N", "Gastos de oficina, personal, servicios b\u00e1sicos, TI",
         "Contabilidad, sueldos, software, internet"],
        ["COSECHA", "Todo lo relacionado con la cosecha de c\u00edtricos",
         "Mano de obra cosecha, bins, flete, packing, abejas"],
        ["ENERG\u00cdA", "Consumo el\u00e9ctrico y servicios de luz",
         "Cuentas CGE, consumo el\u00e9ctrico"],
        ["GASTOS_FINANCIEROS", "Bancos, cr\u00e9ditos, seguros, arriendos financieros",
         "Comisiones bancarias, intereses, seguros"],
        ["INSUMOS_AGR\u00cdCOLAS", "Fertilizantes, agroqu\u00edmicos, herbicidas",
         "Fertilizantes, fungicidas, herbicidas"],
        ["MANTENCI\u00d3N", "Veh\u00edculos, riego, infraestructura, cercos",
         "Combustible, peajes, TAG, filtros riego, reparaciones"],
        ["TRABAJOS_CAMPO", "Labores agr\u00edcolas, poda, servicios, mano de obra",
         "Poda, tractorista, aplicaci\u00f3n herbicida, jornales"],
    ],
    col_widths=[4, 5.5, 7.5],
)

doc.add_heading("Temporalidad de cosecha", level=2)
doc.add_paragraph(
    "Para c\u00edtricos en Chile, la cosecha ocurre principalmente entre "
    "noviembre y diciembre. El modelo ML considera la fecha de emisi\u00f3n "
    "para distinguir entre gastos de cosecha y trabajos de campo regulares."
)
make_table(
    ["Mes", "Temporada", "Efecto en clasificaci\u00f3n"],
    [
        ["Enero \u2013 Febrero", "Postcosecha", "Limpieza, mantenci\u00f3n postcosecha"],
        ["Marzo \u2013 Mayo", "Oto\u00f1o", "Preparaci\u00f3n terreno, poda"],
        ["Junio \u2013 Agosto", "Invierno", "Poda invernal, mantenci\u00f3n riego"],
        ["Septiembre \u2013 Octubre", "Primavera", "Aplicaciones, floraci\u00f3n"],
        ["Noviembre \u2013 Diciembre", "Cosecha", "Mano de obra cosecha, transporte, packing"],
    ],
    col_widths=[4, 3, 10],
)

# =====================================================================
# 14. FAQ
# =====================================================================
doc.add_heading("14. Preguntas frecuentes", level=1)

faqs = [
    ("\u00bfLa app no puede conectarse al SII?",
     "Verifica que las credenciales en config.env (RUT, CLAVE) sean correctas. "
     "Aseg\u00farate de tener conexi\u00f3n a internet estable."),
    ("\u00bfUn PDF no se lee correctamente?",
     "Ejecuta nuevamente \u2018Actualizar Base de Datos\u2019 para reprocesar los PDFs pendientes. "
     "Si persiste, el PDF puede estar da\u00f1ado o en formato no soportado."),
    ("\u00bfLa categorizaci\u00f3n est\u00e1 equivocada?",
     "Exporta a Excel, corrige la categor\u00eda manualmente y reimporta. "
     "El modelo aprender\u00e1 de tu correcci\u00f3n para el futuro."),
    ("\u00bfQuiero agregar una nueva categor\u00eda?",
     "Agrega la nueva combinaci\u00f3n en la tabla catalogo_costos de la base de datos "
     "usando un editor SQLite (como DB Browser for SQLite)."),
    ("\u00bfD\u00f3nde est\u00e1 la base de datos?",
     "En la ruta configurada en config.env bajo DB_PATH_DTE_RECIBIDOS. "
     "Puedes verla y cambiarla desde Opciones > Configurar Rutas."),
    ("\u00bfPuedo usar la app sin internet?",
     "La descarga del SII, la lectura IA y el inicio de sesi\u00f3n requieren internet. "
     "La categorizaci\u00f3n ML, exportaci\u00f3n Excel e importaci\u00f3n funcionan sin conexi\u00f3n."),
    ("\u00bfC\u00f3mo hago respaldo de mis datos?",
     "Copia el archivo DteRecibidos_db.db y la carpeta de PDFs a una ubicaci\u00f3n segura. "
     "Tambi\u00e9n puedes copiar data/classifier_dte.pkl para respaldar el modelo entrenado."),
    ("\u00bfEl modelo ML tiene baja confianza?",
     "Esto es normal al inicio. A medida que corrijas m\u00e1s filas y reimportes, "
     "el modelo mejorar\u00e1 progresivamente. Con unas 200 correcciones manuales ver\u00e1s una mejora significativa."),
    ("\u00bfOlvid\u00e9 mi contrase\u00f1a?",
     "En la pantalla de login, escribe tu email y haz clic en \u2018\u00bfOlvidaste tu contrase\u00f1a?\u2019. "
     "Recibir\u00e1s un correo con un enlace para restablecerla."),
]

for q, a in faqs:
    p = doc.add_paragraph()
    run = p.add_run(f"P: {q}")
    run.bold = True
    p2 = doc.add_paragraph()
    p2.add_run(f"R: {a}")
    p2.paragraph_format.space_after = Pt(12)

# =====================================================================
# GUARDAR
# =====================================================================
doc.save(OUT)
print(f"[OK] Manual generado: {OUT}")
