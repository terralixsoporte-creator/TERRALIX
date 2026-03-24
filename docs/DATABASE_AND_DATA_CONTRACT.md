# Database And Data Contract

## Database Engine

- SQLite
- Main file: `DteRecibidos_db.db`
- Typical location configured by `DB_PATH_DTE_RECIBIDOS`

## Core Tables

### `documentos`

Purpose:

- Header-level DTE data (one row per document).

Key columns:

- `id_doc` (PK, deterministic business identifier)
- `tipo_doc`
- `folio`
- `rut_emisor`
- `razon_social`
- `giro`
- `fecha_emision`
- `monto_total`
- `referencia`
- `DTE_referencia`
- `ruta_pdf`
- `fecha_carga`

### `detalle`

Purpose:

- Line-level invoice details for each document.

Key columns:

- `id_det` (PK)
- `id_doc` (FK -> documentos.id_doc)
- `linea`
- `codigo`
- `descripcion`
- `cantidad`
- `precio_unitario`
- `monto_item`
- `catalogo_costo_id`
- `needs_review`
- `categoria`
- `subcategoria`
- `tipo_gasto`
- `confianza_categoria`
- `confianza_subcategoria`
- `origen_clasificacion`
- `motivo_clasificacion`
- `confianza_ia`

### `catalogo_costos`

Purpose:

- Controlled accounting catalog for classification targets.

Key columns:

- `id` (PK)
- `categoria_costo`
- `subcategoria_costo`
- `tipo_gasto`

### `mantenedor_categoria_proveedor`

Purpose:

- Provider/giro learned mapping for category shortcuts.

Key columns:

- `razon_social`
- `giro`
- `categoria`
- `confianza_categoria`
- `fecha_actualizacion`

### `mantenedor_keyword_categoria`

Purpose:

- Maintained keyword-to-category/subcategory rules.

Key columns:

- `keyword`
- `categoria`
- `subcategoria`
- `tipo_gasto`
- `prioridad`
- `fecha_actualizacion`

## Data Contracts By Stage

Stage 1 (`Scrap.py`) output contract:

- Local PDF file named in pattern `Tipo_RUT_Folio.pdf`.

Stage 2 (`ai_reader.py` + `dte_loader.py`) contract:

- Inserts/updates `documentos`.
- Inserts/updates `detalle`.
- Preserves deterministic `id_doc` from filename-derived identity.

Stage 3 (`categorizer.py`) contract:

- Writes accounting fields in `detalle`.
- Resolves `catalogo_costo_id` from `catalogo_costos`.
- Marks uncertain rows via `needs_review`.

## Quality Gates

Minimum checks before handoff acceptance:

- No orphan `detalle` rows.
- No duplicated `linea` within same `id_doc`.
- Acceptable proportion of `needs_review=1`.
- `id_doc` format consistency with `Tipo_RUT_Folio`.

Recommended command:

- `python tools/auditar_consistencia_facturas.py --db "<DB_PATH>" --check-description-vs-pdf`

