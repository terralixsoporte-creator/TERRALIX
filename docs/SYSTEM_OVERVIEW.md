# System Overview

## Purpose

Terralix ERP automates ingestion and accounting classification of Chilean DTE invoices (received documents) for agricultural operations.

Primary outcomes:

- Standardized `documentos` and `detalle` records in SQLite.
- Accounting categories per line item.
- Operational reports for pending classification.

## High-Level Architecture

UI and orchestration:

- `TERRALIX.py`
- `app/gui/LogIn_page.py`
- `app/gui/main_app.py`
- `app/gui/actualizar_base_de_datos.py`

Core pipeline:

- `app/core/DTE_Recibidos/Scrap.py`
- `app/core/DTE_Recibidos/ai_reader.py`
- `app/core/DTE_Recibidos/categorizer.py`
- `app/core/DTE_Recibidos/dte_loader.py`

Supporting services:

- `app/core/DTE_Recibidos/pipeline_guard.py` (single-process guard lock)
- `app/core/DTE_Recibidos/weekly_background_checker.py` (background weekly SII check)

Maintenance tools:

- `tools/releer_y_reparar_dte.py`
- `tools/auditar_consistencia_facturas.py`

## End-to-End Functional Flow

1. User starts update from GUI ("Actualizar").
2. Stage 1: `Scrap.scrapear(...)` logs in to SII and downloads PDFs.
3. Stage 2: `ai_reader.read_one_pdf_with_ai(...)` extracts:
   - Header fields (issuer, giro, date, totals, references).
   - Line detail list (descripcion, cantidad, precio_unitario, monto_item, etc.).
4. Stage 2 persistence: `dte_loader.guardar_en_bd(...)` inserts/updates SQLite.
5. Stage 3: `categorizer.main()` classifies each `detalle` line:
   - Business rules.
   - Maintained keyword/provider rules.
   - In-document alignment.
   - AI category and subcategory resolution.
   - Assistant fallback for unresolved/low-confidence/ambiguous cases.
6. GUI writes and opens "sin clasificar" report.

## Classification Strategy (Current)

Classification is not single-shot. It is a layered strategy:

1. Hard business rules.
2. Maintained rules from DB.
3. Historical same-document alignment.
4. AI category choice (`choose_category`).
5. Assistant reasoning fallback (`classify_with_assistant`).
6. Subclassification on catalog (`classify_one_line`).
7. Final fallback to `SIN_CLASIFICAR` when confidence is insufficient.

## Technical Boundaries

In scope:

- Local desktop operation.
- Local SQLite DB.
- Local PDF directory.
- OpenAI API integration for extraction/classification.

Out of scope:

- Cloud object storage pipeline.
- TUF/TUFUP release publication path.

