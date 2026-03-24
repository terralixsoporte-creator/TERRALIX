# Script Control

This file centralizes follow-up and maintenance notes for active Python scripts.
Use it as the checklist before changing behavior in production.

## Active Flow
1. `TERRALIX.py` -> app entry and login launch.
2. `app/gui/LogIn_page.py` -> access validation and main app open.
3. `app/gui/main_app.py` -> tab shell for update workflow.
4. `app/gui/actualizar_base_de_datos.py` -> orchestrates 3 stages.
5. `app/core/DTE_Recibidos/Scrap.py` -> stage 1 download.
6. `app/core/DTE_Recibidos/ai_reader.py` -> stage 2 AI read.
7. `app/core/DTE_Recibidos/categorizer.py` -> stage 3 categorization.

## Script Inventory
| Script | Purpose | Inputs | Outputs | Control checkpoints |
|---|---|---|---|---|
| `TERRALIX.py` | Entry point and GUI bootstrap | `data/config.env` | Starts login UI | App starts without traceback; env loaded |
| `app/gui/LogIn_page.py` | Login view + password validation | `CLAVE_LOGIN`, UI assets | Opens main app on valid password | Enter/Esc shortcuts work; missing asset fallback works |
| `app/gui/main_app.py` | Main shell + menu | Login window context | `Actualizar` tab UI | Notebook and icon load correctly |
| `app/gui/actualizar_base_de_datos.py` | End-to-end update pipeline and report | PDF path, DB path, env toggles | Updated DB + generated report file | Buttons disabled/enabled correctly; report opens on finish |
| `app/gui/utils.py` | Shared GUI helpers | Tk window object | Confirm close behavior | Exit confirmation and parent close behavior |
| `app/core/DTE_Recibidos/Scrap.py` | Scrapes SII and downloads PDFs | Credentials/env, Playwright | PDFs (local), progress callbacks | Login works, pagination ends, retries handled |
| `app/core/DTE_Recibidos/ai_reader.py` | Reads PDFs via AI and persists | PDF files, OpenAI key/model, DB path | Document/detail rows in DB | Skip duplicates, retries, temp cleanup |
| `app/core/DTE_Recibidos/categorizer.py` | Classifies detail rows | SQLite DB, catalog, optional OpenAI | Updated classification columns | Handles no catalog, fallback, confidence fields |
| `app/core/DTE_Recibidos/dte_loader.py` | DB schema and persistence utilities | DB path, OCR/AI optional deps | Created/updated tables and rows | Schema migration, insert/update safety |
| `hooks/runtime_playwright_browsers_path.py` | PyInstaller runtime hook for Playwright | Frozen/dev runtime env | `PLAYWRIGHT_BROWSERS_PATH` | Browser path resolved in frozen and dev modes |
| `app/assets/update/__version__.py` | App version source | Manual version edit | Version string used by app/release | Version format `X.Y.Z` and consistent with release |

## Removed In Cleanup
- `build/`, `dist/`, `tools/dist/`, `__pycache__/` (generated artifacts)
- `app/gui/dashboard.py` (legacy UI not referenced)
- `app/gui/comingsoon.py` (legacy UI not referenced)
- `app/core/DTE_Recibidos/categorizer_original.py` (obsolete backup)
- `app/core/DTE_Recibidos/exporter.py` (not referenced)
- `app/core/DTE_Recibidos/supabase_sync.py` (not referenced)
- `app/core/DTE_Recibidos/supabase_utils.py` (not referenced)
- `app/core/DTE_Recibidos/s3_utils.py` (flujo S3 removido; solo local)
- `tools/make_release.py` (release TUF/S3 removido)
- `tools/run_local_repo.py` (repo local TUF removido)
- `.tufup-repo-config`, `root.json`, `tufrepo/`, `app/assets/update/root.json` (artefactos TUF removidos)

## Follow-up Routine
1. Before editing, mark affected scripts from the table.
2. Run `python -m py_compile` on touched scripts.
3. Run one end-to-end update from GUI and verify report generation.
4. Keep this file updated when adding/removing scripts.
