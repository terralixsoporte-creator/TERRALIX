# Terralix ERP (DTE Recibidos)

Local desktop ERP flow for Chilean DTE invoice processing in agricultural accounting.

Current version: `1.1.0` (source: `app/assets/update/__version__.py`).

## Quick Start

1. Create and activate virtualenv:
   - `python -m venv terr`
   - `terr\Scripts\activate`
2. Install dependencies:
   - `pip install -r requirements.txt`
3. Configure `data/config.env` (see docs).
4. Run app:
   - `python TERRALIX.py`
   - or `tools\run-terralix.bat`

## Main Pipeline

1. `Scrap.py` downloads PDFs from SII.
2. `ai_reader.py` extracts document + line details with OpenAI.
3. `categorizer.py` classifies accounting categories and writes to SQLite.

## Documentation

- Handover index: `docs/README.md`
- Architecture: `docs/SYSTEM_OVERVIEW.md`
- Setup and deployment: `docs/DEPLOYMENT_AND_SETUP.md`
- Operations: `docs/OPERATIONS_RUNBOOK.md`
- Data model and contracts: `docs/DATABASE_AND_DATA_CONTRACT.md`
- Handover checklist: `docs/HANDOVER_CHECKLIST.md`
- Script inventory/control: `docs/SCRIPT_CONTROL.md`

