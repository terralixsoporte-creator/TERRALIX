# Handover Checklist

## 1. Business And Ownership Transfer

- Confirm technical owner (name, team, contact channel).
- Confirm operational owner (daily execution responsibility).
- Confirm escalation owner for production incidents.
- Confirm access owner for SII credentials and OpenAI billing.

## 2. Access And Secrets Transfer

- Transfer `data/config.env` securely.
- Rotate and reissue:
  - `CLAVE_LOGIN`
  - `RUT/CLAVE`
  - `RUT2/CLAVE2`
  - `OPENAI_API_KEY`
  - `OPENAI_ASSISTANT_ID` (if used)
- Validate destination machine can run with new keys.

## 3. Infrastructure And Runtime Validation

- Python and virtualenv available.
- `pip install -r requirements.txt` successful.
- Playwright browser installed.
- `RUTA_PDF_DTE_RECIBIDOS` exists and writable.
- `DB_PATH_DTE_RECIBIDOS` exists and writable.

## 4. Functional Acceptance (Smoke Test)

- Start app and login.
- Run complete update flow once.
- Verify:
  - PDFs downloaded.
  - New documents inserted in DB.
  - `detalle` lines created.
  - `categorizer` updates classification fields.
  - report generated in `data/reportes/`.

## 5. Data Integrity Acceptance

- Run:
  - `python tools/auditar_consistencia_facturas.py --db "<DB_PATH>" --check-description-vs-pdf`
- Review high-severity findings.
- Resolve with:
  - `tools/releer_y_reparar_dte.py` when needed.

## 6. Operational Readiness

- Weekly checker enabled/disabled by policy.
- Log review routine defined.
- Backup frequency for SQLite defined.
- Incident response owner on-call confirmed.

## 7. Documentation Acceptance

- `docs/SYSTEM_OVERVIEW.md` reviewed.
- `docs/DEPLOYMENT_AND_SETUP.md` reviewed.
- `docs/OPERATIONS_RUNBOOK.md` reviewed.
- `docs/DATABASE_AND_DATA_CONTRACT.md` reviewed.
- `docs/SCRIPT_CONTROL.md` reviewed.

## 8. Sign-off Template

Use this template in handover meeting notes:

- Date:
- Outgoing owner:
- Incoming owner:
- Runtime validated on machine:
- Last successful end-to-end run timestamp:
- Open risks:
- Action items:
- Final sign-off (yes/no):

