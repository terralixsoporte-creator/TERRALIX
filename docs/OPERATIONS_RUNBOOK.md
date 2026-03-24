# Operations Runbook

## Daily Operational Flow (GUI)

1. Start app:
   - `python TERRALIX.py`
2. Login with `CLAVE_LOGIN`.
3. Open "Actualizar" tab.
4. Click "Actualizar base de datos".
5. Observe 3 stages in console:
   - `[1/3]` SII download.
   - `[2/3]` AI read.
   - `[3/3]` Accounting categorization.
6. Validate completion message and generated report path.

## Manual Operations

Manual AI read:

- Use GUI button "Leer con IA (manual)".
- Or run:
  - `python app/core/DTE_Recibidos/ai_reader.py --file "<PDF_PATH>"`.

Run categorizer only:

- `python app/core/DTE_Recibidos/categorizer.py`.

## Repair And Data Recovery

Re-read and repair detail:

- Dry run:
  - `python tools/releer_y_reparar_dte.py --db "<DB_PATH>" --from-report "<REPORT_PATH>"`
- Apply:
  - `python tools/releer_y_reparar_dte.py --db "<DB_PATH>" --from-report "<REPORT_PATH>" --apply`

Audit consistency:

- `python tools/auditar_consistencia_facturas.py --db "<DB_PATH>" --check-description-vs-pdf --report "<OUT_TXT>" --csv "<OUT_CSV>"`

Notes:

- Both tools create report artifacts under `data/reportes/`.
- Repair tool creates DB backups before destructive actions.

## Background Weekly Checker

Module:

- `app/core/DTE_Recibidos/weekly_background_checker.py`

Behavior:

- Starts with main app.
- Checks schedule/eligibility using state file.
- Runs SII scraping in background when due.
- Writes log to:
  - `data/logs/auto_weekly_dte_check.log`
- Stores state in:
  - `data/auto_weekly_dte_check_state.json`

## Monitoring Checklist

At minimum, monitor:

- `data/logs/auto_weekly_dte_check.log`
- GUI console output during update flow.
- Report files in `data/reportes/`.
- DB growth and backup snapshots in `data/backups/`.

## Failure Playbook

OpenAI failures:

- Symptoms: extraction/classification retries, quota errors.
- Actions:
  - Verify `OPENAI_API_KEY`.
  - Validate model name in `AI_OPENAI_MODEL`.
  - Check quota and retry later.

SII scraping failures:

- Symptoms: login errors, incomplete downloads.
- Actions:
  - Verify `RUT/CLAVE` and `RUT2/CLAVE2`.
  - Re-run update; scraper is idempotent on existing PDFs.
  - Review current SII UI changes if selectors fail.

DB locking or consistency issues:

- Actions:
  - Ensure only one pipeline execution at a time.
  - Use `tools/auditar_consistencia_facturas.py`.
  - Use `tools/releer_y_reparar_dte.py --apply` with backup.

