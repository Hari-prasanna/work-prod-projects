# shift-report-daily-update

Nightly shift report for the LUU floor. Runs an Oracle query for the day's shift
window and writes the result to a Google Sheet. Built on the shared
`dbricks-utils` helpers, and supports an optional date widget for backfilling a
specific day.

## How it works

1. **Schedule.** `0 35 23 ? * MON-FRI` (Europe/Berlin), shipped `UNPAUSED`. The
   bundle also emails on failure.
2. **Target date.** A `day` job widget (blank = today) lets you re-run for a past
   date; the script converts it to a `days_back` offset.
3. **Time window.** `get_utc_window(config, days_back)` builds the UTC window from
   the Berlin shift settings in `config.json`.
4. **Extract → load.** Runs `shift_report.sql` and writes via
   `update_google_sheet_idempotent`, keyed on the formatted date so re-runs
   overwrite rather than duplicate.
5. **Failure notification.** On error it posts a failure card to Google Chat and
   re-raises so the job is marked failed.

> `report_script.py` has a `TEST_WEBHOOK_URL` constant for routing
> notifications to a test space during development — leave it empty for prod.

## Project layout

```
shift-report-daily-update/
├── databricks.yml
├── requirements.txt
└── src/
    ├── report_script.py            # entry point (spark_python_task)
    ├── shift_report.sql            # parameterised query
    ├── report_script_notebook.ipynb# scratch / manual test notebook
    ├── config.template.json        # copy to config.json (gitignored)
    └── config.json                 # sheet id, tab, shift window (local only)
```

## Shared utilities

Same pattern as the other helper-based jobs: `PROJECT_DIR` and `UTILS_DIR` at the
top of `report_script.py` point at the project `src/` and the shared
`dbricks-utils` module. `resolve_dbutils()` also lets the script run outside a
notebook context.

## Configuration

```bash
cp src/config.template.json src/config.json
# set google_sheet.sheet_id / upload_tab and the shift_settings window
```

## Secrets

`common_utils.get_connections` reads the `luu_qm_secrets` scope (`oracle_auth`,
`google_auth`, `chat_webhook_url`).

## Deploy & run

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run shift_report_pilot -t dev
# backfill a specific day:
databricks bundle run shift_report_pilot -t dev -- --day 2026-06-15
```

## Troubleshooting

- **Wrong day's data.** Check the `day` widget value and the shift window in
  config; the window is Berlin-local and converted to UTC at query time.
- **No failure email.** Confirm the address in `email_notifications` in
  `databricks.yml` and that the job is deployed to the target you're running.
