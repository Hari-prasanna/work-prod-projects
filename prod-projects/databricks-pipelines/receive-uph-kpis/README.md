# receive-uph-kpis

Nightly units-per-hour (UPH) KPIs for the receive area. The job runs one Oracle
query for the day's shift window and appends the result to a Google Sheet. It is
one of the jobs built on the shared `dbricks-utils` helpers.

## How it works

1. **Schedule.** `0 35 23 ? * MON-FRI` (Europe/Berlin) — nightly after the last
   shift, shipped `PAUSED` on dev.
2. **Time window.** `get_utc_window(config)` converts the Berlin shift start/end
   (from `config.json`) to the UTC strings the query binds to. This keeps the
   query timezone-correct regardless of where the cluster runs.
3. **Extract.** `run_sql_file` executes `uph_kpis_query.sql` with the
   `start_datetime` / `end_datetime` parameters.
4. **Load (idempotent).** `update_google_sheet_idempotent` deletes any existing
   rows for the target date before appending, so a re-run never duplicates a day.
5. **Failure handling.** Exceptions are logged and re-raised so the Databricks
   job is marked failed.

## Project layout

```
receive-uph-kpis/
├── databricks.yml
├── requirements.txt
└── src/
    ├── receive_uph_kpis.py     # entry point (spark_python_task)
    ├── uph_kpis_query.sql      # parameterised query
    ├── ui_test_notebook.ipynb  # scratch / manual test notebook
    ├── config.template.json    # copy to config.json (gitignored)
    └── config.json             # sheet id, tab, shift window (local only)
```

## Shared utilities

The entry point appends `UTILS_DIR` to `sys.path` and imports
`common_utils` (`get_connections`, `load_config`, `run_sql_file`,
`get_utc_window`, `update_google_sheet_idempotent`). Both `PROJECT_DIR` and
`UTILS_DIR` are set at the top of `receive_uph_kpis.py` — update them to match
your workspace paths.

## Configuration

```bash
cp src/config.template.json src/config.json
# set google_sheet.sheet_id / upload_tab and the shift_settings window
```

## Secrets

Connections come from `common_utils.get_connections`, which reads the
`luu_qm_secrets` scope (`oracle_auth`, `google_auth`, `chat_webhook_url`).

## Deploy & run

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run receive_uph_kpis -t dev
```

## Troubleshooting

- **Duplicate-looking rows.** Shouldn't happen — the load is idempotent on the
  date column. If it does, confirm the date format written matches the values
  already in column 1.
- **`config.json` not found.** `PROJECT_DIR` in the entry script must point at the
  deployed `src/` folder.
- **Import error on `common_utils`.** `UTILS_DIR` is wrong or the shared module
  hasn't been deployed to that path.
