# Outlet Booking

Nightly job that aggregates outlet booking data from Oracle and writes it to Google Sheets. Runs Mon–Fri at 23:45 CET.

---

## What it does

1. Queries Oracle (`outlet_booking.sql`) for the full day's booking records — normal bookings minus clarifications
2. Groups by date, shift, HU, category, sort, quality, and clarification flag
3. Replaces today's rows in the target sheet (idempotent — safe to re-run)
4. Sends a Google Chat notification on failure

---

## Files

| File | Purpose |
|---|---|
| `databricks.yml` | Bundle definition — schedule, cluster, libraries |
| `src/outlet_booking.py` | Job entry point |
| `src/config.json` | Sheet ID, tab name, shift windows, SQL path |
| `src/outlet_booking.sql` | Oracle query (bind params: `:start_datetime`, `:end_datetime`) |

---

## Config (`src/config.json`)

```json
{
  "google_sheet": {
    "sheet_id": "<sheet-id>",
    "upload_tab": "booking_auto"
  },
  "shift_settings": {
    "timezone": "Europe/Berlin",
    "shifts": [
      { "shift_number": 1, "start_time": "05:50:00", "end_time": "14:44:59" },
      { "shift_number": 2, "start_time": "14:45:00", "end_time": "23:59:00" }
    ]
  },
  "file_paths": { "sql_query": "outlet_booking.sql" },
  "run_settings": { "days_back": 0 }
}
```

To backfill a specific date, pass `day=YYYY-MM-DD` as a job parameter when triggering manually.

---

## Deploy & Run

```bash
cd outet-booking

# Validate YAML
databricks bundle validate --target dev

# Deploy to dev
databricks bundle deploy --target dev

# Trigger manually
databricks bundle run outlet_booking --target dev
```

To test a specific date:

```bash
databricks bundle run outlet_booking --target dev --python-params '["--day", "2025-06-15"]'
```

Or set the `day` widget in the Databricks UI under job parameters.

---

## Secrets

Scope: `luu_qm_secrets`

| Key | Used for |
|---|---|
| `google_auth` | Google Sheets service account |
| `oracle_auth` | Oracle DB connection |
| `chat_webhook_url` | Google Chat failure alert |

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Duplicate rows in sheet | Job is idempotent — re-run will delete existing rows for that date and re-insert |
| Shift column shows wrong value | Shift logic is hardcoded in `outlet_booking.sql` CASE WHEN — verify Oracle timestamps are in UTC |
| No rows returned | Check `:start_datetime` / `:end_datetime` UTC conversion; confirm shift window covers the target day |
| Notification not sent | Verify `chat_webhook_url` secret is set; check logs for `Failed to send failure notification` |
