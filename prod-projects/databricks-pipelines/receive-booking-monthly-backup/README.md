# receive-booking-monthly-backup

Month-end snapshot of overstock booking data for the **B-Beauty** and **ZFS**
reports. On the last day of each month the job pulls the full month of bookings
from Oracle, enriches each EAN with its brand name, archives the previous month's
tab, and writes a fresh tab into the master Google Sheet. It then posts a summary
card to Google Chat.

This removes a recurring manual routine of downloading reports, filtering them,
and looking up brand names by hand.

**Destination sheet:** configured per report in `config.json` (`reports[].google_sheet_id`).

## How it works

1. **Schedule.** Runs at 23:30 on the last day of the month
   (`0 30 23 L * ?`, Europe/Berlin).
2. **Extract.** Connects to Oracle and pulls the month's B-Beauty and ZFS
   overstock bookings.
3. **Enrich.** Resolves each EAN to a brand name via the internal catalog
   (`zalando_shared` Unity Catalog table). Dummy EANs are labelled from the
   `custom_brands` map in config; anything still unresolved is written as
   `"no brand info"` so there are no blank cells.
4. **Load.** Moves the current data to a backup tab and creates a new monthly tab
   (e.g. `B-Beauty 05.2026`).
5. **Notify.** Posts a success card (row counts + "Tabelle öffnen") or a failure
   card (with an "SOP öffnen" link to recovery steps).

![Pipeline mapping](assets/receive-monthly-update.png)

## Project layout

```
receive-booking-monthly-backup/
├── databricks.yml
├── requirements.txt
└── src/
    ├── booking_backup.py        # the job (Databricks notebook-source format)
    ├── b_beauty_query.sql       # B-Beauty extract
    ├── zfs_query.sql            # ZFS extract
    ├── booking_script_test.ipynb# scratch / manual test notebook
    ├── config.template.json     # copy to config.json (gitignored)
    └── config.json              # sheet IDs, custom_brands, SOP link (local only)
```

## Configuration

```bash
cp src/config.template.json src/config.json
```

Then set, per report: `google_sheet_id`, the `column_rename_map`,
`columns_to_keep`, the `custom_brands` (dummy-EAN labels), the `sop_link`, and the
`databricks_catalogs.ean_mapping_table`. Notifications are toggled by
`ENABLE_NOTIFICATIONS` at the top of `booking_backup.py`.

## Secrets

Uses the shared `luu_qm_secrets` scope (`oracle_auth`, `google_auth`,
`chat_webhook_url`). Unity Catalog access requires the single-user cluster mode
set in `databricks.yml`.

## Deploy & run

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run receive_booking_monthly_backup -t dev
```

## Troubleshooting

- **Report missing on the 1st.** Check the Chat channel for a failure card and
  follow the SOP link in it.
- **Brand column shows "no brand info" widely.** The catalog lookup likely failed
  (cluster not in single-user mode, or the `ean_mapping_table` path changed).
- **Wrong month tab created.** The job keys off the run date; re-running on a
  different day produces a different tab name.
