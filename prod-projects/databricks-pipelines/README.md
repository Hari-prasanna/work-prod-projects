# Databricks pipelines

Scheduled jobs that run on the Zalando Databricks workspace, deployed as
[Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html).
They share one set of helpers and follow the same secrets and config conventions.

## Pipelines

| Folder | Schedule | Source → sink | README |
| :--- | :--- | :--- | :--- |
| `oracle-to-looker-etl` | 05:40 & 14:40, Mon–Fri | Oracle → Google Sheets (Looker DG dashboard) | [link](oracle-to-looker-etl) |
| `realtime-data-stream` | Every 5 min during shifts | Oracle → Google Sheets (Grafana TV) | [link](realtime-data-stream) |
| `receive-booking-monthly-backup` | 23:30 on the last day of the month | Oracle → Google Sheets | [link](receive-booking-monthly-backup) |
| `receive-uph-kpis` | 23:35 nightly, Mon–Fri | Oracle → Google Sheets | [link](receive-uph-kpis) |
| `shift-report-daily-update` | 23:35 nightly, Mon–Fri | Oracle → Google Sheets | [link](shift-report-daily-update) |

## Shared helpers — `dbricks-utils/common_utils.py`

The newer jobs (`receive-uph-kpis`, `shift-report-daily-update`) import a single
module instead of re-implementing boilerplate. It provides:

- `get_connections(dbutils)` — builds the gspread client, the Oracle SQLAlchemy
  engine, and resolves the Chat webhook from the secret scope.
- `load_config(base_dir)` — reads a project's `config.json`.
- `run_sql_file(engine, path, params)` — runs a parameterised `.sql` file and
  returns a DataFrame.
- `get_utc_window(config, days_back)` — converts a Berlin shift window to the
  UTC strings the Oracle queries expect.
- `update_google_sheet_idempotent(...)` — deletes rows for the target date
  before re-appending, so re-runs don't duplicate data.
- `send_webhook_notification(...)` — posts a success/failure card to Google Chat.

On the workspace the module lives at a shared path; each job appends that path to
`sys.path` before importing. The path is defined as `UTILS_DIR` at the top of each
job's entry script.

> The older jobs (`oracle-to-looker-etl`, `realtime-data-stream`,
> `receive-booking-monthly-backup`) predate this module and still carry their own
> inline connection/notification code. Migrating them onto `common_utils` is the
> natural next refactor.

## Working with a bundle

Run these from inside a pipeline folder (the one containing `databricks.yml`):

```bash
databricks bundle validate -t dev      # check the bundle parses and resolves
databricks bundle deploy   -t dev      # push job + files to the workspace
databricks bundle run <job_key> -t dev # trigger a run
```

Most jobs ship `pause_status: PAUSED` on `dev` so deploying doesn't immediately
start a schedule. Flip to `prod` (`-t prod`) once verified.

## Configuration & secrets

1. Copy `src/config.template.json` to `src/config.json` and fill in the Sheet IDs
   and paths for your workspace. `config.json` is gitignored.
2. Create the secret scope and add the credentials the job reads (Oracle auth,
   Google service-account JSON, and — where used — the Chat webhook URL). The
   exact scope name and keys are listed in each pipeline's README.

> On Linux/macOS, wrap secret values containing `&` or `?` in single quotes when
> using `databricks secrets put-secret`, or the shell will truncate them.
