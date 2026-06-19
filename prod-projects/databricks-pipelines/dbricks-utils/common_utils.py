import os
import json
import logging
import urllib.request
from datetime import datetime, timedelta
import pytz
import pandas as pd
import gspread
from sqlalchemy import create_engine, text

def setup_logging(name=__name__):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    return logging.getLogger(name)

_log = setup_logging(__name__)

def get_connections(dbutils, logger=None):
    """Sets up connections to Google Sheets, Oracle, and grabs the Webhook."""
    log = logger or _log
    log.info("Setting up database, sheets, and webhook connections...")
    try:
        # Google
        google_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="google_auth")
        gc = gspread.service_account_from_dict(json.loads(google_secret))

        # Oracle
        oracle_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="oracle_auth")
        cfg = json.loads(oracle_secret)
        conn_str = f"oracle+oracledb://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/?service_name={cfg['service']}"
        engine = create_engine(conn_str)

        # Webhook
        webhook_url = dbutils.secrets.get(scope="luu_qm_secrets", key="chat_webhook_url").strip().strip('"')

        return gc, engine, webhook_url
    except Exception as error:
        log.error(f"Connection setup failed: {error}")
        raise

def load_config(base_dir=None, filename="config.json"):
    folder = base_dir if base_dir is not None else os.getcwd()
    with open(os.path.join(folder, filename), "r") as f:
        return json.load(f)

def run_sql_file(engine, sql_path, params=None):
    with open(sql_path, "r") as f:
        query_text = f.read()
    with engine.connect() as conn:
        df = pd.read_sql(text(query_text), conn, params=params or {})
    return df

def get_utc_window(config, days_back=0):
    local_tz = pytz.timezone(config["shift_settings"]["timezone"])
    target_day = datetime.now(local_tz) - timedelta(days=days_back)
    ymd = target_day.strftime('%Y-%m-%d')

    shifts = config["shift_settings"].get("shifts")
    if shifts:
        start_time = shifts[0]["start_time"]
        end_time = shifts[-1]["end_time"]
    else:
        start_time = config["shift_settings"]["start_time"]
        end_time = config["shift_settings"]["end_time"]

    b_start = local_tz.localize(datetime.strptime(f"{ymd} {start_time}", "%Y-%m-%d %H:%M:%S"))
    b_end = local_tz.localize(datetime.strptime(f"{ymd} {end_time}", "%Y-%m-%d %H:%M:%S"))

    return (b_start.astimezone(pytz.utc).strftime('%d.%m.%Y %H:%M:%S'),
            b_end.astimezone(pytz.utc).strftime('%d.%m.%Y %H:%M:%S'),
            ymd)

def send_webhook_notification(webhook_url: str, job_name: str, status: str, error: Exception = None, logger=None):
    """Posts a Google Chat card to the webhook for job success or failure."""
    log = logger or _log
    berlin_tz = pytz.timezone("Europe/Berlin")
    timestamp = datetime.now(berlin_tz).strftime("%Y-%m-%d %H:%M:%S %Z")

    if status == "failure":
        header = f"❌ {job_name} — Job Failed"
        widgets = [
            {"keyValue": {"topLabel": "Job", "content": job_name}},
            {"keyValue": {"topLabel": "Date", "content": timestamp}},
            {"keyValue": {"topLabel": "Reason", "content": str(error) if error else "Unknown error"}},
        ]
    else:
        header = f"✅ {job_name} — Completed"
        widgets = [
            {"keyValue": {"topLabel": "Job", "content": job_name}},
            {"keyValue": {"topLabel": "Date", "content": timestamp}},
        ]

    card = {
        "cards": [{
            "header": {"title": header},
            "sections": [{"widgets": widgets}],
        }]
    }

    try:
        payload = json.dumps(card).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        log.info(f"Webhook notification sent: {status}")
    except Exception as exc:
        log.error(f"Failed to send webhook notification: {exc}")

def update_google_sheet_idempotent(sheet, df, match_value, date_col_index=1, logger=None):
    log = logger or _log
    if df.empty: return
    df.columns = [str(col).upper() for col in df.columns]
    df = df.where(pd.notnull(df), None)
    
    vals = sheet.col_values(date_col_index)
    rows = [i + 1 for i, v in enumerate(vals) if v == str(match_value)]
    if rows:
        log.info(f"Deleting {len(rows)} rows for {match_value}")
        sheet.delete_rows(rows[0], rows[-1])
        vals = sheet.col_values(date_col_index)

    if not vals: sheet.append_row(df.columns.tolist())
    sheet.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")