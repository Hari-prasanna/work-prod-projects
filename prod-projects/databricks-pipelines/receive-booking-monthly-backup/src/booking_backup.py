# Databricks notebook source
import os
import json
import logging
import calendar
import pandas as pd
import requests
from datetime import datetime
import pytz
import gspread
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession

# ==========================================
# 1. SETUP LOGGING & CONFIGURATION
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Toggle this to True to enable Google Chat notifications
ENABLE_NOTIFICATIONS = True

# ==========================================
# DYNAMIC PATH DETERMINATION
# ==========================================
if "__file__" in locals():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
else:
    BASE_DIR = os.getcwd()

CONFIG_PATH = os.path.join(BASE_DIR, "config.json")

def load_config():
    """Reads the JSON configuration file using dynamic relative pathing."""
    logger.info(f"Loading configuration from: {CONFIG_PATH}")
    try:
        with open(CONFIG_PATH, 'r') as file:
            return json.load(file)
    except Exception as error:
        logger.error(f"Could not load config file. Error: {error}")
        raise


# ==========================================
# 2. HELPER FUNCTIONS: TIME & CONNECTIONS
# ==========================================
def get_monthly_time_parameters(config):
    """Calculates the 1st and last day of the target month and translates to UTC."""
    tz_string = config["shift_settings"]["timezone"]
    local_tz = pytz.timezone(tz_string)

    # --- MANUAL DATE OVERRIDE ---
    # Set to True to rerun a specific historical month
    use_manual_dates = False
    manual_year = 2026
    manual_month = 4

    if use_manual_dates:
        logger.info(f"MANUAL OVERRIDE ACTIVE: Targeting Month {manual_month}, Year {manual_year}")
        naive_first_day = datetime(manual_year, manual_month, 1, 0, 0, 0)
        first_day_local = local_tz.localize(naive_first_day)

        last_day_num = calendar.monthrange(manual_year, manual_month)[1]
        naive_last_day = datetime(manual_year, manual_month, last_day_num, 23, 59, 59)
        last_day_local = local_tz.localize(naive_last_day)
    else:
        # AUTOMATIC: Use the current month
        now = datetime.now(local_tz)
        first_day_local = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        last_day_num = calendar.monthrange(now.year, now.month)[1]
        last_day_local = now.replace(day=last_day_num, hour=23, minute=59, second=59, microsecond=0)

    # Translate local Berlin time into UTC for Oracle
    utc_start_dt = first_day_local.astimezone(pytz.utc)
    utc_end_dt = last_day_local.astimezone(pytz.utc)

    start_datetime = utc_start_dt.strftime('%d.%m.%Y %H:%M:%S')
    end_datetime = utc_end_dt.strftime('%d.%m.%Y %H:%M:%S')
    target_month_string = first_day_local.strftime("%m.%Y")

    logger.info(f"Targeting Full Month (Local): {first_day_local.strftime('%d.%m.%Y')} to {last_day_local.strftime('%d.%m.%Y')}")

    return start_datetime, end_datetime, target_month_string


def get_connections():
    """Sets up connections to Google Sheets, Oracle, and grabs the Webhook."""
    logger.info("Setting up database, sheets, and webhook connections...")
    try:
        google_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="google_auth")
        gc = gspread.service_account_from_dict(json.loads(google_secret))
 
        oracle_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="oracle_auth")
        oracle_config = json.loads(oracle_secret)
 
        user = oracle_config['user']
        password = oracle_config['password']
        host = oracle_config['host']
        port = oracle_config['port']
        service = oracle_config['service']
        connection_string = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service}"
        engine = create_engine(connection_string)
 
        webhook_url = dbutils.secrets.get(scope="luu_qm_secrets", key="chat_webhook_url")
 
        return gc, engine, webhook_url
    except Exception as error:
        logger.error(f"Connection setup failed: {error}")
        raise


def send_chat_notification(webhook_url, results, target_month_string, sheet_url,
                           is_success=True, error_text=None, sop_link=None):
    """Sends a detailed Google Chat card summarising the pipeline run."""
    
    # 1. Determine Status Styling
    if is_success:
        status_color = "#188038" # Green
        status_text = "Aktualisierung erfolgreich"
        status_icon = "check_circle"
    else:
        status_color = "#D93025" # Red
        status_text = "Aktualisierung fehlgeschlagen"
        status_icon = "cancel"

    # 2. Build the Card Widgets
    widgets = [
        {"decoratedText": {
            "startIcon": {"materialIcon": {"name": status_icon}},
            "text": f'<font color="{status_color}">{status_text}</font>',
        }},
        {"decoratedText": {
            "startIcon": {"materialIcon": {"name": "calendar_month"}},
            "topLabel": "Aktualisierungsmonat",
            "text": target_month_string,
        }},
    ]

    # 3. Add Report-Specific Details (for Success)
    if is_success:
        for r in results:
            if r.get("no_data"):
                widgets.append({"decoratedText": {
                    "startIcon": {"materialIcon": {"name": "block"}},
                    "text": f"Keine {r['name']}-Buchungen gefunden",
                }})
            else:
                widgets.append({"decoratedText": {
                    "startIcon": {"materialIcon": {"name": "table_rows"}},
                    "topLabel": f"{r['name']} Report",
                    "text": f"{r['rows']:,} Zeilen eingefügt",
                }})
        
        if sheet_url:
            widgets.append({"buttonList": {"buttons": [{
                "text": "Tabelle öffnen",
                "onClick": {"openLink": {"url": sheet_url}},
            }]}})
    
    # 4. Add Error Handling & SOP Link (for Failure)
    else:
        widgets.append({"decoratedText": {
            "startIcon": {"materialIcon": {"name": "error"}},
            "topLabel": "Fehler-Log",
            "text": error_text or "Unbekannter Fehler",
            "wrapText": True,
        }})
        widgets.append({"decoratedText": {
            "startIcon": {"materialIcon": {"name": "build"}},
            "text": "Bitte den manuellen Aktualisierungsprozess durchführen.",
            "wrapText": True,
        }})
        
        if sop_link:
            widgets.append({"buttonList": {"buttons": [{
                "text": "SOP öffnen",
                "onClick": {"openLink": {"url": sop_link}},
            }]}})

    payload = {
        "cardsV2": [{
            "cardId": "pipeline_report",
            "card": {
                "header": {
                    "title": "ZFS/B-Beauty Monatsreporting",
                    "subtitle": "Databricks Automatisierungsstatus",
                },
                "sections": [{"widgets": widgets}],
            }
        }]
    }

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Advanced Webhook notification sent.")
    except Exception as e:
        logger.error(f"Failed to send webhook notification: {e}")


# ==========================================
# 3. DATA PROCESSING
# ==========================================
def fetch_and_enrich_data(engine, spark, config, report_config, start_dt, end_dt):
    """Extracts Oracle data, joins Databricks, and organises the columns."""

    sql_file_path = os.path.join(BASE_DIR, report_config['sql_query_file'])
    with open(sql_file_path, 'r') as file:
        query_text = file.read()

    logger.info(f"Running SQL query for {report_config['name']}...")
    with engine.connect() as connection:
        oracle_df = pd.read_sql(
            text(query_text),
            connection,
            params={"start_datetime": start_dt, "end_datetime": end_dt}
        )

    if oracle_df.empty:
        return oracle_df

    spark_df = spark.createDataFrame(oracle_df)
    
    ean_table = spark.table(config["databricks_catalogs"]["ean_mapping_table"]) \
        .selectExpr("ean as EAN", "brand_name as BRAND_NAME") \
        .dropDuplicates(["EAN"])

    enriched_df = spark_df.join(ean_table, on="EAN", how="left")
    final_df = enriched_df.toPandas()

    final_df.columns = [str(col).upper() for col in final_df.columns]

    # Apply dummy brands and fill blanks
    custom_brands_dict = config.get("custom_brands", {})
    final_df['BRAND_NAME'] = (
        final_df['EAN'].astype(str).map(custom_brands_dict).fillna(final_df['BRAND_NAME'])
    )
    final_df['BRAND_NAME'] = final_df['BRAND_NAME'].fillna("no brand info")
    
    # Rename and Order Columns
    rename_map = report_config.get("column_rename_map", {})
    columns_to_keep = report_config.get("columns_to_keep", [])
    final_df = final_df.rename(columns=rename_map)
    actual_cols = [col for col in columns_to_keep if col in final_df.columns]
    final_df = final_df[actual_cols].where(pd.notnull(final_df), None)

    return final_df


# ==========================================
# 4. GOOGLE SHEETS MANAGEMENT
# ==========================================
def manage_google_sheets(gc, sheet_id, report_name, df_new, target_month_string):
    """Handles the _old backup and creates the fresh target sheet."""
    target_sheet_name = f"{report_name} {target_month_string}"
    backup_sheet_name = f"Backup_{target_sheet_name}_old"
    doc = gc.open_by_key(sheet_id)

    # 1. Clear old backup
    try:
        doc.del_worksheet(doc.worksheet(backup_sheet_name))
        logger.info(f"Deleted previous backup: {backup_sheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        pass

    # 2. Archive current sheet
    try:
        doc.worksheet(target_sheet_name).update_title(backup_sheet_name)
        logger.info(f"Renamed current sheet to: {backup_sheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        pass

    # 3. Upload new data
    rows, cols = df_new.shape
    new_ws = doc.add_worksheet(title=target_sheet_name, rows=rows + 100, cols=cols + 5)
    new_ws.append_row(df_new.columns.tolist())
    rows_to_upload = df_new.values.tolist()
    new_ws.append_rows(rows_to_upload, value_input_option='USER_ENTERED')

    logger.info(f"Success! Uploaded {len(rows_to_upload)} rows to {target_sheet_name}.")
    return len(rows_to_upload)


# ==========================================
# 5. THE MAIN SCRIPT
# ==========================================
def main():
    logger.info("--- Starting Monthly Pipeline ---")
    spark = SparkSession.builder.getOrCreate()
    webhook_url = None
    target_month_string = "n/a"
    sop_link = None
    results = []
    sheet_url = None

    try:
        config = load_config()
        sop_link = config.get("sop_link") # Pulled from config.json
        gc, engine, webhook_url = get_connections()

        start_dt, end_dt, target_month_string = get_monthly_time_parameters(config)

        for report in config["reports"]:
            logger.info(f"--- Processing Report: {report['name']} ---")
            df_final = fetch_and_enrich_data(engine, spark, config, report, start_dt, end_dt)

            if df_final.empty:
                logger.warning(f"No data found for {report['name']}. Skipping sheet update.")
                results.append({"name": report["name"], "rows": 0, "no_data": True})
                continue

            rows_inserted = manage_google_sheets(gc, report["google_sheet_id"], report["name"], df_final, target_month_string)
            results.append({"name": report["name"], "rows": rows_inserted, "no_data": False})
            
            # Construct sheet URL for the notification button
            sheet_url = f"https://docs.google.com/spreadsheets/d/{report['google_sheet_id']}/edit"

        logger.info("--- Pipeline Completed Successfully ---")

        if ENABLE_NOTIFICATIONS and webhook_url:
            send_chat_notification(webhook_url, results, target_month_string, sheet_url, is_success=True)

    except Exception as error:
        logger.critical(f"Job failed! Error: {error}")
        if ENABLE_NOTIFICATIONS and webhook_url:
            send_chat_notification(
                webhook_url, [], target_month_string, None,
                is_success=False, error_text=str(error), sop_link=sop_link
            )
        raise error

if __name__ == "__main__":
    main()