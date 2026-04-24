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

BASE_DIR = "/Workspace/Users/hari.prasanna.ravichandran@zalando.de/team-repo/databricks/receive-booking-monthly-backup/src"
CONFIG_PATH = f"{BASE_DIR}/config.json"

# Toggle this to True when you're ready to send Google Chat notifications
ENABLE_NOTIFICATIONS = False


def load_config():
    """Reads the JSON configuration file."""
    logger.info("Loading configuration file...")
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
    # To run a past month, change 'use_manual_dates' to True and set your year/month.
    use_manual_dates = True
    manual_year = 2026
    manual_month = 3

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
        # 1. Connect to Google Sheets
        google_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="google_auth")
        gc = gspread.service_account_from_dict(json.loads(google_secret))

        # 2. Connect to Oracle Database
        oracle_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="oracle_auth")
        oracle_config = json.loads(oracle_secret)

        # Split out the config values for readability
        user = oracle_config['user']
        password = oracle_config['password']
        host = oracle_config['host']
        port = oracle_config['port']
        service = oracle_config['service']
        connection_string = f"oracle+oracledb://{user}:{password}@{host}:{port}/?service_name={service}"
        engine = create_engine(connection_string)

        # 3. Get Google Chat Webhook
        webhook_url = dbutils.secrets.get(scope="luu_qm_secrets", key="chat_webhook_url")

        return gc, engine, webhook_url
    except Exception as error:
        logger.error(f"Connection setup failed: {error}")
        raise


def send_chat_notification(webhook_url, message, is_success=True):
    """Sends a formatted message to Google Chat via Webhook."""
    color = "#00FF00" if is_success else "#FF0000"
    payload = {
        "cardsV2": [{
            "cardId": "pipeline_alert",
            "card": {
                "header": {
                    "title": "Monthly Data Pipeline Report",
                    "subtitle": "B-Beauty & ZFS Automation Status"
                },
                "sections": [{
                    "widgets": [{"textParagraph": {"text": f"<font color=\"{color}\">{message}</font>"}}]
                }]
            }
        }]
    }
    try:
        requests.post(webhook_url, json=payload)
        logger.info("Webhook notification sent.")
    except Exception as e:
        logger.error(f"Failed to send webhook notification: {e}")


# ==========================================
# 3. DATA PROCESSING
# ==========================================
def fetch_and_enrich_data(engine, spark, config, report_config, start_dt, end_dt):
    """Extracts Oracle data, joins Databricks, and organises the columns."""

    # --- STEP 1: Get Oracle Data ---
    sql_file_path = f"{BASE_DIR}/{report_config['sql_query_file']}"
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

    logger.info(f"Retrieved {len(oracle_df)} records. Moving to Spark...")

    # --- STEP 2: Join with Databricks Table in Spark ---
    spark_df = spark.createDataFrame(oracle_df)

    # Extract BRAND_NAME from the Databricks table
    ean_table = spark.table(config["databricks_catalogs"]["ean_mapping_table"]) \
        .selectExpr("ean as EAN", "brand_name as BRAND_NAME") \
        .dropDuplicates(["EAN"])

    enriched_df = spark_df.join(ean_table, on="EAN", how="left")
    final_df = enriched_df.toPandas()

    logger.info("Join complete. Converting back to Pandas...")

    # --- STEP 3: Clean up and Order Columns (Pandas) ---
    # Force all columns to uppercase for a predictable baseline
    final_df.columns = [str(col).upper() for col in final_df.columns]

    # Apply custom dummy brands using .map() — faster than row-by-row .apply()
    custom_brands_dict = config.get("custom_brands", {})
    final_df['BRAND_NAME'] = (
        final_df['EAN']
        .astype(str)
        .map(custom_brands_dict)
        .fillna(final_df['BRAND_NAME'])
    )

    # Pull rename map and column order from config so you don't need to edit
    # this script when adding new reports — just update config.json instead
    rename_map = report_config.get("column_rename_map", {})
    columns_to_keep = report_config.get("columns_to_keep", [])

    final_df = final_df.rename(columns=rename_map)

    # Only keep columns that actually exist to prevent errors
    actual_cols = [col for col in columns_to_keep if col in final_df.columns]
    final_df = final_df[actual_cols]

    # Replace empty values with None for Google Sheets
    final_df = final_df.where(pd.notnull(final_df), None)

    return final_df


# ==========================================
# 4. GOOGLE SHEETS MANAGEMENT
# ==========================================
def manage_google_sheets(gc, sheet_id, report_name, df_new, target_month_string):
    """Handles the _old backup and creates the fresh target sheet."""

    target_sheet_name = f"{report_name} {target_month_string}"
    backup_sheet_name = f"Backup_{target_sheet_name}_old"

    doc = gc.open_by_key(sheet_id)

    # --- STEP 1: Delete any existing "_old" backup so we don't get duplicates ---
    try:
        old_worksheet = doc.worksheet(backup_sheet_name)
        doc.del_worksheet(old_worksheet)
        logger.info(f"Deleted previous backup: {backup_sheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        pass

    # --- STEP 2: Find the current sheet and rename it to "_old" ---
    try:
        current_worksheet = doc.worksheet(target_sheet_name)
        current_worksheet.update_title(backup_sheet_name)
        logger.info(f"Renamed current sheet to: {backup_sheet_name}")
    except gspread.exceptions.WorksheetNotFound:
        pass

    # --- STEP 3: Create the brand new sheet and upload data ---
    logger.info(f"Creating fresh sheet: {target_sheet_name}")
    rows, cols = df_new.shape
    new_ws = doc.add_worksheet(title=target_sheet_name, rows=rows + 100, cols=cols + 5)

    logger.info("Uploading data to new sheet...")
    new_ws.append_row(df_new.columns.tolist())
    rows_to_upload = df_new.values.tolist()
    new_ws.append_rows(rows_to_upload, value_input_option='USER_ENTERED')

    logger.info(f"Success! Uploaded {len(rows_to_upload)} rows to {target_sheet_name}.")


# ==========================================
# 5. THE MAIN SCRIPT
# ==========================================
def main():
    logger.info("--- Starting Monthly Pipeline ---")
    spark = SparkSession.builder.getOrCreate()
    webhook_url = None

    try:
        config = load_config()
        gc, engine, webhook_url = get_connections()

        start_dt, end_dt, target_month_string = get_monthly_time_parameters(config)

        # Loop through each report defined in config.json
        for report in config["reports"]:
            logger.info(f"--- Processing Report: {report['name']} ---")

            df_final = fetch_and_enrich_data(engine, spark, config, report, start_dt, end_dt)

            if df_final.empty:
                logger.warning(f"No data found for {report['name']}. Skipping sheet update.")
                continue

            manage_google_sheets(gc, report["google_sheet_id"], report["name"], df_final, target_month_string)

        logger.info("--- Pipeline Completed Successfully ---")

        if ENABLE_NOTIFICATIONS and webhook_url:
            success_msg = f"✅ Success: Monthly automation completed for {len(config['reports'])} reports."
            send_chat_notification(webhook_url, success_msg, is_success=True)

    except Exception as error:
        logger.critical(f"Job failed! Error: {error}")

        if ENABLE_NOTIFICATIONS and webhook_url:
            fail_msg = f"❌ FAILED: Monthly automation crashed. Error: {str(error)}"
            send_chat_notification(webhook_url, fail_msg, is_success=False)

        raise error


if __name__ == "__main__":
    main()