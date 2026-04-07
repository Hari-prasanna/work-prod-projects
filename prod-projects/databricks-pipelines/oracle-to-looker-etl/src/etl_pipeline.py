# Databricks notebook source
# MAGIC %pip install oracledb==2.1.2 gspread==6.1.0 oauth2client==4.1.3 sqlalchemy==2.0.25
# MAGIC dbutils.library.restartPython()
import oracledb
import gspread
import json
import pandas as pd
import pytz 
import time
import logging
import os
from datetime import datetime
from sqlalchemy import create_engine, text

# ==========================================
# 0. SETUP LOGGING & RUNTIME WIDGETS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Silence the noisy background logs from Py4J and Google APIs
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING) 

# ONLY ask the user for things that change per run
dbutils.widgets.text("category", "Beauty", "1. Product Category")
CATEGORY = dbutils.widgets.get("category")

# ==========================================
# 1. LOAD CONFIGURATION
# ==========================================
def load_config():
    """Reads the static configuration using relative paths for DABs."""
    config_path = os.path.join(os.getcwd(), "config.json")
    logger.info(f"⚙️ Loading configuration from {config_path}")
    
    with open(config_path, 'r') as f:
        return json.load(f)

# ==========================================
# 2. HELPER FUNCTIONS 
# ==========================================
def get_auth_clients():
    logger.info("🔑 Fetching credentials from Databricks Secrets...")
    
    # A. Google Auth
    google_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="google_auth")
    gc = gspread.service_account_from_dict(json.loads(google_secret))

    # B. Oracle Auth
    oracle_config = json.loads(dbutils.secrets.get(scope="luu_qm_secrets", key="oracle_auth"))
    connection_string = (
        f"oracle+oracledb://{oracle_config['user']}:{oracle_config['password']}"
        f"@{oracle_config['host']}:{oracle_config['port']}/?service_name={oracle_config['service']}"
    )
    return create_engine(connection_string), gc

def extract_from_oracle(engine, sql_filename, category):
    """Reads the SQL file via relative path and executes it securely."""
    sql_path = os.path.join(os.getcwd(), sql_filename)
    logger.info(f"📂 Reading SQL query from {sql_filename} & querying Oracle for: {category}...")
    
    with open(sql_path, 'r') as file:
        query = file.read()
        
    with engine.connect() as connection:
        # Pass the category parameter securely to prevent SQL Injection
        df = pd.read_sql(text(query), connection, params={"category": category})
    
    logger.info(f"✅ Raw Data Extracted: {len(df)} rows.")
    return df

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    logger.info("🚀 STARTING JOB: Oracle -> Sheets -> Calc")
    berlin_tz = pytz.timezone('Europe/Berlin')
    current_time = datetime.now(berlin_tz).strftime("%d/%m/%Y %H:%M:%S")
    
    try:
        # Load settings from the config.json file
        cfg = load_config()
        
        # --- EXTRACT ---
        engine, gc = get_auth_clients()
        df_raw = extract_from_oracle(engine, cfg["file_paths"]["sql_query"], CATEGORY)
        
        if len(df_raw) == 0:
            raise ValueError("Oracle returned 0 rows. Aborting job.")

        # --- TRANSFORM ---
        logger.info("⚙️ Transforming data...")
        lhm_col = next((col for col in df_raw.columns if col.lower() == "mainlhm"), None)
        df_clean = df_raw[df_raw[lhm_col].astype(str).str.match(r'^\d')] if lhm_col else df_raw
        df_clean = df_clean.iloc[:, :22].fillna('')
        final_count = len(df_clean)

        # --- LOAD ---
        logger.info("📋 Uploading to Sheets...")
        sh = gc.open_by_key(cfg["google_sheet"]["sheet_id"])
        
        worksheet_upload = sh.worksheet(cfg["google_sheet"]["upload_tab"])
        worksheet_upload.batch_clear(["A:V"])
        worksheet_upload.update(
            range_name="A1", 
            values=[df_clean.columns.values.tolist()] + df_clean.values.tolist()
        )
        
        try:
            sh.worksheet(cfg["google_sheet"]["time_tab"]).update_acell("C2", current_time)
        except Exception as e:
            logger.warning(f"Could not update time tab: {e}")

        logger.info("⏳ Waiting 5 seconds for Google Sheets formulas to sync...")
        time.sleep(5) 

        # --- CALCULATE ---
        logger.info(f"📥 Reading '{cfg['google_sheet']['calc_tab']}' for calculations...")
        worksheet_calc = sh.worksheet(cfg["google_sheet"]["calc_tab"])
        raw_data = worksheet_calc.get_all_values()
        
        if len(raw_data) <= 1:
            raise ValueError("Calculation Sheet was empty after sync!")

        df_calc = pd.DataFrame(raw_data[1:], columns=raw_data[0])
        
        # Calculate Total Vol (Index 11 / Col L)
        vol_series = pd.to_numeric(
            df_calc.iloc[:, 11].astype(str).str.replace(',', '').str.strip(), errors='coerce'
        ).fillna(0)
        total_vol = vol_series.sum()

        # Apply Ready Vol Filters (Defensive Programming)
        logger.info("⚙️ Applying boolean masks for Ready Volume calculation...")
        mask_outlet = df_calc.iloc[:, 5].astype(str).str.strip().str.upper() == "OUTLET"
        mask_not_olap = ~df_calc.iloc[:, 1].astype(str).str.strip().str.upper().str.startswith("OLAP")
        mask_not_fin = ~df_calc.iloc[:, 1].astype(str).str.strip().str.upper().str.startswith("FIN")
        mask_starts_50 = df_calc.iloc[:, 14].astype(str).str.strip().str.startswith("50")
        
        # Combine all masks and calculate final Ready Vol
        final_mask = mask_outlet & mask_not_olap & mask_not_fin & mask_starts_50
        ready_vol = vol_series[final_mask].sum()

        logger.info(f"✅ Total Volume: {total_vol} | Ready Volume: {ready_vol}")

        # --- SUCCESS HANDOFF ---
        logger.info("💾 Saving Task Values for downstream jobs...")
        dbutils.jobs.taskValues.set(key="status", value="SUCCESS")
        dbutils.jobs.taskValues.set(key="rows", value=final_count)
        dbutils.jobs.taskValues.set(key="total_vol", value=float(total_vol))
        dbutils.jobs.taskValues.set(key="ready_vol", value=float(ready_vol))
        dbutils.jobs.taskValues.set(key="run_time", value=current_time)
        dbutils.jobs.taskValues.set(key="error_msg", value="")

    except Exception as e:
        logger.error(f"❌ CRITICAL ERROR CAUGHT: {str(e)}")
        # --- FAILURE HANDOFF ---
        dbutils.jobs.taskValues.set(key="status", value="FAILURE")
        dbutils.jobs.taskValues.set(key="error_msg", value=str(e))
        dbutils.jobs.taskValues.set(key="rows", value=0)
        dbutils.jobs.taskValues.set(key="total_vol", value=0.0)
        dbutils.jobs.taskValues.set(key="ready_vol", value=0.0)
        dbutils.jobs.taskValues.set(key="run_time", value=current_time)
        
        # Ensure the Databricks UI flags this task as FAILED
        raise e 

if __name__ == "__main__":
    main()
