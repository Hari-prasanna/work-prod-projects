import sys
import os
import importlib
from datetime import datetime

# 1. SETUP PATHS
# Directory for this specific pipeline (where config.json and .sql live)
PROJECT_DIR = "/Workspace/Users/hari.prasanna.ravichandran@zalando.de/team-repo/databricks/receive-uph-kpis/src"

# Directory for the shared team utilities
UTILS_DIR = "/Workspace/Users/hari.prasanna.ravichandran@zalando.de/team-repo/databricks/dbricks-utils"

# Add the shared utils folder to Python's search path
if UTILS_DIR not in sys.path:
    sys.path.append(UTILS_DIR)

# 2. IMPORT UTILS
import common_utils as u
importlib.reload(u)  # Force refresh to pick up new updates across the team

# Initialize logger from utils
logger = u.setup_logging(__name__)

def main():
    logger.info("--- Starting Daily Receive UPH KPIs Job ---")
    
    try:
        # 3. LOAD CONFIGURATION
        # Reads config.json sudo from the PROJECT_DIR
        config = u.load_config(base_dir=PROJECT_DIR)

        # 4. INITIALIZE CLIENTS
        # Using the clean, single-line connection setup
        gc, engine, webhook_url = u.get_connections(dbutils, logger=logger)
        
        # Open the specific worksheet
        sheet = gc.open_by_key(config["google_sheet"]["sheet_id"]) \
                  .worksheet(config["google_sheet"]["upload_tab"])

        # 5. CALCULATE TIME WINDOW
        # Uses your reference logic: Berlin -> UTC conversion
        start_dt, end_dt, target_date = u.get_utc_window(config, days_back=0)

        # 6. EXECUTE SQL
        # Points specifically to the PROJECT_DIR for the SQL file
        sql_path = os.path.join(PROJECT_DIR, config['file_paths']['sql_query'])
        df = u.run_sql_file(engine, sql_path, params={
            "start_datetime": start_dt,
            "end_datetime": end_dt,
            "ref_lhm_filter": None,   # Standard run captures all
        })

        # 7. IDEMPOTENT UPDATE TO GOOGLE SHEETS
        # Handles duplication prevention and empty-sheet headers
        u.update_google_sheet_idempotent(
            sheet=sheet, 
            df=df, 
            match_value=target_date, 
            date_col_index=1, 
            logger=logger
        )

        logger.info("--- Job Completed Successfully ---")

    except Exception as error:
        logger.critical(f"Job failed: {error}")
        # Re-raise to ensure Databricks Job UI marks this as a failure
        raise error

if __name__ == "__main__":
    main()
