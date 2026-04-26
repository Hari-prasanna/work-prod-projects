import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
import pytz
import gspread
from sqlalchemy import create_engine, text

# ==========================================
# 1. SETUP LOGGING & CONFIGURATION
# ==========================================
# This helps us track what the script is doing and where it fails
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING) 

# Base path where your config and SQL files live
BASE_DIR = "/Workspace/Users/hari.prasanna.ravichandran@zalando.de/team-repo/databricks/shift-report-pilot/src"
CONFIG_PATH = f"{BASE_DIR}/config.json"

def load_config():
    """Reads the JSON configuration file."""
    logger.info("Loading configuration file...")
    try:
        with open(CONFIG_PATH, 'r') as file:
            config = json.load(file)
            return config
    except Exception as error:
        logger.error(f"Could not load config file at {CONFIG_PATH}. Error: {error}")
        raise

# ==========================================
# 2. HELPER FUNCTIONS
# ==========================================
def setup_connections(config):
    """Sets up connections to Google Sheets and the Oracle Database."""
    logger.info("Setting up database and sheet connections...")
    
    try:
        # 1. Connect to Google Sheets
        google_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="google_auth")
        creds_dict = json.loads(google_secret)
        gc = gspread.service_account_from_dict(creds_dict)
        
        # Use variables from our config file!
        sheet_id = config["google_sheet"]["sheet_id"]
        tab_name = config["google_sheet"]["upload_tab"]
        sheet = gc.open_by_key(sheet_id).worksheet(tab_name)

        # 2. Connect to Oracle Database
        oracle_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="oracle_auth")
        oracle_config = json.loads(oracle_secret)
        
        connection_string = (
            f"oracle+oracledb://{oracle_config['user']}:{oracle_config['password']}"
            f"@{oracle_config['host']}:{oracle_config['port']}/?service_name={oracle_config['service']}"
        )
        engine = create_engine(connection_string)
        
        return sheet, engine
        
    except Exception as error:
        logger.error(f"Connection setup failed: {error}")
        raise

def get_time_parameters(config):
    """Calculates historical shift times and translates them to UTC for Oracle."""
    tz_string = config["shift_settings"]["timezone"] # "Europe/Berlin"
    local_tz = pytz.timezone(tz_string)
    
    # Get the current time in Berlin
    now = datetime.now(local_tz)

    # Calculate the target day 
    target_day = now - timedelta(days=0)
    
    # The date string we use to check Google Sheets (DD.MM.YYYY)
    target_date_str = target_day.strftime('%d.%m.%Y')
    
    # 1. Build the target shift strings for that specific historical day
    start_time_str = config["shift_settings"]["start_time"]
    end_time_str = config["shift_settings"]["end_time"]
    
    # Using YYYY-MM-DD temporarily just to safely construct the datetime object
    target_date_ymd = target_day.strftime('%Y-%m-%d')
    berlin_start_str = f"{target_date_ymd} {start_time_str}"
    berlin_end_str = f"{target_date_ymd} {end_time_str}"
    
    # 2. Tell Python these specific times belong to the Berlin timezone
    berlin_start_dt = local_tz.localize(datetime.strptime(berlin_start_str, "%Y-%m-%d %H:%M:%S"))
    berlin_end_dt = local_tz.localize(datetime.strptime(berlin_end_str, "%Y-%m-%d %H:%M:%S"))
    
    # 3. Translate Berlin time into UTC time (Because Oracle servers run in UTC!)
    utc_start_dt = berlin_start_dt.astimezone(pytz.utc)
    utc_end_dt = berlin_end_dt.astimezone(pytz.utc)
    
    # 4. Format them back into the DD.MM.YYYY HH:MM:SS string format Oracle expects
    start_datetime = utc_start_dt.strftime('%d.%m.%Y %H:%M:%S')
    end_datetime = utc_end_dt.strftime('%d.%m.%Y %H:%M:%S')
    
    logger.info(f"Targeting Berlin Shift: {berlin_start_dt.strftime('%d.%m.%Y %H:%M:%S')} to {berlin_end_dt.strftime('%d.%m.%Y %H:%M:%S')}")
    logger.info(f"Asking Oracle for (UTC): {start_datetime} to {end_datetime}")
    
    return start_datetime, end_datetime, target_date_str

def fetch_data(engine, config, start_dt, end_dt):
    """Reads the SQL file and runs it against Oracle."""
    sql_file_name = config["file_paths"]["sql_query"]
    sql_file_path = f"{BASE_DIR}/{sql_file_name}"
    
    try:
        # Read the SQL query text from the file
        with open(sql_file_path, 'r') as file:
            query_text = file.read()
            
        logger.info("Running SQL query...")
        
        # Connect to the database and run the query
        with engine.connect() as connection:
            df = pd.read_sql(
                text(query_text), 
                connection,
                params={
                    "start_datetime": start_dt,
                    "end_datetime": end_dt,
                    "ref_lhm_filter": None
                }
            )
            
        logger.info(f"Successfully retrieved {len(df)} rows from Oracle.")
        return df
        
    except Exception as error:
        logger.error(f"Failed to fetch data: {error}")
        raise

def update_google_sheet_safely(sheet, df_new, target_date):
    """
    This function cleans up today's old data and replaces it with fresh data.
    """

    # --- STEP 0: Check if there is even any data to upload ---
    if df_new.empty:
        logger.info("No data found to upload. Skipping...")
        return
        
    try:
        # --- STEP 1: Clean up the data look ---
        # Make all column titles BIG LETTERS (e.g., 'date' becomes 'DATE')
        df_new.columns = [str(col).upper() for col in df_new.columns]
        
        # Replace empty/missing values with 'None' so they show up as blank cells
        df_new = df_new.where(pd.notnull(df_new), None)
        
        
        # --- STEP 2: Find and remove old data for today ---
        logger.info(f"Looking for old records dated {target_date}...")
        
        # Get everything currently in the first column (Column A)
        first_column = sheet.col_values(1)
        
        # Make a list of which row numbers have today's date
        # We add 1 because Python starts counting at 0, but Sheets starts at 1
        rows_to_delete = []
        for index, value in enumerate(first_column):
            if value == target_date:
                rows_to_delete.append(index + 1)

        # If we found old data, delete that whole block at once
        if rows_to_delete:
            first_row = rows_to_delete[0]
            last_row = rows_to_delete[-1]
            logger.info(f"Removing old data between rows {first_row} and {last_row}.")
            sheet.delete_rows(first_row, last_row)
        
        
        # --- STEP 3: Add Headers (If the sheet is totally blank) ---
        if len(first_column) == 0:
            logger.info("Sheet is empty! Adding the top header row first.")
            sheet.append_row(df_new.columns.tolist())
            
            
        # --- STEP 4: Upload the new data ---
        logger.info("Adding the new data to the bottom of the sheet...")
        
        # Convert our data table into a simple list format the sheet understands
        rows_to_upload = df_new.values.tolist()
        
        # 'USER_ENTERED' tells Google: "Treat these numbers like a human typed them"
        # This ensures you can still use math formulas like =SUM() on the data.
        sheet.append_rows(rows_to_upload, value_input_option='USER_ENTERED')
        
        logger.info(f"Done! Successfully added {len(rows_to_upload)} new rows.")
        
    except Exception as error:
        # If anything goes wrong, tell us exactly what the error was
        logger.error(f"Something went wrong: {error}")
        raise

# ==========================================
# 3. THE MAIN SCRIPT
# ==========================================
def main():
    logger.info("--- Starting Nightly Shift Report Job ---")
    try:
        # Step 1: Load the configuration
        config = load_config()
        
        # Step 2: Set up connections
        sheet, engine = setup_connections(config)
        
        # Step 3: Figure out what times to query
        start_dt, end_dt, target_date = get_time_parameters(config)
        
        # Step 4: Get the data from Oracle
        df_report = fetch_data(engine, config, start_dt, end_dt)
        
        # Step 5: Safely push the data to Google Sheets
        update_google_sheet_safely(sheet, df_report, target_date)
        
        logger.info("--- Job Completed Successfully ---")
        
    except Exception as error:
        logger.critical("Job failed! Check the logs above for details.")
        raise error

# This tells Python to run the main() function when the script starts
if __name__ == "__main__":
    main()