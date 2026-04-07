import os
import time
import json
import pandas as pd
from datetime import datetime
import pytz
import gspread
import oracledb
from sqlalchemy import create_engine, text

# ==========================================
# 1. SECURE AUTHENTICATION (Vault)
# ==========================================
# A. Google Auth
google_secret = dbutils.secrets.get(scope="luu_qm_secrets", key="google_auth")
creds_dict = json.loads(google_secret)
gc = gspread.service_account_from_dict(creds_dict)

SHEET_ID = "SHEET_ID"
TAB_NAME = "TAB_NAME"
sheet = gc.open_by_key(SHEET_ID).worksheet(TAB_NAME)

# B. Oracle Auth
oracle_secret_string = dbutils.secrets.get(scope="luu_qm_secrets", key="oracle_auth")
oracle_config = json.loads(oracle_secret_string) 

# Create the SQLAlchemy Engine
connection_string = (
    f"oracle+oracledb://{oracle_config['user']}:{oracle_config['password']}"
    f"@{oracle_config['host']}:{oracle_config['port']}/?service_name={oracle_config['service']}"
)
engine = create_engine(connection_string)


# ==========================================
# 2. DYNAMIC COLUMN CONFIGURATION
# ==========================================
# Relative path to your SQL files
SQL_FOLDER = "folderpath"

# Automatically find every .sql file
sql_files = [f for f in os.listdir(SQL_FOLDER) if f.endswith('.sql')]
sql_files.sort()

# Generate row headers (e.g., "Last Updated", "kpi_1", "kpi_2")
column_headers = ["Last Updated"] + [f.replace('.sql', '') for f in sql_files]


# ==========================================
# 3. THE REPEATING LOOP
# ==========================================
def run_dashboard_loop():
    print("Starting Warehouse TV Dashboard feed...")
    sheet.update([column_headers], 'A1')
    
    # Set your local timezone once outside the loop
    berlin_tz = pytz.timezone('Europe/Berlin') 
    
    while True:
        # Grab the current time and immediately convert it to Berlin time
        now = datetime.now(berlin_tz)
        
        if now.hour >= 23 and now.minute >= 30:
            print("Shift ended at 11:30 PM. Stopping the loop.")
            break 
            
        print(f"\n--- Update started at {now.strftime('%H:%M:%S')} ---")
        
        timestamp_string = now.strftime('%Y-%m-%d %H:%M:%S')
        row_data = [timestamp_string]
        
        # Connect to Oracle once per loop
        with engine.connect() as connection:
            
            for sql_file in sql_files:
                file_path = f"{SQL_FOLDER}/{sql_file}"
                
                try:
                    with open(file_path, 'r') as file:
                        query = file.read()
                    
                    # Run the query using pandas & sqlalchemy
                    df = pd.read_sql(text(query), connection)
                    
                    # Extract the single KPI number from the first row and first column
                    kpi_number = df.iloc[0, 0]
                    
                    # Convert to standard Python int/float so Google Sheets accepts it
                    if pd.isna(kpi_number):
                        kpi_number = 0
                    else:
                        kpi_number = int(kpi_number) if float(kpi_number).is_integer() else float(kpi_number)

                    row_data.append(kpi_number)
                    print(f"Success: Pulled {kpi_number} for {sql_file}")
                    
                except Exception as e:
                    row_data.append("ERROR")
                    print(f"Failed to process {sql_file}: {e}")
                
        # Push the row to Google Sheets
        sheet.update([row_data], 'A2')
        
        print("Update complete. Waiting 5 minutes...")
        time.sleep(300) 

# Start the engine!
run_dashboard_loop()