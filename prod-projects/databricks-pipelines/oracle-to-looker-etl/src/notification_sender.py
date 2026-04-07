# Databricks notebook source
import requests
import json
import logging
import os

# ==========================================
# 0. SETUP LOGGING & WIDGETS
# ==========================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
logging.getLogger("py4j").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# The exact name of Notebook 1 as defined in the Databricks Workflow Job
dbutils.widgets.text("previous_task_name", "ETL_Task", "Previous Task Name")
PREVIOUS_TASK_KEY = dbutils.widgets.get("previous_task_name")

# ==========================================
# 1. HELPER FUNCTIONS
# ==========================================
import os

def load_config():
    """Reads the static configuration using relative paths for DABs."""
    config_path = os.path.join(os.getcwd(), "config.json")
    
    logger.info(f"⚙️ Loading configuration from {config_path}")
    with open(config_path, 'r') as f:
        return json.load(f)

def get_webhook_url():
    """Securely fetches the Google Chat Webhook from Databricks Secrets."""
    raw_url = dbutils.secrets.get(scope="luu_qm_secrets", key="chat_webhook_url")
    clean_url = raw_url.strip().strip('"').strip("'")
    
    return clean_url

def send_card(webhook_url, config_links, status, rows, total_vol, ready_vol, time_str, error_msg=None):
    """Builds and sends the Google Chat V2 Card payload."""
    is_success = status == "SUCCESS"
    header_title = "LUU_DG_Stock_Monitor"
    
    # 🎨 HEADER CONFIG
    if is_success:
        header_subtitle = "✅ Aktualisierung erfolgreich"
        header_icon = "https://fonts.gstatic.com/s/i/short_term/release/googlesymbols/check_circle/default/24px.svg"
    else:
        header_subtitle = "❌ Aktualisierung fehlgeschlagen"
        header_icon = "https://fonts.gstatic.com/s/i/short_term/release/googlesymbols/warning/default/24px.svg"

    sections = []
    
    if is_success:
        # --- SUCCESS LAYOUT ---
        fmt_rows = "{:,}".format(int(rows)).replace(",", ".")
        fmt_tot = "{:,.0f}".format(float(total_vol)).replace(",", ".") + " ml"
        fmt_rdy = "{:,.0f}".format(float(ready_vol)).replace(",", ".") + " ml"
        clean_time = str(time_str).split(" ")[1] if " " in str(time_str) else str(time_str)

        sections.append({
            "widgets": [{"columns": {"columnItems": [
                {"horizontalAlignment": "START", "widgets": [{"decoratedText": {"topLabel": "Uhrzeit", "text": clean_time, "startIcon": {"knownIcon": "CLOCK"}}}]},
                {"horizontalAlignment": "START", "widgets": [{"decoratedText": {"topLabel": "Zeilen verarbeitet", "text": fmt_rows, "startIcon": {"knownIcon": "DESCRIPTION"}}}]}
            ]}}]
        })
        sections.append({
            "widgets": [{"columns": {"columnItems": [
                {"horizontalAlignment": "START", "widgets": [{"decoratedText": {"topLabel": "LUU Gesamtvolumen", "text": fmt_tot, "startIcon": {"knownIcon": "STORE"}}}]},
                {"horizontalAlignment": "START", "widgets": [{"decoratedText": {"topLabel": "Outlet bereit", "text": fmt_rdy, "startIcon": {"knownIcon": "SHOPPING_CART"}}}]}
            ]}}]
        })
        sections.append({
            "widgets": [{"buttonList": {"buttons": [
                {"text": "DASHBOARD ÖFFNEN 📊", "color": {"red": 0, "green": 0, "blue": 1, "alpha": 1}, "onClick": {"openLink": {"url": config_links["looker_dashboard"]}}},
                {"text": "ÜBERSICHT ÖFFNEN 📑", "onClick": {"openLink": {"url": config_links["sheet_overview"]}}}
            ]}}]
        })
    else:
        # --- FAILURE LAYOUT ---
        sections.append({
            "widgets": [
                {"textParagraph": {"text": f"<b>⚠️ Automatisierung fehlgeschlagen</b><br>Grund: {str(error_msg)[:250]}..."}},
                {"textParagraph": {"text": "<b>Handlung erforderlich:</b><br>Bitte führen Sie den manuellen Standardprozess durch."}},
                {"buttonList": {"buttons": [{"text": "MANUELLE TABELLE ÖFFNEN 📝", "onClick": {"openLink": {"url": config_links["sheet_manual"]}}}]}}
            ]
        })

    # Construct Payload
    payload = {
        "cardsV2": [{
            "cardId": "stock-card", 
            "card": {
                "header": {"title": header_title, "subtitle": header_subtitle, "imageUrl": header_icon, "imageType": "CIRCLE"}, 
                "sections": sections
            }
        }]
    }
    
    # Send Request
    logger.info("📤 Sending Card to Google Chat...")
    response = requests.post(webhook_url, json=payload)
    
    # This ensures Python throws an error if Google Chat rejects our JSON!
    response.raise_for_status() 
    logger.info("✅ Card Notification Sent Successfully.")

# ==========================================
# 3. MAIN EXECUTION
# ==========================================
def main():
    try:
        cfg = load_config()
        webhook_url = get_webhook_url()
        
        logger.info(f"🔄 Fetching results from task: '{PREVIOUS_TASK_KEY}'...")
        
        # Retrieve values set by Notebook 1
        status = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="status", default="FAILURE")
        error_msg = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="error_msg", default="Unbekannter Systemfehler / Task failed to report")
        
        rows = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="rows", default=0)
        total_vol = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="total_vol", default=0)
        ready_vol = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="ready_vol", default=0)
        time_str = dbutils.jobs.taskValues.get(taskKey=PREVIOUS_TASK_KEY, key="run_time", default="--:--")

        logger.info(f"📥 Status received: {status}")
        
        send_card(webhook_url, cfg["dashboard_links"], status, rows, total_vol, ready_vol, time_str, error_msg)

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"❌ HTTP Error from Google Chat: {http_err.response.text}")
        raise http_err
    except Exception as e:
        logger.error(f"❌ Notification Script Error: {e}")
        raise e

if __name__ == "__main__":
    main()
