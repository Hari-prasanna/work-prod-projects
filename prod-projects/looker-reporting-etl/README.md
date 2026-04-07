## 📂 Featured Projects

### 1. [DG Monitor: Automated Hazardous Compliance Pipeline](./LUU-DG-Monitor)
**Focus:** Data Engineering, Python ETL, Safety Compliance

A fully automated cloud-based pipeline designed to manage hazardous inventory at the Ludwigsfelde warehouse. This project replaced a reactive manual process with a proactive forecasting model.

* **The Challenge:** Managing Dangerous Goods (DG) requires strict adherence to volume thresholds (e.g., <20 Liters). Manual reporting caused latency and compliance risks.
* **The Solution:** Built a Python/Databricks pipeline that pulls from Oracle, cleans data, and syncs to a live dashboard.
* **Key Impact:**
    * **Unified View:** Consolidated disparate data sources into a single source of truth.
    * **Risk Mitigation:** Implemented "Days Difference" (DD) forecasting to flag items before they breach legal limits.
    * **Granular Control:** Visualizes volumes by Hazard Class (e.g., Class 2.1 vs 3) and UN Numbers.

➡️ **[Read Full Documentation & Code](./LUU-DG-Monitor)**

---

### 2. [LUU Quality Feed: Centralized QA Intelligence](./LUU-Quality-Feed)
**Focus:** Analytics Engineering, BI, Scoring Logic

A centralized analytics solution that consolidates quality processes—Inbound, Stock Accuracy, and Outbound—into a single "Overall Score" for management steering.

* **The Challenge:** Quality data was fragmented across multiple spreadsheets, making it impossible to gauge overall warehouse health at a glance.
* **The Solution:** Designed a linear ETL pipeline and a weighted scoring algorithm that aggregates 6+ data sources into a unified dashboard.
* **Key Impact:**
    * **Dynamic Scoring:** Calculates a weighted performance score based on DPMI thresholds (e.g., <3500 DPMI).
    * **Data Governance:** Authored a comprehensive user guide standardizing metrics like "Critical vs. Major" errors.
    * **Efficiency:** Automated data collection saved hours of manual reporting per week.

➡️ **[Read Full Documentation & Code](./LUU-Quality-Feed)**

---

## 📈 Philosophy
I believe that **data without context is noise**. My approach combines technical skills (Python/SQL) with deep operational empathy to build tools that warehouse teams actually use.

* **Automate Everything:** If it takes more than 5 minutes a day, script it.
* **Built for Users:** Dashboards should pass the "5-second rule"—can a manager understand the status in 5 seconds?
* **Documentation:** A tool is only as good as the manual that explains it.

---
