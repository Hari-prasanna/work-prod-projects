# Overstock Inventory Reconciliation Project

![SQL](https://img.shields.io/badge/Language-Oracle_SQL-orange) ![Data Engineering](https://img.shields.io/badge/Focus-Data_Engineering_%26_ETL-blue) ![Impact](https://img.shields.io/badge/Impact-100%25_Accuracy_Restored-green)

## 📋 Executive Summary
This project addresses a critical "data drift" issue in the Overstock department's inventory tracking system. The solution involves a complex SQL algorithm designed to reconcile transaction logs by stitching together disjointed "booking out" and "booking in" events. The resulting query restored **100% accuracy** to the inventory reports and was subsequently adopted by the TGW team as the standard for stock level validation.

---

## 🧐 The Business Challenge
The Overstock department relied on a legacy report to track inventory movements that suffered from significant data discrepancies:
* **Data Drift:** The legacy reporting system was failing to record specific transaction types, leading to a growing gap between physical stock and digital records.
* **Black Box Logic:** The root cause was initially unknown, making the stock levels untrustworthy for operational planning.
* **Missing "Dummy" Items:** Investigation revealed that the system completely ignored "dummy item" barcodes (temporary placeholders), causing inventory to vanish from the logs.

## 🛠️ The Data Engineering Solution
To solve this, I reverse-engineered the transaction flow and built a robust SQL solution from scratch. The core logic tracks the **complete lifecycle** of an item, identifying when it leaves a location (`MENGE < 0`) and matching it to its arrival at the destination (`MENGE = 1`).

### Key Technical Implementations
1.  **Complex Logic Decomposition (CTEs):** Instead of a monolithic query, I used Common Table Expressions to separate "Normal Goods" logic from "Dummy Goods" logic, improving readability and maintainability.
2.  **Semi-Structured Data Parsing (JSON):** The source system stored critical metadata (SKU, Quality, Categories) inside a JSON CLOB column (`CUST_DATA`). I utilized `JSON_VALUE` to extract and normalize this data into tabular columns.
3.  **Advanced Joining & Filtering:** The query performs a self-join on the `LOCAL_TRANSACTION_ID` to stitch the start and end of a transaction, ensuring that we only report on completed inventory movements.
4.  **Data Cleaning & Normalization:** Implemented `DECODE` and `CASE` statements to translate internal system codes (e.g., `QualityID: 1`) into human-readable business terms (e.g., `Grade A`).

---

## 🚀 Impact & Results
* **100% Accuracy Restored:** The new logic captured the previously missing "dummy" transactions, eliminating the data drift.
* **Cross-Team Adoption:** The solution was verified and subsequently implemented by the TGW (Technical) team as the primary source of truth for Overstock booking.
* **Historical Correction:** The query allowed the business to retroactively correct inventory data from previous periods.

---

## 💻 The SQL Logic

### Logic Breakdown
1.  **`normal_goods_t1` & `t2`**: Captures standard items leaving (t1) and arriving (t2).
2.  **`dummy_goods_t1` & `t2`**: Captures "dummy" items. **Crucial Logic:** For dummy items, the actual SKU is often only available in the completion record (t2), requiring a dynamic extraction strategy.
3.  **Union & Clean**: Merges both streams and applies business-readable formatting.
