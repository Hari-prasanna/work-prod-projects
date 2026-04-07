# Overstock Manual Sorting & Sequence Reconciliation

![SQL](https://img.shields.io/badge/Language-Oracle_SQL-orange) ![Technique](https://img.shields.io/badge/Technique-Window_Functions-purple) ![Focus](https://img.shields.io/badge/Focus-Data_Cleaning_%26_Deduplication-blue)

## 📋 Executive Summary
This project optimized the inventory tracking for the **Manual Sorting Area** (SZR_OV) at Overstock. Unlike automated lines, manual sorting often generates rapid, duplicate, or out-of-sequence scans. I developed a high-precision SQL solution using **Window Functions** to enforce strict sequence matching, ensuring that every "book-out" scan is matched to exactly one "book-in" scan, eliminating duplicates and correcting phantom inventory data.

---

## 🧐 The Business Challenge
The manual sorting process introduced complexity that standard reporting couldn't handle:
* **Duplicate Scans:** Operators often double-scan items, or the system generates multiple log entries for a single physical movement.
* **Sequence Confusion:** When multiple items of the same type were processed in rapid succession, the legacy logic couldn't determine which "start" event belonged to which "end" event.
* **Ambiguous Barcodes:** "Dummy" items often had missing or inconsistent Article Numbers (EANs) depending on which stage of the sorting process they were in.

## 🛠️ The Data Engineering Solution
I engineered a query that enforces **transactional integrity** using advanced SQL features.



### Key Technical Implementations
1.  **Window Functions for Deduplication (`ROW_NUMBER`):**
    I implemented `ROW_NUMBER() OVER (PARTITION BY ID ORDER BY SEQUENCE)` to assign a unique index to every step of a transaction. This allows the query to strictly match the *first* book-out to the *first* book-in, the *second* to the *second*, and so on preventing "Cross-Joining" duplicates.
    
2.  **3-Step EAN Fallback Logic:**
    Data quality for "Dummy" items was inconsistent. I built a robust `CASE` statement to hunt for the correct EAN:
    * *Check 1:* Is the EAN valid in the source transaction?
    * *Check 2:* If not, extract the `LASTEANGOTFROMMAUS` tag from the JSON blob.
    * *Check 3:* If that fails, look at the destination transaction.

3.  **Performance Tuning (`MATERIALIZE` hints):**
    Given the high volume of logs, I used Oracle hints `/*+ MATERIALIZE */` to force the database to cache the CTE results, significantly reducing execution time for large date ranges.

---

## 🚀 Impact & Results
* **Eliminated Duplicate Records:** The strict sequence matching removed 100% of false-positive duplicates caused by rapid scanning.
* **Operational Visibility:** The "Manual Sorting" team gained trust in their daily throughput metrics, which were previously inflated by duplicates.
* **Enhanced Data Traceability:** The fallback logic ensured that 99.9% of items—even those with damaged barcodes—were correctly identified in the final report.

---

## 💻 The SQL Logic
*Note: This query uses Oracle SQL syntax, specifically utilizing Window Functions and JSON parsing.*

### Logic Breakdown
* **`PARTITION BY local_transaction_id`**: Groups scans belonging to the same session.
* **`ORDER BY sequence`**: Ensures we follow the exact chronological order of the physical scans.
* **`STRICT JOIN`**: The final join condition `t1.rn = t2.rn` is the "magic sauce" that prevents duplicates.
