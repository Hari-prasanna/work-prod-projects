# Overstock Inventory Reconciliation & Sorting Engine

![SQL](https://img.shields.io/badge/Language-Oracle_SQL-orange)
![Data Engineering](https://img.shields.io/badge/Focus-Data_Engineering_%26_ETL-blue)
![Technique](https://img.shields.io/badge/Technique-Window_Functions-purple)
![Impact](https://img.shields.io/badge/Impact-100%25_Accuracy_Restored-green)

## 📋 Summary

Two SQL solutions fixing interconnected data integrity issues in the Overstock department:

| Problem | Solution | Result |
| :--- | :--- | :--- |
| Legacy system silently dropped "dummy item" transactions → stock drift | CTE-based algorithm stitching "book-out" (`MENGE < 0`) to "book-in" (`MENGE = 1`) via `LOCAL_TRANSACTION_ID` | **100% accuracy restored** · Adopted by TGW as the standard |
| Manual sorting area generated duplicate/out-of-sequence scans → inflated metrics | `ROW_NUMBER()` window function enforcing strict 1:1 sequence matching (`t1.rn = t2.rn`) | **100% duplicates eliminated** |

---

## 🛠️ Part 1: Inventory Reconciliation

**Core idea:** Track the complete lifecycle of an item by joining its departure and arrival records.

- **CTE Decomposition:** Separate pipelines for Normal Goods (`TPARTNR = 520`) and Dummy Goods (`TPARTNR = 614/207`), merged via `UNION ALL`
- **JSON Extraction:** `JSON_VALUE` on `CUST_DATA` CLOB to normalize SKU, Quality, Category, Source Channel, and Distribution Channel into tabular columns
- **Dual-Source Quality Logic:** `COALESCE` across t2/t1 records with a `SORTABLE_ART` override for edge cases
- **Parameterized Filtering:** Supports single values, comma-separated lists, and LIKE wildcards — no dynamic SQL
- **Business Normalization:** `DECODE` / `CASE` translates system codes to human-readable terms (e.g., `QualityID: 1` → `Grade A`)

```text
normal_goods_t1 ──┐                                  
                  ├─ JOIN on LOCAL_TRANSACTION_ID ──┐
normal_goods_t2 ──┘                                ├── UNION ALL ── FINAL SELECT
dummy_goods_t1  ──┐                                │
                  ├─ JOIN on LOCAL_TRANSACTION_ID ──┘
dummy_goods_t2  ──┘
```

## 🛠️ Part 2: Manual Sorting Deduplication

**Core idea:** Assign a strict sequence index so each book-out matches exactly one book-in.

- **`ROW_NUMBER() OVER (PARTITION BY id ORDER BY sequence)`** — unique index per transaction step, preventing cross-join duplicates
- **3-Step EAN Fallback:** Source EAN → `LASTEANGOTFROMMAUS` from JSON → Destination EAN
- **`/*+ MATERIALIZE */` hints** — forces Oracle to cache CTE results for large date ranges

---

## 🚀 Impact

- **100% inventory accuracy restored** — missing "dummy" transactions fully captured
- **100% duplicate elimination** — manual sorting metrics now trustworthy
- **99.9% barcode traceability** — even damaged barcodes resolved via fallback logic
- **Cross-team adoption** — TGW implemented the reconciliation query as their primary source of truth
- **Historical correction** — enabled retroactive inventory fixes for previous periods