# inbound-booking-report — inventory reconciliation & sorting dedup

Two Oracle SQL solutions for data-integrity problems in the Overstock department.
Both are read-only analytical queries (no DDL/DML); they reconstruct correct
figures from raw transaction logs.

## Part 1 — inventory reconciliation

The legacy system silently dropped "dummy item" transactions, causing stock
drift. The query rebuilds each item's lifecycle by joining its **book-out**
(`MENGE < 0`) record to its **book-in** (`MENGE = 1`) record on
`LOCAL_TRANSACTION_ID`.

- **CTE decomposition.** Separate pipelines for normal goods (`TPARTNR = 520`)
  and dummy goods (`TPARTNR = 614/207`), merged with `UNION ALL`.
- **JSON extraction.** `JSON_VALUE` over the `CUST_DATA` CLOB normalises SKU,
  quality, category, source channel, and distribution channel into columns.
- **Dual-source quality logic.** `COALESCE` across the two records, with a
  `SORTABLE_ART` override for edge cases.
- **Parameterised filtering.** Single values, comma-separated lists, and LIKE
  wildcards — no dynamic SQL.
- **Code translation.** `DECODE`/`CASE` map system codes to readable terms
  (e.g. `QualityID 1` → `Grade A`).

```text
normal_goods_t1 ─┐
                 ├─ JOIN on LOCAL_TRANSACTION_ID ─┐
normal_goods_t2 ─┘                                ├─ UNION ALL ─ FINAL SELECT
dummy_goods_t1  ─┐                                │
                 ├─ JOIN on LOCAL_TRANSACTION_ID ─┘
dummy_goods_t2  ─┘
```

File: `normal_booking_logic.sql`

## Part 2 — manual-sorting deduplication

The manual sorting area produced duplicate / out-of-sequence scans that inflated
metrics. A `ROW_NUMBER()` window function assigns a strict sequence index so each
book-out matches exactly one book-in (`t1.rn = t2.rn`), preventing cross-join
duplication.

- **3-step EAN fallback.** Source EAN → `LASTEANGOTFROMMAUS` from JSON →
  destination EAN, so even damaged barcodes resolve.
- **`/*+ MATERIALIZE */` hints** cache CTE results for large date ranges.

File: `manual_sorting_logic.sql`

## Project layout

```
inbound-booking-report/
├── normal_booking_logic.sql     # Part 1: reconciliation
├── manual_sorting_logic.sql     # Part 2: sequence-matched dedup
└── luu-volumes/                 # supporting volume queries
    ├── inbound.sql
    ├── receive.sql
    └── akl.sql
```

## Running

These are parameterised Oracle queries. Run them in your SQL client (or via the
Databricks Oracle connection used by the pipelines), supplying the date-range and
filter bind variables at the top of each file. Start with a narrow date range —
the reconciliation joins are wide.
