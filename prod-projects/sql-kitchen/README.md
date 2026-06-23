# SQL Kitchen

A reference library of Oracle SQL queries written for the LUU logistics warehouse. Organised by purpose — queries here are standalone and may overlap with logic used inside deployed pipelines.

## Structure

| Folder | Contents |
| :--- | :--- |
| `booking/` | Transaction-level booking queries — receive, manual receive, and outlet variants |
| `kpis/` | Aggregated KPI queries that pivot quality, source channel, and disposition by shift |
| `tgw-infosystem-live/` | Queries wired directly into the TGW infosystem UI; original file names preserved |

## Files

### booking/

| File | Bind params | Output |
| :--- | :--- | :--- |
| `receive_booking.sql` | `:start_datetime`, `:end_datetime`, `:ref_lhm_filter` | Rows by date, shift, LHM, category, sort, quality, clarification flag |
| `receive_manual_booking.sql` | same | Same shape; uses `ROW_NUMBER()` for stricter dummy-goods matching and targets a different LHM scope |
| `outlet_booking.sql` | same | Same shape; filtered to outlet LHM numbers (`4%` / `5%`) |

### kpis/

| File | Bind params | Output |
| :--- | :--- | :--- |
| `receive_uph_kpis.sql` | `:start_datetime`, `:end_datetime`, `:ref_lhm_filter` | Pivoted KPI table (German column names) with quality grades A–D, source channels, and disposition breakdown by date/shift/AP/user |

### tgw-infosystem-live/

| File | TGW params | Output |
| :--- | :--- | :--- |
| `NEU Overstock- Buchung detail + Sortierkriterien.sql` | `:reflhmfilter`, `:rpv`, `:rpb` | Detail rows with sorting criteria — not aggregated |
| `Overstock_Übersicht_bearbeiteter_Artikel.sql` | `:rpv`, `:rpb` | Same KPI pivot as `receive_uph_kpis.sql` adapted for TGW parameter naming |

## Notes

- `receive_booking.sql`, `outlet_booking.sql`, and the TGW detail file share the same core CTE pattern (normal\_goods / dummy\_goods join + quality mapping). Differences are the location filter, parameter names, and whether rows are aggregated or kept as detail.
- `receive_uph_kpis.sql` and `Overstock_Übersicht_bearbeiteter_Artikel.sql` are near-identical; the TGW version uses `:rpv`/`:rpb` instead of `:start_datetime`/`:end_datetime` and has no `:ref_lhm_filter`.
- All queries target Oracle. Dates use `TO_DATE(:param, 'DD.MM.YYYY HH24:MI:SS')` format.
