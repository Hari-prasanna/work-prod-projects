# qa-intelligence-engine (LUU Quality Feed)

Consolidates the site's quality audits — inbound, stock accuracy, and outbound —
into a single dashboard with one weighted "overall score", used as the standing
view in daily steering meetings.

## How it works

```mermaid
graph LR
    A[WE / Stock / Outlet audits] -->|Extract| B[Quality Databank]
    B -->|Normalise + map to KPIs| C[Scoring model]
    C -->|Load| D[Dashboard pages]
```

![Data pipeline architecture](images/pipeline.png)

1. **Extract.** Pulls from the distinct audit sources (WE audit, stock-audit
   containers, outlet audit, etc.).
2. **Transform.** Normalises each source into the "Quality Databank" and applies
   the scoring logic.
3. **Load.** Feeds the dashboard pages (refurbishment, inbound, stock accuracy…).

## Scoring model

- Three buckets: **inbound**, **stock accuracy**, **outbound**.
- A sub-process "passes" only if it meets its DPMI (defects per million items)
  threshold (e.g. < 3500 DPMI).
- The overall score is the share of targets met (meeting 2 of 3 → 66.7%).
- Drill-downs isolate root causes (e.g. sorter-audit vs pack-audit failures).

![Dashboard view](images/newsletter.png)

## Documentation

A written information manual defines each metric (e.g. critical vs major DPMI)
and the exact error-rate calculations, so the dashboard isn't a black box and new
team members can self-serve.

![Information manual](images/manual.png)
