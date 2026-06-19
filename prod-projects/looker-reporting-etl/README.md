# Looker reporting ETL

BI deliverables that combine an Oracle/Databricks data pull with a published
dashboard. The code here covers the ETL and scoring logic; the dashboards
themselves live in Looker Studio.

## Projects

### [dg-compliance-pipeline](./dg-compliance-pipeline)

Dangerous-goods (DG) compliance monitor. A Python/Databricks job pulls stock and
handling-unit data from Oracle, classifies it by UN number and hazard class,
computes net volume per location, and derives a "days difference" forecast that
flags items before they approach storage thresholds. Feeds a live Looker Studio
dashboard. The upstream extract is the
[oracle-to-looker-etl](../databricks-pipelines/oracle-to-looker-etl) pipeline.

### [qa-intelligence-engine](./qa-intelligence-engine)

LUU Quality Feed. A linear ETL that aggregates 6+ quality audit sources (inbound,
stock accuracy, outbound) into a single "quality databank", then applies a
weighted scoring model: each sub-process passes if it meets its DPMI threshold,
and the overall score is the share of targets met. Used as the standing view in
daily steering meetings, with a written manual defining each metric.
