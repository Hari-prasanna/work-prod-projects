# work-prod-projects

Automation work built for the **LUU (Ludwigsfelde) logistics
site at Zalando** — production Databricks pipelines, Oracle SQL reconciliation
logic, and Google Workspace automations.

Each project is self-contained and has its own README with setup, configuration,
and run/deploy instructions. This top-level file explains how the repository is
organised and the conventions every project follows.

## Repository layout

```
work-prod-projects/
├── prod-projects/                      
│   ├── databricks-pipelines/           
│   │   ├── dbricks-utils/             
│   │   ├── oracle-to-looker-etl/       
│   │   ├── realtime-data-stream/      
│   │   ├── receive-booking-monthly-backup/  
│   │   ├── receive-uph-kpis/            
│   │   └── shift-report-daily-update/   
│   ├── inventory-reconciliation-sql/    
│   └── looker-reporting-etl/            
└── internal-team-projects/             
    ├── kaizando-automation-appscript/   
    └── order-duration-efficiency-analysis/  
```

## Project index

| Project | Stack | What it does |
| :--- | :--- | :--- |
| [oracle-to-looker-etl](prod-projects/databricks-pipelines/oracle-to-looker-etl) | Python, Pandas, SQLAlchemy, Databricks | Pulls dangerous-goods stock from Oracle, cleans it, writes to Google Sheets that backs a Looker Studio dashboard. |
| [realtime-data-stream](prod-projects/databricks-pipelines/realtime-data-stream) | Python, SQLAlchemy, Databricks, Grafana | Runs transport KPI queries every 5 min and pushes values to a Sheet that Grafana streams to floor TVs. |
| [receive-booking-monthly-backup](prod-projects/databricks-pipelines/receive-booking-monthly-backup) | Python, Pandas, Databricks | Month-end snapshot of B-Beauty and ZFS overstock bookings with EAN→brand enrichment. |
| [receive-uph-kpis](prod-projects/databricks-pipelines/receive-uph-kpis) | Python, SQLAlchemy, Databricks | Nightly units-per-hour KPIs with Berlin→UTC windowing and idempotent Sheet refresh. |
| [shift-report-daily-update](prod-projects/databricks-pipelines/shift-report-daily-update) | Python, SQLAlchemy, Databricks | Nightly shift report; supports a date widget for backfills. |
| [inventory-reconciliation-sql](prod-projects/inventory-reconciliation-sql/inbound-booking-report) | Oracle SQL | Reconstructs item lifecycle from book-out/book-in pairs and de-duplicates manual-sorting scans. |
| [dg-compliance-pipeline](prod-projects/looker-reporting-etl/dg-compliance-pipeline) | Databricks, Oracle, Looker Studio | Dangerous-goods volume dashboard with a "days to threshold" forecast. |
| [qa-intelligence-engine](prod-projects/looker-reporting-etl/qa-intelligence-engine) | ETL, scoring logic | Consolidates quality audits into one weighted score for steering meetings. |
| [kaizando-automation-appscript](internal-team-projects/kaizando-automation-appscript) | Google Apps Script | Auto-translates Kaizen ideas, posts Chat cards, sends monthly reward emails. |
| [order-duration-efficiency-analysis](internal-team-projects/order-duration-efficiency-analysis) | Apps Script, SQL | Correlates background transport load with order processing delays. |

## Conventions

These hold across every project unless a project README says otherwise.

**Secrets.** Credentials are never committed. Databricks jobs read them from a
secret scope via `dbutils.secrets.get(...)`; Apps Script reads them from Script
Properties. Sheet IDs and dashboard URLs live in a per-project `config.json`,
which is gitignored — commit the `config.template.json` next to it instead and
copy it to `config.json` locally.

**Databricks Asset Bundles.** Each pipeline ships a `databricks.yml` describing
its job (schedule, cluster, libraries) and `dev`/`prod` targets. Validate and
deploy from the project folder:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run <job_key> -t dev
```

**Shared helpers.** The Databricks jobs import `dbricks-utils/common_utils.py`
(connections, config loading, UTC windowing, idempotent Sheet writes, Chat
notifications). On the workspace it lives under a shared path that each job
appends to `sys.path`; see that module and the per-job README for the exact path.

**Python deps.** Each Python project lists its pinned packages in
`requirements.txt`. The same pins are declared in `databricks.yml` so local and
job environments match.

## License

MIT — see [LICENSE](LICENSE).
