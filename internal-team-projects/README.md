# Internal team projects

Smaller, mostly self-serve tools — Google Apps Script automations and analysis
scripts that solve a specific operational problem without a full pipeline.

## Projects

### [kaizando-automation-appscript](./kaizando-automation-appscript)

Automates the LUU Kaizen (continuous-improvement) idea flow: translates incoming
ideas to German, posts a Google Chat card to the management channel on each new
submission, and emails contributors their monthly reward-point balance. Built on
Google Apps Script bound to the intake spreadsheet.

### [order-duration-efficiency-analysis](./order-duration-efficiency-analysis)

A one-click Apps Script analysis that joins order-release timestamps, workstation
scans, and system-wide transport logs to measure how background transport load
correlates with order-processing delays at palletization workstations. The output
supported a change to the warehouse control system's prioritisation logic.
