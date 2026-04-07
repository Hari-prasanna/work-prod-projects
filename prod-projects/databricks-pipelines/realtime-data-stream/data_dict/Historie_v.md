# Data Dictionary: `HISTORIE_V` (History View)

This table defines the core columns and business logic used to calculate our internal transport KPIs.

| Column Name | Description & Business Context | Example Values / Key Usage |
| :--- | :--- | :--- |
| **`TRANSPORTLHMNR`** / **`LHMNR`** | **Load Carrier Number (Ladehilfsmittelnummer):** The unique physical identifier (barcode/RFID) of the pallet, box, or container being moved. We use this to track a specific item's journey through the warehouse. | `100456789` |
| **`TYP_ID`** | **Event Type ID:** A numeric code representing the specific status or event logged in the system at that moment in time. *Crucial for KPI logic.* | `42` = Active/Started<br>`47` = Completed/Finished<br>`39` or `5` = Blocked/Error<br>`132` = Order Status |
| **`CREATED`** | **Creation Timestamp:** The exact date and time the system recorded the event. We use this to filter our KPIs to rolling time windows (e.g., `SYSDATE - 5` for the last 5 days). | `2026-03-20 14:30:00` |
| **`ZUG_ID`** | **Transport Lifecycle ID:** A unique grouping ID that links all events of a single transport attempt together. We use this to ensure that a completion status (`47`) actually belongs to the current transport attempt, not an old one from last week. | `ZUG_88712` |
| **`TRANSPORTTASKQUELLE`** | **Transport Source:** The starting location, zone, or department where the transport task originated. | `WE` (Wareneingang)<br>`FIN_AP01`<br>`BGL` |
| **`TRANSPORTREQUESTZIEL`** / **`TRANSPORTTASKZIEL`** | **Transport Destination:** The target location, zone, or endpoint where the LHM is supposed to be dropped off. | `BSF_O`<br>`FIN_AP` |
| **`LAGBEZ`** | **Storage Area Designation (Lagerbezeichnung):** The specific physical zone or storage type associated with the record. We use this to filter out specific areas like the Automated Mini-Load (`AKL`) or isolate Goods Receipt (`Wareneingang`). | `Overstock`<br>`BGL`<br>`AKL`<br>`Wareneingang` |
| **`HOSTAUFTRNR`** | **Host Order Number (Auftragsnummer):** The overarching order number passed down from the parent ERP or WMS. We count this distinctively to see how many unique *orders* are active, rather than just individual *pallets*. | `ORD-2026-9912` |
| **`ZUGGRUPPE_TOKEN`** | **Group Token:** A unique text string that binds multiple transport tasks together under a single active order. We run a `COUNT(DISTINCT)` on this to accurately calculate the "Active Orders" KPI without duplicating data. | `TOK_A1B2C3` |
| **`ZUGGRUPPE_SENDER`** | **Group Sender/Trigger:** Identifies the internal system process or department that triggered the transport group. | `BESTANDFREIGABE` (Inventory Release) |