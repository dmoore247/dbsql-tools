# dbsql-tools
Tools for Databricks DBSQL management

DBSQL Serverless is Databricks' best SQL product. Databricks customers demanded many years ago for Data Warehousing on their cloud data lakes. In the early days of Big Data on public clouds, data warehousing options were nacent. At this time, The Databricks Lakehouse is the best Warehouse in the market.

<img width="661" alt="image" src="https://github.com/user-attachments/assets/6978e97a-d76e-4949-bcbf-ff7c45b04276">

## Prepare
The dashboard and the user importing and 'owning' the dashboard needs access to system table schemas:
- `system.access` (audit table)
- `system.billing` (usage table)
- `system.compute` (cluster and node_timeline tables)

  
Also see [documentation for enabling system tables](https://docs.databricks.com/en/admin/system-tables/index.html#enable-system-table-schemas) to enable system tables.


## Install
- Download "Better SQL for Customers.lvdash.json"
- Go to Databricks "Dashboards", import the json file.
