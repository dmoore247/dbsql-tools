# Databricks DBSQL - Better SQL For Customer

## Purpose
Databricks customers have been eager for data warehousing solutions on their cloud data lakehouse. In the early days of Big Data on public clouds, data warehousing options were quite limited. Now, DBSQL Serverless is Databricks’ top SQL product in term of both performance and cost.

<img width="661" alt="image" src="https://github.com/user-attachments/assets/6978e97a-d76e-4949-bcbf-ff7c45b04276">

This tool automatically generates a dashboard to identify SQL workloads running on Interactive Clusters, which would be more efficiently managed by more specialized DBSQL Warehouse endpoints.

## Learn
Short video tutorial on the "Shift Right" program, pre-requsites, installation and operations.
[![Video Title](https://img.youtube.com/vi/KfrKfXLXwNw/0.jpg)](https://www.youtube.com/watch?v=KfrKfXLXwNw)

## Prepare

**Basic Prerequisites**

* Databricks workspace with Unity Catalog and 3 system table schemas (access, billing, compute) enabled
* Make sure `Verbose Audit Logs`  under Settings > Workspace Admin > Advanced > Other section is enabled
* The user executing this tool needs the following permissions:
  SELECT permission to below system tables:
    * system.access (audit table)
    * system.billing (usage table)
    * system.compute and (cluster and node_timeline tables)

**Advanced Prerequisites**

If the workspace lacks the required system table schemas, an account admin can run this tool to enable the system schemas. After that, a metastore admin can grant the necessary permissions to users.

## Installation

1. Clone this repo to Databricks workspace. 
  * Refer to this [documentation](https://docs.databricks.com/en/repos/git-operations-with-repos.html#run-git-operations-on-databricks-git-folders-repos) on how to use Git repo on Databricks. 
2. Attach the `main` notebook to a Databricks interactive UC-enabled cluster DBR14.3+
3. Follow the instruction in the `main` notebook

## Output

A dashboard will be generated to identify SQL workloads running on Interactive Clusters similar to below:

<img width="661" alt="image" src="https://github.com/anhhchu/dbsql-tools/blob/main/sample_dashboard.png?raw=true">
