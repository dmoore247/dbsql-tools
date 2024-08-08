# Databricks DBSQL - Better SQL For Customer

## Purpose
Databricks customers have been eager for data warehousing solutions on their cloud lake house. In the early days of Big Data on public clouds, data warehousing options were quite limited. Now, DBSQL Serverless is Databricks’ top SQL product in term of both performance and cost.

<img width="661" alt="image" src="https://github.com/user-attachments/assets/6978e97a-d76e-4949-bcbf-ff7c45b04276">

This tool automatically generates a dashboard to identify SQL workloads running on Interactive Clusters, which would be more efficiently managed by a more specialized DBSQL Warehouse endpoints.

## Prerequisites

**Basic Prerequisites**

* Databricks workspace with Unity Catalog and 3 system table schemas (access, billing, compute) enabled
* The user executing this tool needs the following permissions:
  1. Permission to [create DBSQL warehouse](https://docs.databricks.com/en/compute/sql-warehouse/create.html) 
  2. SELECT permission to below system tables:
    * system.access (audit table)
    * system.billing (usage table)
    * system.compute and (cluster and node_timeline tables)

**Advanced Prerequisites**

If the workspace lacks the required system table schemas, an account admin can run this tool to enable the system schemas and grant the necessary permissions to users.

## Installation

1. [Clone](https://docs.databricks.com/en/repos/git-operations-with-repos.html#run-git-operations-on-databricks-git-folders-repos) this repo to Databricks workspace
2. Open the `main` notebook
3. Execute all cells in the "Set Up" section.
4. Verify if the system schemas (access, compute, billing) are enabled in your workspace:
  * If enabled, proceed to Step 2: Create Dashboard.
  * If not enabled:
    * Proceed to Step 3: Enable System Tables (ensure an account admin is available to perform this step).
    * Then return to Step 2.

## Output

A dashboard will be generated to identify SQL workloads running on Interactive Clusters (which is an anti-pattern)

<img width="661" alt="image" src="https://github.com/anhhchu/dbsql-tools/blob/main/sample%20dashboard.png?raw=true">
