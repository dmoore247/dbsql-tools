# Databricks notebook source
# MAGIC %md
# MAGIC # Purpose
# MAGIC
# MAGIC This tool is designed to automatically create a dashboard that helps identify high usage for Interactive clusters still being used for SQL workloads, that would be better served via the specialized DBSQL Warehouse endpoints.
# MAGIC
# MAGIC ![Move to the right](https://github.com/user-attachments/assets/6978e97a-d76e-4949-bcbf-ff7c45b04276)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Instruction
# MAGIC
# MAGIC 1. Execute all cells in the "Set Up" section.
# MAGIC 2. Verify if the system schemas (access, compute, billing) are enabled in your workspace:
# MAGIC    - If enabled, proceed to Step 2: Create Dashboard.
# MAGIC    - If not enabled:
# MAGIC      - Proceed to Step 3: Enable System Tables (ensure an account admin is available to perform this step).
# MAGIC      - Then, return to Step 2.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Set Up

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import logging
from databricks.sdk import WorkspaceClient
import os
import requests
import re
import json

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %run ./utils/lakeview_dash_manager

# COMMAND ----------

HOSTNAME = spark.conf.get('spark.databricks.workspaceUrl')
TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Create Dashboard
# MAGIC
# MAGIC * The Lakeview dashboard is generated using the template: `./utils/Better SQL for Customers.lvdash.json`.
# MAGIC * This template is stored in the user's workspace location.
# MAGIC * All dashboard assets for each user are also saved in their respective workspace locations.
# MAGIC * A link to the dashboard draft will be provided, and direct you to the dashboard. 
# MAGIC * Click `publish` the dashboard and share it with other users.
# MAGIC
# MAGIC

# COMMAND ----------

user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
lv_workspace_path = f"/Users/{user_name}"
lv_dashboard_name = "Better SQL for Customers"
template_path = "./utils/Better SQL for Customers.lvdash.json"

# COMMAND ----------

# DBTITLE 1,Generate dashboard
lv_api = LakeviewDashManager(host=HOSTNAME, token=TOKEN)
lv_api.load_dash_local(template_path)
try: 
  # dashboard_link = lv_api.import_dash(path=lv_workspace_path, dashboard_name=lv_dashboard_name)
  dashboard_link = lv_api.import_dash(path=lv_workspace_path, dashboard_name=lv_dashboard_name)
  print(f"The Dashboard Draft is ready at: {dashboard_link}. \nClick ‘Publish’ to make it live and share it with others.")
except Exception as e:
  print(e)

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 3: Enable System Tables (Account Admin required)
# MAGIC
# MAGIC Execute all cells in this section to enable system schemas (compute, access, billing) if they are not already enabled in your workspace.
# MAGIC
# MAGIC **Note:** Only an account admin can run these cells.
# MAGIC
# MAGIC Account admin will be prompted to provide a list of email addresses to grant SELECT access to these system tables.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Since system tables are governed by Unity Catalog, you must have at least one Unity Catalog-enabled workspace in your account to enable and access system tables. System tables include data from all workspaces in your account but can only be accessed from a Unity Catalog-enabled workspace. (System tables are not available in AWS GovCloud regions.)
# MAGIC
# MAGIC The following system tables are required to run the dashboard:
# MAGIC   * `system.access.audit`
# MAGIC   * `system.compute.clusters`
# MAGIC   * `system.billing.usage`
# MAGIC
# MAGIC We need to enable three schemas: access, compute, and billing in the system catalog. The billing schema is enabled by default; other schemas must be enabled manually.
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

required_system_tables = ["system.access.audit", "system.compute.clusters", "system.billing.usage"]
required_system_schemas = ["compute", "access"]
metastore_id = w.metastores.current().metastore_id

# COMMAND ----------

# DBTITLE 1,Enable system tables
for schema_name in required_system_schemas:
    w.system_schemas.enable(metastore_id=metastore_id, schema_name=schema_name)

# COMMAND ----------

# DBTITLE 1,Grant access to users
users = input("Provide list of users to grant access to system tables, separating by commas: ")

# COMMAND ----------

users_list = [user.strip() for user in users.split(",")]
print(users_list)

for user in users_list:
  for table in required_system_tables:
    query = f"GRANT SELECT ON TABLE {table} TO `{user}`"
    spark.sql(query)
