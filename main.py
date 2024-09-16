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
# MAGIC 1. Execute all cells in the "Set Up" and "Presequisites" sections.
# MAGIC 2. Verify that all the presequites are met:
# MAGIC   * If met, proceed to Step 2 - Create Dashboard
# MAGIC   * If NOT, contact Account Admin and Metastore admin and follow instructions in the `presequisites` notebook
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 0: Set Up

# COMMAND ----------

# MAGIC %pip install --quiet --upgrade databricks-sdk 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./utils/constants

# COMMAND ----------

# MAGIC %run ./utils/lakeview_dash_manager

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 1: Check for Prerequisites
# MAGIC
# MAGIC 1. Required System tables enabled. The following system tables are required to run the dashboard:
# MAGIC   * `system.access.audit`
# MAGIC   * `system.compute.clusters`
# MAGIC   * `system.billing.usage`
# MAGIC 2. User has permission to query system tables
# MAGIC 3. Medium or large warehouse exists in the workspace
# MAGIC
# MAGIC ---
# MAGIC * If all the validation passed, proceed to Step 2. 
# MAGIC * If not, resolve prerequisites
# MAGIC

# COMMAND ----------

# MAGIC %run ./utils/prereqs_validation

# COMMAND ----------

validator = PrerequisiteValidator()
validator.validate_presequites()

validator.results

# COMMAND ----------

# MAGIC %md
# MAGIC # Step 2: Create Dashboard
# MAGIC
# MAGIC * The Lakeview dashboard is generated using the template: `./Better SQL for Customers.lvdash.json`.
# MAGIC * This template is stored in the user's workspace location.
# MAGIC * All dashboard assets for each user are also saved in their respective workspace locations.
# MAGIC * A link to the dashboard draft will be provided, and direct you to the dashboard. 
# MAGIC * Attach a Medium or Large SQL warehouse to the dashboard and rerun queries if needed
# MAGIC * Click `publish` the dashboard and share it with other users.
# MAGIC
# MAGIC

# COMMAND ----------

lv_workspace_path = f"/Users/{USERNAME}"
lv_dashboard_name = "Better SQL for Customers"
template_path = "./Better SQL for Customers.lvdash.json"

# COMMAND ----------

# DBTITLE 1,Generate dashboard
lv_api = LakeviewDashManager(host=HOSTNAME, token=TOKEN)
lv_api.load_dash_local(template_path)
try: 
  # dashboard_link = lv_api.import_dash(path=lv_workspace_path, dashboard_name=lv_dashboard_name)
  dashboard_link = lv_api.import_dash(path=lv_workspace_path, dashboard_name=lv_dashboard_name)
  print(f"The Dashboard Draft is ready at: {dashboard_link}. \nAttach warehouse to the dashboard and rerun queries if needed \nClick ‘Publish’ to make it live and share it with others.")
except Exception as e:
  print(e)

# COMMAND ----------


