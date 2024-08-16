# Databricks notebook source
# MAGIC %md
# MAGIC # Enable System Tables & Grant System Tables Permissions 
# MAGIC
# MAGIC **Account Admin & Metastore Admin required**
# MAGIC
# MAGIC Execute all cells in this section to enable system schemas (compute, access, billing) if they are not already enabled in your workspace.
# MAGIC
# MAGIC **Note:** 
# MAGIC
# MAGIC * Only [Account Admins](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/admin-privileges#account-admins) has permission to enable system schemas. 
# MAGIC * Only [Metastore Admins](https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/admin-privileges#metastore-admins) can grant access to these system tables.
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

# MAGIC %pip install --quiet --upgrade databricks-sdk 
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ./utils/constants

# COMMAND ----------

# MAGIC %md
# MAGIC ### To run below cells and enable system tables, an account admin is required

# COMMAND ----------

# DBTITLE 1,Enable system tables
for schema_name in REQUIRED_SYSTEM_SCHEMAS:
    w.system_schemas.enable(metastore_id=METASTORE_ID, schema_name=schema_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### To run below cells and grant permission to users, a metastore admin is required.

# COMMAND ----------

# DBTITLE 1,Grant access to users
users = input("Provide list of users to grant access to system tables, separating by commas: ")

# COMMAND ----------

users_list = [user.strip() for user in users.split(",")]

for user in users_list:
  spark.sql(f"GRANT USE CATALOG ON CATALOG system TO `{user}`")
  for schema in REQUIRED_SYSTEM_SCHEMAS:
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA system.{schema} TO `{user}`")
  for table in REQUIRED_SYSTEM_TABLES:
    query = f"GRANT SELECT ON TABLE {table} TO `{user}`"
    print(query)
    spark.sql(query)

# COMMAND ----------


