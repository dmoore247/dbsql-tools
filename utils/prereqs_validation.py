# Databricks notebook source
# MAGIC %run ./constants

# COMMAND ----------

from collections import namedtuple
class PrerequisiteValidator:
    def __init__(self):
        self.results = []
        self.ValidationResult = namedtuple('ValidationResult', ['requirement', 'is_pass', 'error'])

    def validate_system_schemas(self):
        """
        Validates the presence of required system schemas in the metastore.

        This function checks if the required system schemas, as specified in the 
        REQUIRED_SYSTEM_SCHEMAS list, are present in the current metastore. If all 
        required schemas are present, it returns a ValidationResult indicating success. 
        Otherwise, it returns a ValidationResult indicating failure and lists the missing schemas.

        Returns:
            ValidationResult: A named tuple containing:
                - requirement (str): The requirement being validated ('system tables').
                - is_pass (bool): True if validation passes, False otherwise.
                - error (str or None): An error message if validation fails, None if it passes.
        """
        
        try:
            system_schemas = w.system_schemas.list(metastore_id=METASTORE_ID)
            schemas = {schema.schema for schema in system_schemas}
            if set(REQUIRED_SYSTEM_SCHEMAS).issubset(schemas):
                result = self.ValidationResult(requirement='system tables', is_pass=True, error=None)
            else:
                missing_schemas = set(REQUIRED_SYSTEM_SCHEMAS) - schemas
                result = self.ValidationResult(requirement='system tables', is_pass=False, error=f'The necessary schemas {missing_schemas} are not enabled. Go to `prerequisites` notebook for next steps')
        except Exception as e:
            result = self.ValidationResult(requirement='system tables', is_pass=False, error=str(e) + f". You do not have permission to enable or access the system schemas. Verify if required schemas {REQUIRED_SYSTEM_SCHEMAS} exists in the workspace. If not, go to `prerequisites` notebook for next steps")
        
        return result


    def validate_permission(self):
        """
        Validates the user's permissions for accessing required system tables.

        This function checks if the user has the necessary permissions to access 
        the required system tables as specified in the REQUIRED_SYSTEM_TABLES list. 
        It queries the system.information_schema.table_privileges to determine the 
        tables the user has access to. If all required tables are accessible, it 
        returns a ValidationResult indicating success. Otherwise, it returns a 
        ValidationResult indicating failure and lists the missing tables.

        Returns:
            ValidationResult: A named tuple containing:
                - requirement (str): The requirement being validated ('permission').
                - is_pass (bool): True if validation passes, False otherwise.
                - error (str or None): An error message if validation fails, None if it passes.
        """

        USERID = w.current_user.me().id

        USER_GROUPS = w.users.get(id=USERID).groups

        USER_GROUPS = [g.display for g in w.users.get(id=USERID).groups] + [USERNAME]

        USER_GROUP_NAMES = ','.join([f"'{user}'" for user in USER_GROUPS])

        try:
            rows = spark.sql(f"select table_catalog, table_schema, table_name from system.information_schema.table_privileges where table_catalog = 'system' AND grantee IN ({USER_GROUP_NAMES}, 'account users');").collect()
            granted_tables = [row.table_catalog + '.' + row.table_schema + '.' + row.table_name for row in rows]

            if set(REQUIRED_SYSTEM_TABLES).issubset(set(granted_tables)):
                result = self.ValidationResult(requirement='permission', is_pass=True, error=None)
            else:
                missing_tables = set(REQUIRED_SYSTEM_TABLES) - set(granted_tables)
                result = self.ValidationResult(requirement='permission', is_pass=False, error=f"You do not have permission to access the necessary system tables {missing_tables}. Go to to `Prerequisites` notebook and Contact metastore admin for permission")
            
            return result

        except Exception as e:
            print(e)
            print("You do not have permission to query the system.information_schema table to check other permissions. Contact metastore admin for permission")

        


    def validate_warehouse(self):
        """
        Validates the presence of acceptable DBSQL warehouses for the Lakeview dashboard.

        This function checks if there are any DBSQL warehouses with acceptable sizes 
        ('Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large') available 
        in the current workspace. If at least one acceptable warehouse is found, it 
        returns a ValidationResult indicating success. Otherwise, it returns a 
        ValidationResult indicating failure and provides an error message.

        Returns:
            ValidationResult: A named tuple containing:
                - requirement (str): The requirement being validated ('warehouse').
                - is_pass (bool): True if validation passes, False otherwise.
                - error (str or None): An error message if validation fails, None if it passes.
        """

        # List of DBSQL warehouses for the Lakeview dashboard
        warehouses = [(x.name, x.cluster_size) for x in w.warehouses.list()]

        acceptable_sizes = {'Medium', 'Large', 'X-Large', '2X-Large', '3X-Large', '4X-Large'}
        acceptable_warehouses = [size for _, size in warehouses if size in acceptable_sizes]

        if not acceptable_warehouses:
            result = self.ValidationResult(requirement='warehouse', is_pass=False, error="Please have at least a Medium or Larger warehouse available")
        else:
            result = self.ValidationResult(requirement='warehouse', is_pass=True, error=None)

        return result


    def validate_presequites(self):
        """
        Validates the prerequisites for accessing the Lakeview dashboard.

        This function performs a series of validation checks to ensure that the 
        necessary system schemas, user permissions, and acceptable DBSQL warehouses 
        are available. It calls three separate validation functions:
        - validate_system_schemas: Checks for the presence of required system schemas.
        - validate_permission: Checks if the user has the necessary permissions to 
        access required system tables.
        - validate_warehouse: Checks for the presence of acceptable DBSQL warehouses.

        The results of these validations are collected and added to the results set.

        Returns:
            results (set): A set containing the results of the validation checks.
        """
        system_schemas_validation = self.validate_system_schemas()
        permission_validation = self.validate_permission()
        warehouse_validation = self.validate_warehouse()
        self.results.append(system_schemas_validation)
        self.results.append(permission_validation)
        self.results.append(warehouse_validation)


# COMMAND ----------


