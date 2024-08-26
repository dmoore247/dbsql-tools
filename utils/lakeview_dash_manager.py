# Databricks notebook source
import base64
import json
import re
from databricks.sdk import WorkspaceClient
import requests


class LakeviewDashManager:
    """Lakeview Dashboard API class
    """
    def __init__(self, host=None, token=None):
        self.host = host
        self.lakeview_json = None
        self.dashboard_id = None
        w = WorkspaceClient()


    def search_dash(self, dashboard_name):
        dashboards = w.lakeview.list()
        print(dashboards)
    
    def create_dash(self, path, dashboard_name, warehouse_id=None):
        dashboard = w.lakeview.create(
            display_name=dashboard_name, 
            parent_path=path, 
            serialized_dashboard=json.dumps(self.lakeview_json),
            warehouse_id=warehouse_id
            )
        dashboard_id = dashboard.dashboard_id
        return f"https://{self.host}/sql/dashboardsv3/{dashboard_id}"
    
    def update_dash(self, dashboard_id):
        dashboard = w.lakeview.update(
            dashboard_id=dashboard_id,
            serialized_dashboard=json.dumps(self.lakeview_json)
            )
        return f"https://{self.host}/sql/dashboardsv3/{dashboard_id}"
    
    def save_dash_local(self, path):
        """save lakeview dashboard json to local file

        Args:
            path: path to save the lakeview dashboard to
        """
        with open(path, "w") as f:
            json.dump(self.lakeview_json, f, indent=4)
        return
    
    def load_dash_local(self, path):
        """load lakeview dashboard template from local file

        Args:
            path: path to load the lakeview dashboard tempate from
        """
        with open(path, "r") as f:
            self.lakeview_json = json.load(f)

    def set_query_uc(self, catalog_name, schema_name, table_name):
        """update the catalog_name, schema_name, table_name in the lakeview dashboard json obj

        Args:
            catalog_name: catalog_name in string
            schema_name: schema_name in string
            table_name: metrics table_name in string
        """
        for item in self.lakeview_json["datasets"]:
            item["query"] = re.sub(r"CATALOG_NAME", catalog_name, item["query"])
            item["query"] = re.sub(r"SCHEMA_NAME", schema_name, item["query"])
            item["query"] = re.sub(r"TABLE_NAME", table_name, item["query"])

    @staticmethod
    def basee64_encode(x):
        """base64 encode a string

        Args:
            x: string to encode

        Returns:
            endcoded string
        """
        encoded_bytes = base64.b64encode(x.encode("utf-8"))
        encoded_string = encoded_bytes.decode("utf-8")
        return encoded_string
