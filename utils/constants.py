# Databricks notebook source
import pandas as pd
import logging
from databricks.sdk import WorkspaceClient
import os
import requests
import re
import json
from dbruntime.databricks_repl_context import get_context

w = WorkspaceClient()

TOKEN = get_context().apiToken
HOSTNAME = get_context().browserHostName
USERNAME = get_context().user
METASTORE_ID = w.metastores.current().metastore_id

REQUIRED_SYSTEM_TABLES = ["system.access.audit", "system.compute.clusters", "system.billing.usage"]
REQUIRED_SYSTEM_SCHEMAS = ["billing", "compute", "access", "information_schema"]
