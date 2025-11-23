import json
import os
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

# Import your custom Operator from plugins
from adf_operator import TriggerADFOperator


# ===================================================
# Load environment variables
# ===================================================

ADLS_ACCOUNT_NAME = os.environ.get("ADLS_ACCOUNT_NAME")
ADLS_CONTAINER_BRONZE = os.environ.get("ADLS_CONTAINER_BRONZE", "bronze")

AZURE_CLIENT_ID = os.environ.get("AZURE_CLIENT_ID")
AZURE_CLIENT_SECRET = os.environ.get("AZURE_CLIENT_SECRET")
AZURE_TENANT_ID = os.environ.get("AZURE_TENANT_ID")

ADF_SUBSCRIPTION_ID = os.environ.get("ADF_SUBSCRIPTION_ID")
ADF_RESOURCE_GROUP = os.environ.get("ADF_RESOURCE_GROUP")
ADF_FACTORY_NAME = os.environ.get("ADF_FACTORY_NAME")

ADF_PIPELINE_BRONZE = os.environ.get("ADF_PIPELINE_BRONZE")
ADF_PIPELINE_SILVER = os.environ.get("ADF_PIPELINE_SILVER")
ADF_PIPELINE_GOLD = os.environ.get("ADF_PIPELINE_GOLD")


# ===================================================
# Authentication Helper
# ===================================================
def get_credential() -> ClientSecretCredential:
    return ClientSecretCredential(
        tenant_id=AZURE_TENANT_ID,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET,
    )


# ===================================================
# ADLS Client
# ===================================================
def get_adls_client() -> DataLakeServiceClient:
    credential = get_credential()
    return DataLakeServiceClient(
        account_url=f"https://{ADLS_ACCOUNT_NAME}.dfs.core.windows.net",
        credential=credential,
    )

# ===================================================
# DAG Definition
# ===================================================
default_args = {
    "owner": "lucas",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bees_breweries_adf_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["bees", "adf", "databricks"],
) as dag:

    # Bronze ADF Pipeline Task
    bronze_task = TriggerADFOperator(
        task_id="run_adf_bronze_pipeline",
        subscription_id=ADF_SUBSCRIPTION_ID,
        resource_group=ADF_RESOURCE_GROUP,
        factory_name=ADF_FACTORY_NAME,
        pipeline_name=ADF_PIPELINE_BRONZE,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET,
        tenant_id=AZURE_TENANT_ID,
    )

    # Silver ADF Pipeline Task
    silver_task = TriggerADFOperator(
        task_id="run_adf_silver_pipeline",
        subscription_id=ADF_SUBSCRIPTION_ID,
        resource_group=ADF_RESOURCE_GROUP,
        factory_name=ADF_FACTORY_NAME,
        pipeline_name=ADF_PIPELINE_SILVER,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET,
        tenant_id=AZURE_TENANT_ID,
    )

    # Gold ADF Pipeline Task
    gold_task = TriggerADFOperator(
        task_id="run_adf_gold_pipeline",
        subscription_id=ADF_SUBSCRIPTION_ID,
        resource_group=ADF_RESOURCE_GROUP,
        factory_name=ADF_FACTORY_NAME,
        pipeline_name=ADF_PIPELINE_GOLD,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET,
        tenant_id=AZURE_TENANT_ID,
    )

    bronze_task >> silver_task >> gold_task
