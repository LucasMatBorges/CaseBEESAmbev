import json
import os
from datetime import datetime, timedelta

import time
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


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
# Task 1: Fetch API and write Bronze to ADLS
# ===================================================
def fetch_and_save_bronze(**context):
    url = "https://api.openbrewerydb.org/v1/breweries"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    # Execution date from Airflow
    ds = context["ds"]
    file_path = f"breweries/date={ds}/raw.json"

    service_client = get_adls_client()
    fs_client = service_client.get_file_system_client(ADLS_CONTAINER_BRONZE)

    # Ensure directory exists
    dir_client = fs_client.get_directory_client(os.path.dirname(file_path))
    try:
        dir_client.create_directory()
    except Exception:
        pass  # Ignore if exists

    file_client = dir_client.create_file(os.path.basename(file_path))
    body = json.dumps(data)
    file_client.append_data(body, offset=0, length=len(body))
    file_client.flush_data(len(body))


# ===================================================
# Helper: Get Azure Management Token
# ===================================================
def get_management_token():
    credential = get_credential()
    token = credential.get_token("https://management.azure.com/.default")
    return token.token


# ===================================================
# Task 2 and 3: Call Azure Data Factory Pipelines
# ===================================================
def run_adf_pipeline(pipeline_name: str, **_):
    token = get_management_token()

    # 1. Trigger the ADF pipeline
    trigger_url = (
        f"https://management.azure.com/subscriptions/{ADF_SUBSCRIPTION_ID}/"
        f"resourceGroups/{ADF_RESOURCE_GROUP}/providers/Microsoft.DataFactory/"
        f"factories/{ADF_FACTORY_NAME}/pipelines/{pipeline_name}/createRun"
        "?api-version=2018-06-01"
    )

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    response = requests.post(trigger_url, headers=headers)
    response.raise_for_status()
    run_id = response.json()["runId"]

    print(f"Pipeline {pipeline_name} triggered successfully. runId={run_id}")

    # 2. Poll the run status until completion
    status_url = (
        f"https://management.azure.com/subscriptions/{ADF_SUBSCRIPTION_ID}/"
        f"resourceGroups/{ADF_RESOURCE_GROUP}/providers/Microsoft.DataFactory/"
        f"factories/{ADF_FACTORY_NAME}/pipelineruns/{run_id}"
        "?api-version=2018-06-01"
    )

    while True:
        status_response = requests.get(status_url, headers=headers)
        status_response.raise_for_status()
        status = status_response.json()["status"]

        print(f"[ADF STATUS] {pipeline_name}: {status}")

        if status in ("Succeeded", "Failed", "Cancelled"):
            break

        # wait before the next check
        time.sleep(10)

    if status != "Succeeded":
        raise Exception(f"ADF pipeline '{pipeline_name}' failed with status: {status}")

    print(f"ADF pipeline '{pipeline_name}' completed successfully.")


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

    bronze_task = PythonOperator(
        task_id="fetch_and_save_bronze",
        python_callable=fetch_and_save_bronze,
        provide_context=True,
    )

    silver_task = PythonOperator(
        task_id="run_adf_silver_pipeline",
        python_callable=run_adf_pipeline,
        op_kwargs={"pipeline_name": ADF_PIPELINE_SILVER},
    )

    gold_task = PythonOperator(
        task_id="run_adf_gold_pipeline",
        python_callable=run_adf_pipeline,
        op_kwargs={"pipeline_name": ADF_PIPELINE_GOLD},
    )

    bronze_task >> silver_task >> gold_task
