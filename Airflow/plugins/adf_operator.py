from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import ClientSecretCredential
import time

from adf_links import ADFPipelineRunLink


class TriggerADFOperator(BaseOperator):
    operator_extra_links = (ADFPipelineRunLink(),)

    @apply_defaults
    def __init__(
        self,
        subscription_id,
        resource_group,
        factory_name,
        pipeline_name,
        client_id,
        client_secret,
        tenant_id,
        poll_interval=15,  # seconds
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.factory_name = factory_name
        self.pipeline_name = pipeline_name
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.poll_interval = poll_interval

    def execute(self, context):
        credential = ClientSecretCredential(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
        )

        client = DataFactoryManagementClient(credential, self.subscription_id)

        # Trigger pipeline
        run = client.pipelines.create_run(
            self.resource_group,
            self.factory_name,
            self.pipeline_name
        )

        run_id = run.run_id
        self.log.info(f"Triggered ADF Pipeline: {self.pipeline_name}")
        self.log.info(f"Run ID: {run_id}")

        # Save for the UI link
        context["ti"].xcom_push("adf_run_id", run_id)

        # Polling ADF status
        while True:
            run_status = client.pipeline_runs.get(
                self.resource_group,
                self.factory_name,
                run_id
            )

            status = run_status.status
            self.log.info(f"[ADF STATUS] {self.pipeline_name}: {status}")

            if status in ("Succeeded", "Failed", "Cancelled"):
                break

            time.sleep(self.poll_interval)

        # Raise exception if failed
        if status != "Succeeded":
            raise AirflowException(
                f"ADF pipeline '{self.pipeline_name}' failed with status: {status}"
            )

        self.log.info(f"ADF pipeline '{self.pipeline_name}' completed successfully.")
        return run_id
