from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.identity import ClientSecretCredential

from .adf_links import ADFPipelineRunLink


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

    def execute(self, context):
        cred = ClientSecretCredential(
            client_id=self.client_id,
            client_secret=self.client_secret,
            tenant_id=self.tenant_id,
        )

        client = DataFactoryManagementClient(cred, self.subscription_id)

        run = client.pipelines.create_run(
            self.resource_group,
            self.factory_name,
            self.pipeline_name
        )

        run_id = run.run_id
        self.log.info(f"Triggered ADF Pipeline. Run ID: {run_id}")

        context['ti'].xcom_push(key="adf_run_id", value=run_id)

        return run_id
