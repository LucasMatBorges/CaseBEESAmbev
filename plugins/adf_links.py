from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.models import XCom

class ADFPipelineRunLink(BaseOperatorLink):
    name = "Open ADF Run in Azure Portal"

    def get_link(self, operator, dttm):
        run_id = XCom.get_one(
            key="adf_run_id",
            dag_id=operator.dag_id,
            task_id=operator.task_id,
            execution_date=dttm,
        )

        if not run_id:
            return None

        return (
            f"https://portal.azure.com/#view/"
            f"Microsoft_Azure_DataFactory/RunDetailsBlade/"
            f"runId/{run_id}/"
            f"subscriptionId/{operator.subscription_id}/"
            f"resourceGroup/{operator.resource_group}/"
            f"factoryName/{operator.factory_name}"
        )
