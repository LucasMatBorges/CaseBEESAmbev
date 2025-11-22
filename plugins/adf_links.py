from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.xcom import XCom


class ADFPipelineRunLink(BaseOperatorLink):
    name = "Azure Data Factory Run"

    def get_link(self, operator, dttm):
        run_id = XCom.get_value(
            key="adf_run_id",
            task_id=operator.task_id,
            dag_id=operator.dag_id,
            execution_date=dttm,
        )

        if not run_id:
            return None

        url = (
            f"https://portal.azure.com/#view/"
            f"Microsoft_Azure_DataFactory/RunDetailsBlade/"
            f"runId/{run_id}/"
            f"subscriptionId/{operator.subscription_id}/"
            f"resourceGroup/{operator.resource_group}/"
            f"factoryName/{operator.factory_name}"
        )
        return url
