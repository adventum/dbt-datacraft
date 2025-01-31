import base64
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.sensors.base import BaseSensorOperator


class AirbyteSyncSensor(BaseSensorOperator):
    """
    Custom sensor for waiting for Airbyte synchronization to complete.
    :param airbyte_conn_id: Required. Airbyte connection id
    :param job_id: Airbyte job id for status check
    """

    def __init__(self, airbyte_conn_id: str, job_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.airbyte_conn_id: str = airbyte_conn_id
        self.job_id: str = job_id
        self.api_version: str = "v1"

    def poke(self, context):
        hook = AirbyteHook(
            airbyte_conn_id=self.airbyte_conn_id,
            api_version=self.api_version,
        )
        conn: Connection = hook.get_connection(self.airbyte_conn_id)
        token: str = base64.b64encode(
            bytes(f"{conn.login}:{conn.password}", "utf-8")
        ).decode("utf-8")

        self.log.info(f"Checking status Airbyte Job ID: {self.job_id}")

        response = hook.run(
            endpoint="api/v1/jobs/get",
            json={"id": self.job_id},
            headers={
                "accept": "application/json",
                "Authorization": f"Basic {token}",
            },
        ).json()

        if not response:
            raise AirflowException("Some error while requesting a job status")

        job_status = response.get("job", {}).get("status", None)
        self.log.info(f"Current task status: {job_status}")

        # Checking the task status
        if job_status in ["completed", "succeeded"]:
            return True
        elif job_status in ["failed", "cancelled"]:
            self.log.info(f"ERROR WHILE SYNC - {response}")
            raise AirflowException(f"Synchronization ended with an error: {job_status}")

        # If the task is in progress, return False to continue checking
        return False
