import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators import AirbyteListJobsOperator

from airflow.providers.datacraft.airbyte.models import JobListSpec
from pendulum import datetime

from airflow.providers.datacraft.airbyte.enums import JobStatusEnum


@pytest.mark.airbyte_test
def test_get_list_jobs(
    airbyte_conn: Connection,
):
    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_get_list_jobs():
        get_result: list[dict[str, any]] | AirbyteListJobsOperator = (
            AirbyteListJobsOperator(
                task_id="get_list_jobs",
                airbyte_conn_id=airbyte_conn,
                statuses=[JobStatusEnum.succeeded],
            )
        )
        return get_result

    dag_object = test_dag_get_list_jobs()
    dag_run = dag_object.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    task_id: str = "get_list_jobs"
    result: list[JobListSpec] = dag_run.get_task_instance(task_id).xcom_pull(
        task_ids=task_id
    )

    if not result:
        raise Exception("You have not any jobs in airbyte, but test passed")

    assert result
    assert len(result) >= 1
    assert result[0].id
    assert result[0].status
    assert result[0].attempts
