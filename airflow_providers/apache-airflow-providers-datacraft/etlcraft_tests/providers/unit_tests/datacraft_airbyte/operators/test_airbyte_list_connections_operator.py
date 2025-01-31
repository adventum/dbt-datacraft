import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators import AirbyteListConnectionsOperator

from airflow.providers.datacraft.airbyte.models import ConnectionSpec
from pendulum import datetime


@pytest.mark.airbyte_test
def test_get_list_connections(
    airbyte_conn: Connection,
    get_workspace_id: str,
):
    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_get_list_connections():
        get_result: list[dict[str, any]] | AirbyteListConnectionsOperator = (
            AirbyteListConnectionsOperator(
                task_id="get_list_connections",
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
            )
        )
        return get_result

    dag_object = test_dag_get_list_connections()
    dag_run = dag_object.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    task_id: str = "get_list_connections"
    result: list[ConnectionSpec] = dag_run.get_task_instance(task_id).xcom_pull(
        task_ids=task_id
    )

    if not result:
        raise Exception("You have not any connections in airbyte, but test passed")

    assert result
    assert len(result) >= 1
    assert result[0].connectionId
    assert result[0].name
    assert result[0].sourceId
    assert result[0].destinationId
