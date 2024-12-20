import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_create_destination_definitions_operator import (
    AirbyteCreateDestinationDefinitionsOperator,
)

from airflow.providers.datacraft.airbyte.models import DestinationDefinitionSpec
from pendulum import datetime


@pytest.mark.airbyte_test
def test_create_destination_definition(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    destination_definition_configuration = {
        "name": "Test Clickhouse Destination v1",
        "dockerRepository": "airbyte/destination-clickhouse",
        "dockerImageTag": "1.0.0",
        "documentationUrl": "https://docs.airbyte.com/integrations/destinations/clickhouse",
    }

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_destination_definition():
        create_result: dict[str, any] | AirbyteCreateDestinationDefinitionsOperator = (
            AirbyteCreateDestinationDefinitionsOperator(
                task_id="create_destination_definition",
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                destination_definition_configuration=destination_definition_configuration,
            )
        )
        return create_result

    dag_object = test_dag_create_destination_definition()
    dag_run = dag_object.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    task_id: str = "create_destination_definition"
    result: DestinationDefinitionSpec = dag_run.get_task_instance(task_id).xcom_pull(
        task_ids=task_id
    )

    assert result

    assert result.destinationDefinitionId
    assert result.name == destination_definition_configuration["name"]
    assert (
        result.dockerRepository
        == destination_definition_configuration["dockerRepository"]
    )
    assert (
        result.dockerImageTag == destination_definition_configuration["dockerImageTag"]
    )
    assert (
        result.documentationUrl
        == destination_definition_configuration["documentationUrl"]
    )
