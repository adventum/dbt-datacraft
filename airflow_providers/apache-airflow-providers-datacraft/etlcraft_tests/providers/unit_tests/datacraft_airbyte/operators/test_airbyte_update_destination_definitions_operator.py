import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_update_destination_definitions_operator import (
    AirbyteUpdateDestinationDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.operators.airbyte_create_destination_definitions_operator import (
    AirbyteCreateDestinationDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.models import DestinationDefinitionSpec
from pendulum import datetime


@pytest.mark.airbyte_test
def test_update_destination_definition(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    """
    В тесте сначала создаем destination_definition, а потом изменяем его.
    # В API Airbyte написано, что поменять можно только dockerImageTag
    """

    create_destination_definition_configuration = {
        "name": "Test Clickhouse Destination v1",
        "dockerRepository": "airbyte/destination-postgres",
        "dockerImageTag": "2.3.2",
        "documentationUrl": "https://docs.airbyte.com/integrations/destinations/clickhouse",
    }
    task_id_create: str = "create_destination_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_destination_definition():
        create_result: dict[str, any] | AirbyteCreateDestinationDefinitionsOperator = (
            AirbyteCreateDestinationDefinitionsOperator(
                task_id=task_id_create,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                destination_definition_configuration=create_destination_definition_configuration,
            )
        )
        return create_result

    dag_object_create = test_dag_create_destination_definition()
    dag_run_create = dag_object_create.test()
    create_result: DestinationDefinitionSpec = dag_run_create.get_task_instance(
        task_id_create
    ).xcom_pull(task_ids=task_id_create)

    assert create_result.destinationDefinitionId

    update_destination_definition_configuration = {
        "name": "Test Clickhouse Destination v1 Updated",
        "dockerRepository": "airbyte/destination-postgres",
        "dockerImageTag": "2.4.0",
        "documentationUrl": "https://docs.airbyte.com/integrations/destinations/clickhouse",
    }
    task_id_update: str = "update_destination_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_update_destination_definition():
        update_result: dict[str, any] | AirbyteUpdateDestinationDefinitionsOperator = (
            AirbyteUpdateDestinationDefinitionsOperator(
                task_id=task_id_update,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                destination_definition_id=create_result.destinationDefinitionId,
                destination_definition_configuration=update_destination_definition_configuration,
            )
        )
        return update_result

    dag_object_update = test_dag_update_destination_definition()
    dag_run_update = dag_object_update.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    update_result: DestinationDefinitionSpec = dag_run_update.get_task_instance(
        task_id_update
    ).xcom_pull(task_ids=task_id_update)

    assert update_result
    assert (
        create_result.destinationDefinitionId == update_result.destinationDefinitionId
    )
    assert (
        update_result.dockerImageTag
        == update_destination_definition_configuration["dockerImageTag"]
    )
