import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_create_destination_definitions_operator import (
    AirbyteCreateDestinationDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.operators.airbyte_create_destination_operator import (
    AirbyteCreateDestinationOperator,
)
from airflow.providers.datacraft.airbyte.operators.airbyte_update_destination_operator import (
    AirbyteUpdateDestinationOperator,
)
from airflow.providers.datacraft.airbyte.models import (
    DestinationSpec,
    DestinationDefinitionSpec,
)
from pendulum import datetime


@pytest.mark.airbyte_test
def test_update_destination(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    """
    В тесте сначала создаем destination_definition, потом создаем destination, и обновляем этот destination.
    """

    create_destination_definition_configuration = {
        "name": "Test Clickhouse Destination v1",
        "dockerRepository": "airbyte/destination-clickhouse",
        "dockerImageTag": "1.0.0",
        "documentationUrl": "https://docs.airbyte.com/integrations/destinations/clickhouse",
    }
    task_id_create_definition: str = "create_destination_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_destination_definition():
        create_definition_result: (
            dict[str, any] | AirbyteCreateDestinationDefinitionsOperator
        ) = AirbyteCreateDestinationDefinitionsOperator(
            task_id=task_id_create_definition,
            airbyte_conn_id=airbyte_conn,
            workspace_id=get_workspace_id,
            destination_definition_configuration=create_destination_definition_configuration,
        )
        return create_definition_result

    dag_object_create = test_dag_create_destination_definition()
    dag_run_create = dag_object_create.test()
    create_result: DestinationDefinitionSpec = dag_run_create.get_task_instance(
        task_id_create_definition
    ).xcom_pull(task_ids=task_id_create_definition)

    assert create_result.destinationDefinitionId

    connection_configuration = {
        "host": "example",
        "port": 8123,
        "database": "default",
        "username": "default",
        "destinationType": "clickhouse",
    }
    task_id_create_destination: str = "create_destination"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_destination():
        create_destination_result: dict[str, any] | AirbyteCreateDestinationOperator = (
            AirbyteCreateDestinationOperator(
                task_id=task_id_create_destination,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                destination_definition_id=create_result.destinationDefinitionId,
                name=create_result.name,
                connection_configuration=connection_configuration,
            )
        )
        return create_destination_result

    dag_object_update = test_dag_create_destination()
    dag_run_update = dag_object_update.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    create_destination_result: DestinationSpec = dag_run_update.get_task_instance(
        task_id_create_destination
    ).xcom_pull(task_ids=task_id_create_destination)

    assert create_destination_result
    assert create_destination_result.destinationId
    assert (
        create_destination_result.name
        == create_destination_definition_configuration["name"]
    )

    # Обновляем созданный destination
    connection_configuration_to_update = {
        "host": "example_updated",
        "port": 8125,
        "database": "default_updated",
        "username": "default_updated",
        "destinationType": "clickhouse",
    }
    task_id_update_destination: str = "update_destination"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_update_destination():
        update_destination_result: dict[str, any] | AirbyteUpdateDestinationOperator = (
            AirbyteUpdateDestinationOperator(
                task_id=task_id_update_destination,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                destination_id=create_destination_result.destinationId,
                name=create_destination_result.name + "_updated",
                connection_configuration=connection_configuration_to_update,
            )
        )
        return update_destination_result

    dag_object_update = test_dag_update_destination()
    dag_run_update = dag_object_update.test()

    update_destination_result: DestinationSpec = dag_run_update.get_task_instance(
        task_id_update_destination
    ).xcom_pull(task_ids=task_id_update_destination)

    assert update_destination_result
    assert update_destination_result.name == create_destination_result.name + "_updated"
