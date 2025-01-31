import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_create_source_definitions_operator import (
    AirbyteCreateSourceDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.operators.airbyte_create_source_operator import (
    AirbyteCreateSourceOperator,
)
from airflow.providers.datacraft.airbyte.operators.airbyte_update_source_operator import (
    AirbyteUpdateSourceOperator,
)
from airflow.providers.datacraft.airbyte.models import SourceSpec, SourceDefinitionSpec
from pendulum import datetime


@pytest.mark.airbyte_test
def test_update_source(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    """
    В тесте сначала создаем source_definition, потом создаем source, и обновляем этот source.
    """

    create_source_definition_configuration = {
        "name": "TEST TG_STAT V1 FOR TESTS",
        "dockerRepository": "adventum/source-tg-stat",
        "dockerImageTag": "0.0.3",
        "documentationUrl": "",
    }
    task_id_create_definition: str = "create_source_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_source_definition():
        create_definition_result: (
            dict[str, any] | AirbyteCreateSourceDefinitionsOperator
        ) = AirbyteCreateSourceDefinitionsOperator(
            task_id=task_id_create_definition,
            airbyte_conn_id=airbyte_conn,
            workspace_id=get_workspace_id,
            source_definition_configuration=create_source_definition_configuration,
        )
        return create_definition_result

    dag_object_create = test_dag_create_source_definition()
    dag_run_create = dag_object_create.test()
    create_result: SourceDefinitionSpec = dag_run_create.get_task_instance(
        task_id_create_definition
    ).xcom_pull(task_ids=task_id_create_definition)

    assert create_result.sourceDefinitionId

    connection_configuration = {
        "channels": ["premium"],
        "date_range": {"date_range_type": "last_n_days", "last_days_count": 10},
        "access_token": "test_token",
        "client_name_constant": "test",
        "product_name_constant": "test",
    }
    task_id_create_source: str = "create_source"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_source():
        create_source_result: dict[str, any] | AirbyteCreateSourceOperator = (
            AirbyteCreateSourceOperator(
                task_id=task_id_create_source,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                source_definition_id=create_result.sourceDefinitionId,
                name=create_result.name,
                connection_configuration=connection_configuration,
            )
        )
        return create_source_result

    dag_object_update = test_dag_create_source()
    dag_run_update = dag_object_update.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    create_source_result: SourceSpec = dag_run_update.get_task_instance(
        task_id_create_source
    ).xcom_pull(task_ids=task_id_create_source)

    assert create_source_result
    assert create_source_result.sourceId
    assert create_source_result.name == create_source_definition_configuration["name"]

    # Обновляем созданный source
    connection_configuration_to_update = {
        "channels": ["premium"],
        "date_range": {"date_range_type": "last_n_days", "last_days_count": 20},
        "access_token": "test_token_updated",
        "client_name_constant": "test_updated",
        "product_name_constant": "test_updated",
    }
    task_id_update_source: str = "update_source"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_update_source():
        update_source_result: dict[str, any] | AirbyteUpdateSourceOperator = (
            AirbyteUpdateSourceOperator(
                task_id=task_id_update_source,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                source_id=create_source_result.sourceId,
                name=create_source_result.name,
                connection_configuration=connection_configuration_to_update,
            )
        )
        return update_source_result

    dag_object_update = test_dag_update_source()
    dag_run_update = dag_object_update.test()

    update_source_result: SourceSpec = dag_run_update.get_task_instance(
        task_id_update_source
    ).xcom_pull(task_ids=task_id_update_source)

    assert update_source_result
    assert (
        update_source_result.connectionConfiguration["client_name_constant"]
        == "test_updated"
    )
    assert (
        update_source_result.connectionConfiguration["product_name_constant"]
        == "test_updated"
    )
