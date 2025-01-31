import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_update_source_definitions_operator import (
    AirbyteUpdateSourceDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.operators.airbyte_create_source_definitions_operator import (
    AirbyteCreateSourceDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.models import SourceDefinitionSpec
from pendulum import datetime


@pytest.mark.airbyte_test
def test_update_source_definition(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    """
    В тесте сначала создаем source_definition, а потом изменяем его.
    В API Airbyte написано, что поменять можно только dockerImageTag
    """

    create_source_definition_configuration = {
        "name": "TEST TG_STAT V1 FOR TESTS",
        "dockerRepository": "adventum/source-tg-stat",
        "dockerImageTag": "0.0.3",
        "documentationUrl": "",
    }
    task_id_create: str = "create_source_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_source_definition():
        create_result: dict[str, any] | AirbyteCreateSourceDefinitionsOperator = (
            AirbyteCreateSourceDefinitionsOperator(
                task_id=task_id_create,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                source_definition_configuration=create_source_definition_configuration,
            )
        )
        return create_result

    dag_object_create = test_dag_create_source_definition()
    dag_run_create = dag_object_create.test()
    create_result: SourceDefinitionSpec = dag_run_create.get_task_instance(
        task_id_create
    ).xcom_pull(task_ids=task_id_create)

    assert create_result.sourceDefinitionId

    update_source_definition_configuration = {
        "name": "TEST TG_STAT V1 FOR TESTS",
        "dockerRepository": "adventum/source-tg-stat",
        "dockerImageTag": "0.0.2",
        "documentationUrl": "",
    }
    task_id_update: str = "update_source_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_update_source_definition():
        update_result: dict[str, any] | AirbyteUpdateSourceDefinitionsOperator = (
            AirbyteUpdateSourceDefinitionsOperator(
                task_id=task_id_update,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                source_definition_id=create_result.sourceDefinitionId,
                source_definition_configuration=update_source_definition_configuration,
            )
        )
        return update_result

    dag_object_update = test_dag_update_source_definition()
    dag_run_update = dag_object_update.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    update_result: SourceDefinitionSpec = dag_run_update.get_task_instance(
        task_id_update
    ).xcom_pull(task_ids=task_id_update)

    assert update_result
    assert create_result.sourceDefinitionId == update_result.sourceDefinitionId
    assert (
        update_result.dockerImageTag
        == update_source_definition_configuration["dockerImageTag"]
    )
