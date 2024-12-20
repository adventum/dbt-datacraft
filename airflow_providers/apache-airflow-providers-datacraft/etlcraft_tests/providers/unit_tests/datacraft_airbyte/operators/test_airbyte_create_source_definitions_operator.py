import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_create_source_definitions_operator import (
    AirbyteCreateSourceDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.models import SourceDefinitionSpec
from pendulum import datetime


@pytest.mark.airbyte_test
def test_create_source_definition(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    source_definition_configuration = {
        "name": "TEST TG_STAT V1 FOR TESTS",
        "dockerRepository": "adventum/source-tg-stat",
        "dockerImageTag": "0.0.3",
        "documentationUrl": "",
    }

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_source_definition():
        create_result: dict[str, any] | AirbyteCreateSourceDefinitionsOperator = (
            AirbyteCreateSourceDefinitionsOperator(
                task_id="create_source_definition",
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                source_definition_configuration=source_definition_configuration,
            )
        )
        return create_result

    dag_object = test_dag_create_source_definition()
    dag_run = dag_object.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    task_id: str = "create_source_definition"
    result: SourceDefinitionSpec = dag_run.get_task_instance(task_id).xcom_pull(
        task_ids=task_id
    )

    assert result

    assert result.sourceDefinitionId
    assert result.name == source_definition_configuration["name"]
    assert (
        result.dockerRepository == source_definition_configuration["dockerRepository"]
    )
    assert result.dockerImageTag == source_definition_configuration["dockerImageTag"]
    assert (
        result.documentationUrl == source_definition_configuration["documentationUrl"]
    )

    # assert 0 == 1
