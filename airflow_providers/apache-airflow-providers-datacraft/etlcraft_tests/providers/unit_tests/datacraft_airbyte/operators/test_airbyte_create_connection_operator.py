import json
import pathlib

import pytest

from airflow.decorators import dag
from airflow.models import Connection
from airflow.providers.datacraft.airbyte.operators.airbyte_create_source_definitions_operator import (
    AirbyteCreateSourceDefinitionsOperator,
)
from airflow.providers.datacraft.airbyte.operators import (
    AirbyteCreateSourceOperator,
    AirbyteCreateDestinationDefinitionsOperator,
    AirbyteCreateDestinationOperator,
    AirbyteCreateConnectionOperator,
    AirbyteTriggerSyncOperator,
    AirbyteSyncSensor,
)
from airflow.providers.datacraft.airbyte.models import (
    SourceSpec,
    SourceDefinitionSpec,
    DestinationSpec,
    DestinationDefinitionSpec,
    ConnectionSpec,
    TriggerSyncResponseSpec,
)
from pendulum import datetime


@pytest.mark.airbyte_test
def test_create_and_update_connection_sync_after(
    airbyte_conn: Connection,
    get_workspace_id: str,
    clear_test_objects,
):
    """
    В тесте проходимся по полному процессу, создавая sourceDefinition, destinationDefinition,
    source, destination, соединяя их через connection и запуская sync

    Данные выгружаются в Тестовую БД Постгресс, которая создается в начале тестов
    (которая в т.ч для airflow компонентов)
    """

    """ Создаем source """
    create_source_definition_configuration = {
        "name": "TEST AIRBYTE FILE CONNECTOR JSON",
        "dockerRepository": "airbyte/source-file",
        "dockerImageTag": "0.5.0",
        "documentationUrl": "",
    }
    task_id_create: str = "create_source_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_source_definition():
        create_definition_result: (
            dict[str, any] | AirbyteCreateSourceDefinitionsOperator
        ) = AirbyteCreateSourceDefinitionsOperator(
            task_id=task_id_create,
            airbyte_conn_id=airbyte_conn,
            workspace_id=get_workspace_id,
            source_definition_configuration=create_source_definition_configuration,
        )
        return create_definition_result

    dag_object_create = test_dag_create_source_definition()
    dag_run_create = dag_object_create.test()
    create_result: SourceDefinitionSpec = dag_run_create.get_task_instance(
        task_id_create
    ).xcom_pull(task_ids=task_id_create)

    assert create_result.sourceDefinitionId

    connection_configuration = {
        "url": "http://json-schema.org/draft-07/schema#",
        "format": "json",
        "provider": {"storage": "HTTPS", "user_agent": True},
        "dataset_name": "Public Json",
    }
    task_id_update: str = "create_source"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_source():
        create_source_result: dict[str, any] | AirbyteCreateSourceOperator = (
            AirbyteCreateSourceOperator(
                task_id=task_id_update,
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
        task_id_update
    ).xcom_pull(task_ids=task_id_update)

    assert create_source_result
    assert create_source_result.sourceId

    """ Создаем destination """

    create_destination_definition_configuration = {
        "name": "Test Postgres Destination v1 for sync",
        "dockerRepository": "airbyte/destination-postgres",
        "dockerImageTag": "2.0.9",
        "documentationUrl": "",
    }
    task_id_create: str = "create_destination_definition"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_destination_definition():
        create_definition_result: (
            dict[str, any] | AirbyteCreateDestinationDefinitionsOperator
        ) = AirbyteCreateDestinationDefinitionsOperator(
            task_id=task_id_create,
            airbyte_conn_id=airbyte_conn,
            workspace_id=get_workspace_id,
            destination_definition_configuration=create_destination_definition_configuration,
        )
        return create_definition_result

    dag_object_create = test_dag_create_destination_definition()
    dag_run_create = dag_object_create.test()
    create_result: DestinationDefinitionSpec = dag_run_create.get_task_instance(
        task_id_create
    ).xcom_pull(task_ids=task_id_create)

    assert create_result.destinationDefinitionId

    connection_configuration = {
        "ssl": False,
        "host": "localhost",
        "port": 5438,
        "schema": "public",
        "database": "airflow_test",
        "password": "airflow",
        "ssl_mode": {"mode": "disable"},
        "username": "airflow",
        "drop_cascade": False,
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        "disable_type_dedupe": False,
    }
    task_id_update: str = "create_destination"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_destination():
        create_destination_result: dict[str, any] | AirbyteCreateDestinationOperator = (
            AirbyteCreateDestinationOperator(
                task_id=task_id_update,
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
        task_id_update
    ).xcom_pull(task_ids=task_id_update)

    assert create_destination_result
    assert create_destination_result.destinationId

    """ Создаем connection между source и destination """

    sync_catalog_path = (
        pathlib.Path(__file__).parent / "files_for_tests" / "syncCatalog_for_test.json"
    )
    sync_catalog = json.loads(sync_catalog_path.read_text())
    task_id_update: str = "create_connection"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_create_connection():
        create_connection_result: dict[str, any] | AirbyteCreateConnectionOperator = (
            AirbyteCreateConnectionOperator(
                task_id=task_id_update,
                airbyte_conn_id=airbyte_conn,
                name="TEST CONNECTION",
                source_id=create_source_result.sourceId,
                destination_id=create_destination_result.destinationId,
                params={"syncCatalog": sync_catalog, "status": "active"},
            )
        )
        return create_connection_result

    dag_object_create_connection = test_dag_create_connection()
    dag_run_update = dag_object_create_connection.test()

    create_connection_result: ConnectionSpec = dag_run_update.get_task_instance(
        task_id_update
    ).xcom_pull(task_ids=task_id_update)

    assert create_connection_result
    assert create_connection_result.connectionId

    """ Запускаем синхронизацию connection """

    task_id_sync_connection: str = "start_sync"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_start_sync_connection():
        start_sync_result: dict[str, any] | AirbyteTriggerSyncOperator = (
            AirbyteTriggerSyncOperator(
                task_id=task_id_sync_connection,
                airbyte_conn_id=airbyte_conn,
                workspace_id=get_workspace_id,
                connection_id=create_connection_result.connectionId,
            )
        )
        return start_sync_result

    dag_object_start_sync_connection = test_dag_start_sync_connection()
    dag_run_start_sync_connection = dag_object_start_sync_connection.test()

    sync_connection_result: TriggerSyncResponseSpec = (
        dag_run_start_sync_connection.get_task_instance(
            task_id_sync_connection
        ).xcom_pull(task_ids=task_id_sync_connection)
    )

    assert sync_connection_result

    """ Используем AirbyteSyncSensor для ожидания окончания синхронизации """

    task_id_sensor: str = "start_sensor"

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_start_sensor():
        start_sync_result: dict[str, any] | AirbyteSyncSensor = AirbyteSyncSensor(
            task_id=task_id_sensor,
            airbyte_conn_id=airbyte_conn,
            job_id=sync_connection_result.job["id"],
            poke_interval=30,
        )
        return start_sync_result

    dag_object_start_sensor = test_dag_start_sensor()
    dag_object_start_sensor.test()
