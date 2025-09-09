import time

import docker
import pytest
from airflow import settings
from airflow.models import Connection
from airflow.utils import db
from airflow.utils.session import provide_session

from .object_deleters import (
    sources_deleter,
    source_definitions_deleter,
    destinations_deleter,
    destination_definitions_deleter,
    connections_deleter,
    collect_airbyte_objects,
)


@pytest.fixture(scope="session", autouse=True)
def setup_db():
    # Создаем контейнер с базой
    client = docker.from_env()

    try:
        # Проверяем, существует ли контейнер с именем "providers_postgres_tests"
        # и если да, но выключен - запускаем
        existing_container = client.containers.get("providers_postgres_tests")
        if existing_container.status == "exited":
            existing_container.start()
    except:  # noqa: E722
        # Запуск контейнера PostgreSQL с маппингом на порт 5438
        # Если в системе нет контейнера с базой для тестов то создаем
        # и немного ждем для инициализации
        client.containers.run(
            "postgres:13",
            name="providers_postgres_tests",
            environment={
                "POSTGRES_USER": "airflow",
                "POSTGRES_PASSWORD": "airflow",
                "POSTGRES_DB": "airflow_test",
            },
            ports={"5432/tcp": 5438},
            detach=True,
        )
        time.sleep(5)

    settings.SQL_ALCHEMY_CONN = (
        "postgresql+psycopg2://airflow:airflow@localhost:5438/airflow_test"
    )

    # Очищаем бд до тестов
    db.resetdb()

    # Инициализируем бд для тестов
    db.initdb()

    yield

    # Очищаем бд после тестов
    db.resetdb()


@pytest.fixture(scope="session")
def airbyte_conn():
    """
    Создает соединение с airbyte, нужно для тестов,
    но работает только с локальным airbyte, с заводскими настройками(логин/пароль)

    И, важно: тесты на компоненты airbyte, например на операторы airbyte,
    будут записывать данные в не тестовый инстанс airbyte !
    Это ничего не сломает, но может создать ненужные sources, destinations, connections
    """

    conn_id = "airbyte_default"
    conn_type = "http"
    host = "172.17.0.1"
    port = 8001
    schema = "http"
    login = "airbyte"
    password = "password"

    # Устанавливаем соединение в Airflow
    @provide_session
    def create_connection(session=None):
        conn = Connection(
            conn_id=conn_id,
            conn_type=conn_type,
            host=host,
            port=port,
            schema=schema,
            login=login,
            password=password,
        )
        session.add(conn)
        session.commit()

    create_connection()
    return conn_id


# @pytest.fixture(scope='session', autouse=True)
# def setup_db():
#     # Создание временной БД в памяти
#     settings.SQL_ALCHEMY_CONN = "sqlite:////:memory:"
#     db.initdb()
#
#     yield
#     db.resetdb()


@pytest.fixture(scope="session")
def get_workspace_id(request):
    """
    Эта фикстура нужна для получения id workspace для тестирования компонентов airbyte, например для операторов
    Значение, которое возвращает эта фикстура указывается вручную, в зависимости от устройства, где запускаются тесты
    """
    return "507367ae-c937-404d-a3a4-c62dc597ec17"


@pytest.fixture
def get_airflow_variables(request):
    """
    Фикстура возвращает моковые airflow Variables для теста get_configs
    Чтобы их использовать как Variables.get(...), нужно запатчить в тесте примерно так:

    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default_var=None: get_airflow_variables.get(key),
    )

    Предварительно импортировав модуль "from airflow.models import Variable"
    И передав фикстуру get_airflow_variables как аргумент теста
    """
    variables: dict = {
        "from_datacraft": {"datasources": "Data from Datacraft"},
        "metadata_json": {
            "object_one": "value_one",
            "object_two": "value_two",
        },
        "format_for_etlcraft_datasources": "json",
        "format_for_yaml": "yaml",
        "source_for_etlcraft_templated_file": "templated_file",
        "source_for_etlcraft_other_variable": "other_variable",
        "source_for_etlcraft_datasources": "datacraft_variable",
        "source_for_etlcraft_file": "file",
        "path_for_etlcraft_datasources": "",
        "path_for_etlcraft_templated_file": "some_path/",
        "source_for_etlcraft_metaconfigs": "templated_file",
        "format_for_etlcraft_metaconfigs": "json",
        "path_for_etlcraft_metaconfigs": "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/metaconfigs.json",
        "source_for_etlcraft_base": "templated_file",
        "format_for_etlcraft_base": "json",
        "path_for_etlcraft_base": "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/files_for_tests/base_example_for_tests.json",
        "source_for_etlcraft_base_datasources": "other_variable",
        "format_for_etlcraft_base_datasources": "json",
        "path_for_etlcraft_base_datasources": "base_datasources",
        "base_datasources": {"datasources": "Data from Datacraft 2"},
        "source_for_etlcraft_presets": "templated_file",
        "format_for_etlcraft_presets": "json",
        "path_for_etlcraft_presets": "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/presets.json",
        "source_for_etlcraft_metadata": "templated_file",
        "format_for_etlcraft_metadata": "yaml",
        "path_for_etlcraft_metadata": "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/metadata.yaml",
    }
    return variables


# Три функции ниже позволяют запускать тесты для airbyte компонентов,
# передавая флаг --run-airbyte-tests в команду "pytest etlcraft_tests"
# Иначе, они будут пропускаться
# Чтобы пометить тест как airbyte_test, нужно навесить декоратор @pytest.mark.airbyte_test на него
def pytest_addoption(parser):
    parser.addoption(
        "--run-airbyte-tests",
        action="store_true",
        default=False,
        help="Run airbyte tests",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "airbyte_test: mark special tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-airbyte-tests"):
        # Если параметр передан, запускаем все тесты
        return
    # Иначе исключаем тесты с маркером "airbyte_tests"
    skip_airbyte_test = pytest.mark.skip(
        reason="Need --run-airbyte-tests option to run"
    )
    for item in items:
        if "airbyte_test" in item.keywords:
            item.add_marker(skip_airbyte_test)


@pytest.fixture(scope="session")
def clear_test_objects(get_workspace_id, airbyte_conn):
    """
    Эта фикстура собирает существующие объекты airbyte:
    sourceDefinitions, destinationDefinitions, sources, destinations, connections до тестов,
    чтобы в последствии не удалить нужные

    Затем, вновь собираются все объекты после окончания теста,
    и сравниваются списки до и после, и новые удаляются

    Возможная проблема - в окно выполнения тестов, если будет создан объект,
    например через UI airbyte, то и он удалится
    """
    print("Setup: собираем объекты перед тестами")
    objects_before_test: dict[str, list[str]] = collect_airbyte_objects(
        get_workspace_id, airbyte_conn
    )

    yield

    print("Teardown: собираем объекты после тестов")
    objects_after_test: dict[str, list[str]] = collect_airbyte_objects(
        get_workspace_id, airbyte_conn
    )

    # Формируем листы с объектами, которые нужно удалить после тестов
    source_definitions_to_delete: list[str] = list(
        set(objects_after_test["source_definitions"])
        - set(objects_before_test["source_definitions"])
    )
    destination_definitions_to_delete: list[str] = list(
        set(objects_after_test["destination_definitions"])
        - set(objects_before_test["destination_definitions"])
    )
    sources_to_delete: list[str] = list(
        set(objects_after_test["sources"]) - set(objects_before_test["sources"])
    )
    destinations_to_delete: list[str] = list(
        set(objects_after_test["destinations"])
        - set(objects_before_test["destinations"])
    )
    connections_to_delete: list[str] = list(
        set(objects_after_test["connections"]) - set(objects_before_test["connections"])
    )

    # Удаление объектов
    print("Cleaning: удаляем новые объекты")
    source_definitions_deleter(source_definitions_to_delete, airbyte_conn)
    destination_definitions_deleter(destination_definitions_to_delete, airbyte_conn)
    sources_deleter(sources_to_delete, airbyte_conn)
    destinations_deleter(destinations_to_delete, airbyte_conn)
    connections_deleter(connections_to_delete, airbyte_conn)
