import pathlib

import pytest

from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.datacraft.dags.configs.get_configs import (
    get_configs,
    get_default_path,
    get_metaconfig,
    get_single_config,
    parse_by_format,
    Metaconfig,
    Source,
    Format,
)

from pendulum import datetime
from airflow.operators.python import PythonOperator
from pytest_mock import MockerFixture


def test_run_get_configs(mocker: MockerFixture, get_airflow_variables: dict[str, any]):
    """Тест на полный прогон функции"""

    # Через мокер подмениваем переменную Variable от airflow
    # на словарь с тестовыми переменными, которые находятся в conftest.py
    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default_var=None: get_airflow_variables.get(key),
    )

    configs: dict[str, any] = get_configs(
        namespace="etlcraft", config_names=["datasources", "presets", "metadata"]
    )

    assert "base" in configs.keys()
    assert "datasources" in configs.keys()
    assert "presets" in configs.keys()
    assert "metadata" in configs.keys()


def test_run_get_configs_in_dag(
    mocker: MockerFixture, get_airflow_variables: dict[str, any]
):
    """Тест на полный прогон функции внутри дага"""

    # Через мокер подмениваем переменную Variable от airflow
    # на словарь с тестовыми переменными, которые находятся в conftest.py
    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default_var=None: get_airflow_variables.get(key),
    )

    @dag(
        start_date=datetime(2024, 1, 1),
        catchup=False,
    )
    def test_dag_get_configs():
        configs: dict[str, any] | PythonOperator = PythonOperator(
            task_id="test_get_configs",
            python_callable=get_configs,
            op_kwargs={
                "namespace": "etlcraft",
                "config_names": ["datasources", "presets", "metadata"],
            },
        )
        return configs

    dag_object = test_dag_get_configs()
    dag_run = dag_object.test()

    # Получаем результат дага, и проверяем через ассерты спаршенные конфиги
    task_id = "test_get_configs"
    result = dag_run.get_task_instance(task_id).xcom_pull(task_ids=task_id)

    assert "base" in result.keys()
    assert "datasources" in result.keys()
    assert "presets" in result.keys()
    assert "metadata" in result.keys()


# @pytest.mark.parametrize("config_name", ["metaconfigs", "not_metaconfigs"])
def test_get_defaults_metaconfig(
    mocker: MockerFixture,
    get_airflow_variables: dict[str, any],
    config_name: str = "metaconfigs",
):
    """Тест на получение метаконфига как template_file,
    из пакета apache-airflow-providers-datacraft-defaults"""

    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default_var=None: get_airflow_variables.get(key),
    )

    metaconfig: Metaconfig = get_metaconfig(
        namespace="etlcraft", config_name=config_name, defaults=None
    )

    # Изначально, значения format, source, path берутся из conftest.py,
    # конкретно из get_airflow_variables
    assert metaconfig is not None
    assert metaconfig.format == Format.json
    assert metaconfig.source == Source.templated_file
    assert (
        metaconfig.path
        == "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/metaconfigs.json"
    )

    defaults_metaconfigs: dict = get_single_config(
        config_name="metaconfigs", metaconfig=metaconfig
    )

    required_keys = [
        "connectors",
        "presets",
        "datasources",
        "metadata",
        "events",
        "attributions",
        "base",
    ]

    assert defaults_metaconfigs is not None
    assert all(key in defaults_metaconfigs.keys() for key in required_keys)
    assert "birds" not in defaults_metaconfigs.keys()


def test_get_base_config(
    mocker: MockerFixture,
    get_airflow_variables: dict[str, any],
    namespace: str = "etlcraft",
):
    """Тест на получение конфига base"""

    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default_var=None: get_airflow_variables.get(key),
    )

    default_metaconfigs: dict | None = get_single_config(
        "metaconfigs", get_metaconfig(namespace, "metaconfigs", defaults=None)
    )

    base: dict = get_single_config(
        "base", get_metaconfig(namespace, "base", default_metaconfigs)
    )

    assert default_metaconfigs is not None
    assert base is not None
    assert "datasources" in base.keys()


@pytest.mark.parametrize(
    "source", ["datacraft_variable", "other_variable", "templated_file", "file"]
)
def test_get_default_path(source: str):
    """Тест на проверку получения пути по умолчанию"""

    source = Source[source]
    default_path = get_default_path(source)

    if source == "datacraft_variable":
        assert default_path == ""

    if source == "other_variable":
        assert default_path == source

    if source == "templated_file":
        assert default_path == ""

    if source == "file":
        assert default_path == f"configs/{source}"


@pytest.mark.parametrize(
    "source", ["datacraft_variable", "other_variable", "templated_file", "file"]
)
@pytest.mark.parametrize("source_format", ["json", "yaml"])
def test_parse_by_format(
    source: str,
    source_format: str,
    mocker: MockerFixture,
    get_airflow_variables: dict[str, any],
):
    """Тест на проверку получения данных по формату"""

    source = Source[source]
    source_format = Format[source_format]

    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default_var=None: get_airflow_variables.get(key),
    )

    if source == "datacraft_variable":
        result = parse_by_format(Variable.get("from_datacraft"), source_format)
        assert result
        assert "datasources" in result.keys()

    if source == "other_variable":
        result = parse_by_format(Variable.get("metadata_json"), source_format)
        assert result
        assert "object_one" in result.keys()

    if source == "templated_file" and source_format == "json":
        path = "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/presets.json"
        filepath = pathlib.Path(path)
        result = parse_by_format(text=filepath.read_text(), format=source_format)
        assert result
        assert "source_presets" in result.keys()

    if source == "file" and source_format == "yaml":
        path = "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/metadata.yaml"
        filepath = pathlib.Path(path)
        result = parse_by_format(text=filepath.read_text(), format=source_format)
        assert result
        assert "entities" in result.keys()
