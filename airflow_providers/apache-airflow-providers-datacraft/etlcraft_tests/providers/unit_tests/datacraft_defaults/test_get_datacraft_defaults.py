from airflow.providers.datacraft.defaults.get_datacraft_defaults import (
    get_datacraft_defaults,
)

from pathlib import Path
from pytest_mock import MockerFixture


def test_get_datacraft_defaults_jinja_file(mocker: MockerFixture):
    """
    Тест на нахождения файла jinja, передача в него переменных,
    и получение отрендеренного результата

    При этом, существует только файл presets_jinja_example_for_tests формата .j2
    JSON, YAML файлов с таким же названием не существует
    """

    config_name: str = "presets_jinja_example_for_tests"
    file_format: str = "json"
    templated_variables: dict = {
        "example_value_one": 123,
        "example_value_two": [1, 2, 3],
        "example_value_three": str("apple"),
    }

    # Здесь идет мок пути, чтобы получить файл для теста из папки files_for_tests,
    # чтобы не засорять основную директорию с defaults
    mock_path = mocker.patch("pathlib.Path")
    mock_path.return_value.parent = Path(
        "../apache-airflow-providers-datacraft-defaults/airflow/providers/datacraft/defaults/files_for_tests"
    )

    rendered_jinja_file = get_datacraft_defaults(
        config_name=config_name,
        format=file_format,
        template_variables=templated_variables,
    )

    assert rendered_jinja_file
    assert (
        rendered_jinja_file["example_key_one"]
        == templated_variables["example_value_one"]
    )
    assert (
        rendered_jinja_file["example_key_two"]
        == templated_variables["example_value_two"]
    )
    assert (
        rendered_jinja_file["example_key_three"]
        == templated_variables["example_value_three"]
    )


def test_get_datacraft_defaults_preset_json():
    """
    В этом тесте получаем presets.json, обходя блок кода с поиском jinja файла
    """

    config_name: str = "presets"
    file_format: str = "json"

    presets = get_datacraft_defaults(
        config_name=config_name, format=file_format, template_variables={}
    )

    assert presets
    assert "source_presets" in presets.keys()
    assert "not_a_source_presets_" not in presets.keys()
