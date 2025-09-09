import pytest


@pytest.fixture
def get_airflow_variables(request):
    variables: dict = {
        "from_datacraft": "Data from Datacraft",
        "metadata_json": {
            "object_one": "value_one",
            "object_two": "value_two",
        },
        "format_for_config_json": "json",
        "format_for_config_yaml": "yaml",
        "source_for_config_templated_file": "templated_file",
        "source_for_config_other_variable": "other_variable",
        "source_for_config_datacraft_variable": "datacraft_variable",
        "source_for_config_file": "file",
        "path_for_config": "some_path/",
    }
    return variables
