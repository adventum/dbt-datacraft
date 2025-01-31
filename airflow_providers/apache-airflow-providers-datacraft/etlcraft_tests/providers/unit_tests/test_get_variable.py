from airflow.models import Variable
from pytest_mock import MockerFixture


variables_for_mock = {"from_datacraft": {"leo": "pard"}, "from_airbyte": "/some/path/"}


def test_run():
    assert 1 != 0


def test_get_variables_from_airflow_components(mocker: MockerFixture):
    mocker.patch.object(
        Variable,
        "get",
        side_effect=lambda key, default=None: variables_for_mock.get(key, default),
    )

    variable: any = Variable.get("from_datacraft", None)
    assert variable == {"leo": "pard"}

    variable: any = Variable.get("from_airbyte", None)
    assert variable == "/some/path/"

    variable: any = Variable.get("non_existent_key", "default_value")
    assert variable == "default_value"


def test_get_variables_from_fixture(get_airflow_variables: dict[str, any]):
    variables: dict[str, any] = get_airflow_variables

    assert variables.get("from_datacraft") == {"datasources": "Data from Datacraft"}
    assert variables.get("format_for_yaml") == "yaml"
    assert not variables.get("format_for_config_hypertext")
