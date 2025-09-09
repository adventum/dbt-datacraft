import shutil
from pathlib import Path


from datetime import datetime

from airflow.models import Variable
from airflow.models import DAG

from airflow.operators.python_operator import PythonOperator

from airflow.providers.datacraft.dags.exceptions import DatacraftConfigError


"""В панеле администратора должна быть переменная dbt_project_directory, в которой будет путь до 
   проекта dbt, она используется для создания/проверки и парсинга папки templated_files"""

default_value_mapping = {
    "connectors": {
        "type": "templated_file",
        "format": "yaml",
    },
    "presets": {
        "type": "templated_file",
        "format": "json",
    },
    "datasources": {
        "type": "datacraft_variable",
        "format": "json",
    },
    "metadata": {
        "type": "templated_file",
        "format": "yaml",
    },
    "events": {
        "type": "templated_file",
        "format": "yaml",
    },
    "attributions": {
        "type": "datacraft_variable",
        "format": "json",
    },
    "base": {
        "type": "templated_file",
        "format": "yaml",
    },
}


def ensure_templated_configs_directory(base_path: str):
    # Определение пути к папке templated_configs
    templated_configs_path = Path(base_path) / "templated_configs"

    # Проверка существования папки
    if not templated_configs_path.exists():
        templated_configs_path.mkdir(parents=True, exist_ok=True)


def check_file(
    config_name: str, file_format: str, base_path: str = "", metaconfig_path: str = ""
):
    # Определение всех возможных расширений для проверки
    extensions_to_check = ["json"] if file_format == "json" else ["yml", "yaml"]

    for ext in extensions_to_check:
        manual_file_path = f"templated_files/{config_name}_manual.{ext}"
        manual_file = Path(manual_file_path)

        if manual_file.is_file():
            return manual_file_path, True

    else:
        # Пример templated_file_path если есть base_path, но нет metaconfig_path
        # /path_to_datacraft/templated_configs/connectors.yml
        templated_file_path = (
            f"{base_path}{'/' if base_path else ''}"
            f"templated_files/{config_name}"
            f"{'/' if metaconfig_path else ''}{metaconfig_path}.{file_format}"
        )
        return templated_file_path, False


def prepare(config_names: list[str], namespace: str) -> list[dict[any:any]]:
    configs_map: list = []

    dbt_directory_variable = Variable.get("dbt_project_directory", "")
    ensure_templated_configs_directory(dbt_directory_variable)

    for config_name in config_names:
        # Определение формата, пути, источника конфига
        source_key = f"source_for_config_{namespace}_{config_name}"
        format_key = f"format_for_config_{namespace}_{config_name}"
        path_key = f"path_for_config_{namespace}_{config_name}"

        source: str = Variable.get(
            source_key,
            default_var=default_value_mapping.get(config_name).get("type")
            if config_name in default_value_mapping.keys()
            else {},
        )
        file_format: str = Variable.get(
            format_key,
            default_var=default_value_mapping.get(config_name).get("format")
            if config_name in default_value_mapping.keys()
            else {},
        )
        path: str = Variable.get(path_key, default_var="")

        # Формирование конфига. Он наполняется во время выполнения кода ниже
        config_map: dict[str:any] = {
            "name": config_name,
        }

        if source == "other_variable":
            source = "airflow_variable"
            config_map["source"] = source
            config_map["variable_name"] = path
            config_map["format"] = file_format

        elif source == "datacraft_variable":
            raise DatacraftConfigError(
                "You can not use datacraft_variable. It is not supported"
            )

        elif source == "templated_file":
            file, symlink = check_file(config_name, file_format, dbt_directory_variable)

            if file and symlink:
                config_map["source"] = "file"
                config_map["file_path"] = file
                config_map["format"] = file_format
                config_map["symlink"] = True

            elif file and not symlink:
                config_map["source"] = "file"
                config_map["file_path"] = file
                config_map["format"] = file_format
                config_map["symlink"] = False
        else:
            config_map["source"] = "file"
            config_map["file_path"] = path
            config_map["format"] = file_format

        configs_map.append(config_map)
    return configs_map


def process_config_file(config: dict, base_path: str, macros_path: str):
    config_name = config["name"]
    file_format = config["format"]
    source = config["source"]
    symlink = config.get("symlink", False)

    # Путь к файлу для выполнения действий
    path = f"{base_path}{'/' if base_path else ''}templated_files/{config_name}.{file_format}"
    target_file_path = Path(path)

    # Удаление существующего файла
    if target_file_path.exists():
        target_file_path.unlink()

    # Выполнение действий в зависимости от source
    if source == "airflow_variable":
        value = Variable.get(config["variable_name"])
        with open(target_file_path, "w") as file:
            file.write(value)

    elif source == "file":
        file_path = Path(config["file_path"])
        if symlink:
            target_file_path.symlink_to(file_path)
        else:
            shutil.copy(file_path, target_file_path)

    # Чтение содержимого целевого файла
    with open(target_file_path, "r") as target_file:
        file_content = target_file.read()

    # Создание SQL макроса
    macro_file_path = Path(macros_path) / f"{config_name}.sql"
    with open(macro_file_path, "w") as file:
        file.write(f"""
            {{% macro {config_name}_data() %}}
            {{
            {file_content}
            }}
            {{% endmacro %}}

            {{% macro {config_name}() %}}
            from{file_format}({config_name}_data())
            {{% endmacro %}}
        """)


# Функция для создания задач
def generate_tasks(config_names: list[str], namespace: str, **kwargs):
    # Вызов функции prepare
    configs = prepare(config_names=config_names, namespace=namespace)

    dbt_directory_variable = Variable.get("dbt_project_directory", "")
    macros_path = Path(dbt_directory_variable) / "macros/templated_files"

    ensure_templated_configs_directory(dbt_directory_variable)
    if not macros_path.exists():
        macros_path.mkdir(parents=True, exist_ok=True)

    # Создание задач на основе конфигураций
    for config in configs:
        task = PythonOperator(
            task_id=f"process_{config['name']}",
            python_callable=process_config_file,
            op_kwargs={
                "config": config,
                "base_path": dbt_directory_variable,
                "macros_path": macros_path,
            },
        )
        task.execute(context=kwargs)


# Определение DAG
params = {
    "config_names": ["connectors", "other_variable", "presets"],
    "namespace": "datacraft",
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

with DAG(
    "template_configs_dag",
    default_args=default_args,
    description="DAG для обработки конфигурационных файлов и создания SQL макросов",
    schedule_interval=None,
    start_date=datetime(2024, 9, 4),
    catchup=False,
    params=params,
) as dag:
    generate_tasks_operator = PythonOperator(
        task_id="generate_tasks",
        python_callable=generate_tasks,
        op_kwargs=params,
        provide_context=True,
    )
