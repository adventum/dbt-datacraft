import random

from airflow.decorators import dag, task
from datetime import datetime

from airflow.providers.datacraft.airbyte.models import (
    BaseDestinationDefinition,
    BaseSourceDefinition,
    SourceDefinitionSpec,
    DestinationDefinitionSpec
)
from airflow.providers.datacraft.airbyte.operators import (
    AirbyteCreateDestinationDefinitionsOperator,
    AirbyteCreateSourceDefinitionsOperator,
    AirbyteUpdateDestinationDefinitionsOperator,
    AirbyteUpdateSourceDefinitionsOperator,
    AirbyteListWorkspacesOperator,
    AirbyteListSourceDefinitionsOperator,
    AirbyteListDestinationDefinitionsOperator,
)

# Perhaps the import could be done differently so that each library remains independent?
from airflow.providers.datacraft.dags.configs.get_configs import get_configs

from typing import Union, List, Dict


def filter_definitions_for_update_and_creation(
    definitions_configuration: List[Dict],
    existing_definitions: List[Union['SourceDefinitionSpec', 'DestinationDefinitionSpec']]
) -> Dict[str, List[Dict]]:

    definitions_to_create: list = []
    definitions_to_update: list = []

    for configuration in definitions_configuration:
        matched: bool = False  # Флаг для проверки наличия совпадений

        # Compare with each element from existing_definitions
        for existing_definition in existing_definitions:
            if (
                configuration.get("name") == existing_definition.name
                and configuration.get("dockerRepository") == existing_definition.dockerRepository
            ):
                # If name and dockerRepository match, check dockerImageTag
                if configuration.get("dockerImageTag") == existing_definition.dockerImageTag:
                    # If dockerImageTag also matches, skip the element
                    matched = True
                    break
                else:
                    # If dockerImageTag is different, add to the update list
                    if isinstance(existing_definition, SourceDefinitionSpec):
                        configuration["sourceDefinitionId"] = existing_definition.sourceDefinitionId
                    elif isinstance(existing_definition, DestinationDefinitionSpec):
                        configuration["destinationDefinitionId"] = (
                            existing_definition.destinationDefinitionId
                        )
                    else:
                        raise Exception("Existing definitions don't match with Pydantic models")

                    definitions_to_update.append(configuration)
                    matched = True
                    break

        # If there were no matches, add to the list for creation
        if not matched:
            definitions_to_create.append(configuration)

    return {
        "definitions_to_create": definitions_to_create,
        "definitions_to_update": definitions_to_update
    }


params = {
    "namespace": "place_for_namespace",
}


@dag(
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="create_source_destination_definitions_dag",
    params=params,
)
def create_source_destination_definitions_dag():

    @task
    def get_configurations(**kwargs) -> dict[str, object]:
        dag_params: dict = (
            kwargs.get('dag_run').conf
            if kwargs.get('dag_run')
            else kwargs['dag'].params
        )

        get_configurations_task_result: dict[str, object] = get_configs(
            namespace=dag_params.get("namespace", "etlcraft"),
            config_names=dag_params.get("config_names", ["connectors",])
        )
        return get_configurations_task_result

    @task
    def prepare(base_configuration: dict, **kwargs) -> dict[str, list[dict]]:
        """
        Prepare gets workspace_id, airbyte_conn_id from the received configs from get_configuration
        Then it receives, after removing duplicates, source_definitions and destination_definitions
        for subsequent transfer to tasks that will create or update these objects
        """

        workspace_id: str = base_configuration.get(f"base", {}).get("workspace_id")
        airbyte_conn_id: str = (
            base_configuration.get(f"base", {}).get("airbyte_conn_id")
            or "airbyte_default"
        )

        if not workspace_id:
            # Get workspace_id from airbyte if it was not defined in the base config
            airbyte_list_workspaces_operator = AirbyteListWorkspacesOperator(
                task_id="get_list_airbyte_workspaces",
                airbyte_conn_id=airbyte_conn_id,
            )
            workspaces: list = airbyte_list_workspaces_operator.execute(kwargs)

            if len(workspaces) == 0:
                raise Exception(
                    "The workspaces list is empty. "
                    "Please provide at least one workspace in Airbyte."
                )

            elif len(workspaces) > 1:
                raise Exception(
                    "workspace_id is not provided in the base config, "
                    "and the workspaces list has more than one workspace. "
                    "Please add workspace_id to the base config"
                )

            else:
                workspace_id = workspaces[0].workspaceId

        # We pass variables to xcom to use them in other tasks
        kwargs['ti'].xcom_push(key='workspace_id', value=workspace_id)
        kwargs['ti'].xcom_push(key='airbyte_conn_id', value=airbyte_conn_id)

        airbyte_list_source_definitions_operator = AirbyteListSourceDefinitionsOperator(
            task_id="get_list_airbyte_source_definitions",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id
        )
        airbyte_list_destination_definitions_operator = AirbyteListDestinationDefinitionsOperator(
            task_id="get_list_airbyte_destination_definitions",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id
        )

        source_definitions_configuration: list[dict] = (
            base_configuration.get("connectors").get("source_definitions")
        )
        destination_definitions_configuration: list[dict] = (
            base_configuration.get("connectors").get("destination_definitions")
        )

        existing_source_definitions: list[BaseSourceDefinition] = (
            airbyte_list_source_definitions_operator.execute(context=kwargs)
        )
        existing_destination_definitions: list[BaseDestinationDefinition] = (
            airbyte_list_destination_definitions_operator.execute(context=kwargs)
        )

        # Filtering is needed to avoid creating duplicate definitions.
        # Here we exclude from the list the configurations that came with get_configurations,
        # which are already in airbyte. We also get a list of those that need to be updated
        filtered_source_definitions: dict[str, list[dict]] = (
            filter_definitions_for_update_and_creation(
                source_definitions_configuration, existing_source_definitions
            )
        )
        filtered_destination_definitions: dict[str, list[dict]] = (
            filter_definitions_for_update_and_creation(
                destination_definitions_configuration, existing_destination_definitions
            )
        )

        object_definitions_map = {
            "source_definitions_to_create": filtered_source_definitions["definitions_to_create"],
            "destination_definitions_to_create": filtered_destination_definitions["definitions_to_create"],
            "source_definitions_to_update": filtered_source_definitions["definitions_to_update"],
            "destination_definitions_to_update": filtered_destination_definitions["definitions_to_update"],
        }

        return object_definitions_map

    @task
    def get_source_definitions_to_create(prepared_definitions: dict) -> list[dict]:
        """Get prepare data for create_source_definition"""
        return prepared_definitions.get("source_definitions_to_create", [])

    @task
    def get_destination_definitions_to_create(prepared_definitions: dict) -> list[dict]:
        """Get prepare data for create_destination_definition"""
        return prepared_definitions.get("destination_definitions_to_create", [])

    @task
    def get_source_definitions_to_update(prepared_definitions: dict) -> list[dict]:
        """Get prepare data for update_source_definition"""
        return prepared_definitions.get("source_definitions_to_update", [])

    @task
    def get_destination_definitions_to_update(prepared_definitions: dict) -> list[dict]:
        """Get prepare data for update_destination_definition"""
        return prepared_definitions.get("destination_definitions_to_update", [])

    @task
    def create_source_definitions(
        source_definition_configuration: BaseSourceDefinition | None = None,
        **kwargs
    ) -> SourceDefinitionSpec | str:
        if source_definition_configuration:
            # We get variables from prepare that we use in the operator
            workspace_id: str = kwargs['ti'].xcom_pull(key='workspace_id', task_ids='prepare')
            airbyte_conn_id: str = kwargs['ti'].xcom_pull(key='airbyte_conn_id', task_ids='prepare')
            random_int: int = random.randint(1000, 9999)

            create_source_definitions_task = AirbyteCreateSourceDefinitionsOperator(
                task_id=(
                    f"create_source_definition_"
                    f"{source_definition_configuration['name']}_{random_int}"
                ),
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                source_definition_configuration=source_definition_configuration
            )

            create_source_definitions_task_result = (
                create_source_definitions_task.execute(context=kwargs)
            )
            return create_source_definitions_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def create_destination_definitions(
        destination_definition_configuration: BaseDestinationDefinition | None = None,
        **kwargs
    ) -> DestinationDefinitionSpec | str:
        if destination_definition_configuration:
            # We get variables from prepare that we use in the operator
            workspace_id: str = kwargs['ti'].xcom_pull(key='workspace_id', task_ids='prepare')
            airbyte_conn_id: str = kwargs['ti'].xcom_pull(key='airbyte_conn_id', task_ids='prepare')
            random_int: int = random.randint(1000, 9999)

            create_destination_definitions_task = AirbyteCreateDestinationDefinitionsOperator(
                task_id=(
                    f"create_destination_definition_"
                    f"{destination_definition_configuration['name']}_{random_int}"
                ),
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                destination_definition_configuration=destination_definition_configuration
            )

            create_destination_definitions_task_result = (
                create_destination_definitions_task.execute(context=kwargs)
            )
            return create_destination_definitions_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def update_source_definitions(
        source_definition_configuration: SourceDefinitionSpec | None = None,
        **kwargs
    ) -> SourceDefinitionSpec | str:
        if source_definition_configuration:
            # We get variables from prepare that we use in the operator
            workspace_id: str = kwargs['ti'].xcom_pull(key='workspace_id', task_ids='prepare')
            airbyte_conn_id: str = kwargs['ti'].xcom_pull(key='airbyte_conn_id', task_ids='prepare')

            source_definition_id: str = source_definition_configuration.pop("sourceDefinitionId")
            random_int: int = random.randint(1000, 9999)

            update_source_definitions_task = AirbyteUpdateSourceDefinitionsOperator(
                task_id=(
                    f"update_source_definition_"
                    f"{source_definition_configuration['name']}_{random_int}"
                ),
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                source_definition_id=source_definition_id,
                source_definition_configuration=source_definition_configuration
            )

            update_source_definitions_task_result = (
                update_source_definitions_task.execute(context=kwargs)
            )
            return update_source_definitions_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def update_destination_definitions(
        destination_definition_configuration: DestinationDefinitionSpec | None = None,
        **kwargs
    ) -> DestinationDefinitionSpec | str:
        if destination_definition_configuration:
            # We get variables from prepare that we use in the operator
            workspace_id: str = kwargs['ti'].xcom_pull(key='workspace_id', task_ids='prepare')
            airbyte_conn_id: str = kwargs['ti'].xcom_pull(key='airbyte_conn_id', task_ids='prepare')

            destination_definition_id: str = destination_definition_configuration.pop(
                "destinationDefinitionId"
            )
            random_int: int = random.randint(1000, 9999)

            update_destination_definitions_task = AirbyteUpdateDestinationDefinitionsOperator(
                task_id=(
                    f"update_destination_definition_"
                    f"{destination_definition_configuration['name']}_{random_int}"
                ),
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                destination_definition_id=destination_definition_id,
                destination_definition_configuration=destination_definition_configuration
            )

            update_destination_definitions_task_result = (
                update_destination_definitions_task.execute(context=kwargs)
            )
            return update_destination_definitions_task_result
        else:
            return "No configuration provided. Task skipped"

    """
    Taskflow
    1. Get configs from get_configs (base, metaconfigs, connectors)
    2. Preparing data and filtering out duplicate objects airbyte definitions
    3. We distribute the prepared data into auxiliary tasks,
    each of which contains a list of objects
    4. Create new or update existing definitions
    """
    get_configurations_task = get_configurations()
    prepare_task = prepare(base_configuration=get_configurations_task)

    get_source_definitions_to_create_task = get_source_definitions_to_create(prepare_task)
    get_destination_definitions_to_create_task = get_destination_definitions_to_create(prepare_task)
    get_source_definitions_to_update_task = get_source_definitions_to_update(prepare_task)
    get_destination_definitions_to_update_task = get_destination_definitions_to_update(prepare_task)

    create_source_definitions_tasks = create_source_definitions.expand(
        source_definition_configuration=get_source_definitions_to_create_task
    )

    create_destination_definitions_tasks = create_destination_definitions.expand(
        destination_definition_configuration=get_destination_definitions_to_create_task
    )

    update_source_definitions_tasks = update_source_definitions.expand(
        source_definition_configuration=get_source_definitions_to_update_task
    )

    update_destination_definitions_tasks = update_destination_definitions.expand(
        destination_definition_configuration=get_destination_definitions_to_update_task
    )

    # Prepare data for create/update definitions
    get_configurations_task >> prepare_task
    prepare_task >> get_source_definitions_to_create_task
    prepare_task >> get_destination_definitions_to_create_task
    prepare_task >> get_source_definitions_to_update_task
    prepare_task >> get_destination_definitions_to_update_task

    # Create/Update definitions
    get_source_definitions_to_create_task >> create_source_definitions_tasks
    get_destination_definitions_to_create_task >> create_destination_definitions_tasks
    get_source_definitions_to_update_task >> update_source_definitions_tasks
    get_destination_definitions_to_update_task >> update_destination_definitions_tasks


dag_instance = create_source_destination_definitions_dag()
