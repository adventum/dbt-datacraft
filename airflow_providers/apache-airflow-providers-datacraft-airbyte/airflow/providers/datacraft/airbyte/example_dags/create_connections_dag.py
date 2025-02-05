import random
import copy

from deepdiff import DeepDiff

from airflow.decorators import dag, task
from airflow.providers.datacraft.airbyte.models import (
    SourceSpec,
    ConnectionSpec,
    SourceDefinitionSpec,
    DestinationDefinitionSpec,
    DestinationSpec,
    ClickhouseDestinationConfiguration,
)
from airflow.providers.datacraft.airbyte.operators import (
    AirbyteCreateConnectionOperator,
    AirbyteCreateSourceOperator,
    AirbyteCreateDestinationOperator,
    AirbyteListSourcesOperator,
    AirbyteListDestinationDefinitionsOperator,
    AirbyteListDestinationsOperator,
    AirbyteListSourceDefinitionsOperator,
    AirbyteListWorkspacesOperator,
    AirbyteListConnectionsOperator,
    AirbyteUpdateConnectionOperator,
    AirbyteUpdateDestinationOperator,
    AirbyteUpdateSourceOperator,
)
from airflow.models import Variable
from airflow.providers.datacraft.dags.configs.get_configs import get_configs

from datetime import datetime

from typing import Any, Union

"""
platform_account_id_field_mapping: dict[str, str] = {
    "yd": "client_login",  # yd - YandexDirect
    "ym": "counter_id",  # ym - YandexMetrica
    "appmetrica": "application_id",
    "adjust": "app_id",
    "appsflyer": "app_id",
    "calltouch": "app_id",
    "google_analytics": "view_id",
    "huawei_ads": "client_name_constraint",
    "mt": "product_name",  # mt - MyTarget
    "smart_callback": "api_token",
    "twitter": "twitter_account_id",
    "vkads": "client_name",
    "ydisk": "path_placeholder",  # ydisk - YandexDisk
    "ya_market": "account_id",
}
"""


def validate_and_transform_datasources(
    datasources: dict[str, dict[str, Any]]
) -> list[dict[str, Any]]:
    """
    We cut off datasource objects if at least one key value is None.
    This is necessary to avoid receiving errors and create correct sources and connections.
    We also convert the dictionary into a list of objects
    """
    validated_datasources: list[dict[str, Any]] = []
    for datasource_id, datasource_configuration in datasources.items():
        if all(value not in [None, "null"] for value in datasource_configuration.values()):
            validated_datasources.append(datasource_configuration)
        else:
            print(
                f"Datasource with id - {datasource_id} is not valid, "
                f"because some values in datasource configuration is missing. "
                f"This one will be skipped"
            )
    return validated_datasources


def pydantic_models_to_dict(
    models: list[
        Union[
            SourceDefinitionSpec,
            SourceSpec,
            DestinationDefinitionSpec,
            DestinationSpec,
            ConnectionSpec,
        ]
    ],
) -> list[dict]:
    return [model.model_dump() for model in models]


def replace_presets_fields(
    base: dict[str, Any],
    datasource: dict[str, Any],
    raw_source_configuration: dict[str, Any],
    connectors: dict[str, list[dict]],
) -> dict:
    """
    The function replaces the credentials_craft_host, credentials_craft_token,
    credentials_craft_token_id, account_id(e.g app_id) fields in the presets config,
    taking them from the base and datasource configs
    """
    credentials_craft_host: str = base["credentials_craft_host"]
    credentials_craft_token: str = base["credentials_craft_token"]

    source_type: str = datasource["source_type"]  # E.g appmetrica | ydisk | ym | yd
    account_id: str = datasource["account_id"]
    credentials_craft_token_id: str | int = datasource["credentials_craft_token_id"]

    account_id_field: str | None = None
    for source_definition in connectors["source_definitions"]:
        if source_definition["slug"] == source_type:
            account_id_field = source_definition["account_id_field"]

    # Replace credentials and account_id in preset config
    raw_source_configuration["credentials"]["credentials_craft_host"] = (
        credentials_craft_host
    )
    raw_source_configuration["credentials"]["credentials_craft_token"] = (
        credentials_craft_token
    )
    raw_source_configuration["credentials"]["credentials_craft_token_id"] = (
        credentials_craft_token_id
    )
    raw_source_configuration[account_id_field] = account_id

    return raw_source_configuration


def fill_destination_configuration_with_secrets(
    destination_configuration: dict,
) -> dict:
    host: str = Variable.get("clickhouse_default_host")
    database: str = Variable.get("clickhouse_default_database")
    password: str = Variable.get("clickhouse_default_password")
    username: str = Variable.get("clickhouse_default_user")

    destination_configuration["host"] = host
    destination_configuration["database"] = database
    destination_configuration["password"] = password
    destination_configuration["username"] = username

    return destination_configuration


def clean_raw_connection_config(
    raw_connection_configuration: dict,
) -> dict:
    """
    We remove unnecessary fields from the existing connection configuration in airbyte
    and the one we are going to create or update

    This is needed to correctly compare two connections using DeepDiff in the following functions
    """

    fields_to_clean: list[str] = [
        "source",
        "destination",
        "createdAt",
        "connectionId",
        "name",
        "schemaChange",
        "catalogId",
        "sourceCatalogId",
        "isSyncing",
        "connectionId",
        "sourceId",
        "destinationId",
        "operations",
        "entities",
        "pipeline"
        "links",
    ]
    cleaned_connection_configuration = {
        key: value
        for key, value in raw_connection_configuration.items()
        if key not in fields_to_clean
    }

    for stream in cleaned_connection_configuration.get("syncCatalog").get("streams"):
        stream.pop("pipeline", None)

    return cleaned_connection_configuration


def filter_sources_for_update_and_creation(
    base: dict[str, Any],
    datasources: list[dict[str, Any]],
    presets: dict[str, Any],
    connectors: dict[str, list[dict]],
    existing_sources: list[dict],
    existing_source_definitions: list[dict],
) -> dict[str, list[dict]]:
    """
    Here we filter which sources we will create, and which ones we will update, or do nothing.
    As input, we receive configs and existing source and source_definition

    Using DeepDiff, we compare the source configuration that we have in presets.json
    and the one that already exists (if not, we create it)

    We return a dictionary with two keys: sources_to_create, sources_to_update
    """

    sources_to_create: list[dict] = []
    sources_to_update: list[dict] = []

    for datasource in datasources:
        # Replace sensitive data like account_id, credentials in source configuration
        preset_name = datasource["preset"]
        source_type = datasource["source_type"]
        account_id = datasource["account_id"]
        source_name = f"{source_type}_{preset_name}_{account_id}"

        try:
            raw_source_configuration = presets["connection_presets"][preset_name][
                "source"
            ]["connectionConfiguration"]
        except KeyError as ex:
            raise KeyError(f"{ex} | Can not find preset {preset_name} in presets.json")

        replaced_source_configuration: dict = replace_presets_fields(
            base, datasource, raw_source_configuration, connectors
        )

        # Checking what sources we can create
        if not any(
            existing_source["name"] == source_name
            for existing_source in existing_sources
        ):
            # If a source with the same name does not exist, create it
            source_definition_id: str | None = None
            for existing_source_definition in existing_source_definitions:
                if existing_source_definition["name"] == source_type:
                    source_definition_id = existing_source_definition[
                        "sourceDefinitionId"
                    ]
                    break

            if not source_definition_id:
                raise Exception(
                    f"Can not find sourceDefinition which named | {source_type} |. "
                    f"Please check the slugs in the connectors.yaml"
                )

            # These fields will be cut in the create_sources task
            replaced_source_configuration["sourceDefinitionId"] = source_definition_id
            replaced_source_configuration["name_for_source_creation"] = source_name
            sources_to_create.append(replaced_source_configuration)
            continue

        # Checking which sources we can update
        for existing_source in existing_sources:
            if existing_source["name"] == source_name:
                existing_source_configuration = existing_source[
                    "connectionConfiguration"
                ]
                source_configuration_delta = DeepDiff(
                    replaced_source_configuration,
                    existing_source_configuration,
                    exclude_paths=["root['credentials']['credentials_craft_token']"],
                )

                if "values_changed" in source_configuration_delta:
                    # These fields will be cut in the create_sources task
                    replaced_source_configuration["sourceId"] = existing_source[
                        "sourceId"
                    ]
                    replaced_source_configuration["name_for_source_creation"] = (
                        source_name
                    )
                    sources_to_update.append(replaced_source_configuration)

                    # Logging changed fields
                    changed_fields = [
                        str(path)
                        for path in source_configuration_delta["values_changed"].keys()
                    ]
                    print(
                        f"The source named {source_name} has changed. "
                        f"The following are the fields that have been changed "
                        f"| {changed_fields} |"
                    )

    return {
        "sources_to_create": sources_to_create,
        "sources_to_update": sources_to_update,
    }


def filter_destinations_for_update_and_creation(
    base: dict[str, Any],
    connectors: dict[str, list[dict]],
    existing_destinations: list[dict],
    existing_destinations_definitions: list[dict],
) -> dict[str, list[dict]]:
    """
    Here we define whether we need to update an existing destination or create a new one.
    We also populate the destination configuration with sensitive data.
    """
    clickhouse_connectors_destination: dict = connectors["destination_definitions"][0]
    clickhouse_configuration: dict = base["clickhouse_default"][
        "connectionConfiguration"
    ]
    clickhouse_configuration: dict = fill_destination_configuration_with_secrets(
        clickhouse_configuration
    )

    destinations_to_create: list[dict] = []
    destinations_to_update: list[dict] = []

    is_configured_destination_exists: bool = False
    for existing_destination in existing_destinations:
        destination_name = (
            f"{clickhouse_connectors_destination['slug']}_default"  # clickhouse_default
        )

        if destination_name == existing_destination["name"]:
            existing_destination_configuration = existing_destination[
                "connectionConfiguration"
            ]
            destination_configuration_delta = DeepDiff(
                clickhouse_configuration,
                existing_destination_configuration,
                exclude_paths=["root['password']"],
            )

            is_configured_destination_exists = True

            if "values_changed" in destination_configuration_delta:
                clickhouse_configuration["destinationId"] = existing_destination[
                    "destinationId"
                ]
                destinations_to_update.append(clickhouse_configuration)

                # Logging changed fields
                changed_fields = [
                    str(path)
                    for path in destination_configuration_delta["values_changed"].keys()
                ]
                print(
                    f"The destination named clickhouse_default has changed. "
                    f"The following are the fields that have been changed "
                    f"| {changed_fields} |"
                )
                break

    if not is_configured_destination_exists:
        for existing_destinations_definition in existing_destinations_definitions:
            if (
                clickhouse_connectors_destination["slug"]
                == existing_destinations_definition["name"]
            ):
                clickhouse_configuration["destinationDefinitionId"] = (
                    existing_destinations_definition["destinationDefinitionId"]
                )
                destinations_to_create.append(clickhouse_configuration)
                break

    return {
        "destinations_to_create": destinations_to_create,
        "destinations_to_update": destinations_to_update,
    }


def filter_connections_for_update_and_creation(
    presets: dict[str, Any],
    datasources: list[dict[str, Any]],
    existing_connections: list[dict],
    existing_sources: list[dict],
    existing_destinations: list[dict],
) -> dict[str, list[dict]]:
    """
    Here we filter which connections we will create, and which ones we will update, or do nothing.
    As input, we receive configs and existing sources, connections, destinations

    Using DeepDiff, we compare the connection configuration that we have in presets.json
    and the one that already exists (if not, we create it)

    We return a dictionary with two keys: connections_to_create, connections_to_update
    """

    connections_to_create: list[dict] = []
    connections_to_update: list[dict] = []

    for datasource in datasources:
        preset_name: str = datasource["preset"]
        source_type: str = datasource["source_type"]
        account_id: str = datasource["account_id"]
        source_name: str = f"{source_type}_{preset_name}_{account_id}"
        raw_connection_configuration = presets["connection_presets"][preset_name]
        raw_connection_configuration_copy = copy.deepcopy(raw_connection_configuration)

        cleaned_connection_configuration = clean_raw_connection_config(
            raw_connection_configuration_copy
        )

        if not any(
            str(existing_connection["name"]).startswith(source_name)
            for existing_connection in existing_connections
        ):
            # Getting sourceId in an existing source by name
            source_id: str | None = next(
                (
                    existing_source["sourceId"]
                    for existing_source in existing_sources
                    if existing_source["name"] == source_name
                ),
                None,
            )
            destination_id: str | None = next(
                (
                    existing_destination["destinationId"]
                    for existing_destination in existing_destinations
                    if existing_destination["name"] == "clickhouse_default"
                ),
                None,
            )

            if not source_id or not destination_id:
                raise ValueError(
                    f"Error: Could not find sourceId or destinationId to create connection, "
                    f"check existing Airbyte objects and connectors, presets, datasources, base configs. "
                    f"Source name that dag is looking for: {source_name}. "
                    f"Destination name that dag is looking for: clickhouse_default"
                )

            cleaned_connection_configuration["sourceId"] = source_id
            cleaned_connection_configuration["destinationId"] = destination_id
            cleaned_connection_configuration["name"] = (
                f"{source_name} → clickhouse_default"
            )

            connections_to_create.append(cleaned_connection_configuration)
            continue

        else:
            for existing_connection in existing_connections:
                if str(existing_connection["name"]).startswith(source_name):
                    existing_connection_copy = copy.deepcopy(existing_connection)
                    connection_configuration_delta = DeepDiff(
                        cleaned_connection_configuration, existing_connection_copy
                    )

                    if "values_changed" in connection_configuration_delta:
                        cleaned_connection_configuration["connectionId"] = (
                            existing_connection["connectionId"]
                        )
                        cleaned_connection_configuration["name"] = (
                            f"{source_name} → clickhouse_default"
                        )

                        connections_to_update.append(cleaned_connection_configuration)

                        # Logging changed fields
                        changed_fields = [
                            str(path)
                            for path in connection_configuration_delta[
                                "values_changed"
                            ].keys()
                        ]
                        print(
                            f"The connection named '{source_name} → clickhouse_default' has changed. "
                            f"The following are the fields that have been changed "
                            f"| {changed_fields} |"
                        )

    return {
        "connections_to_create": connections_to_create,
        "connections_to_update": connections_to_update,
    }


params: dict[str, str] = {"namespace": "place_for_namespace"}


@dag(
    params=params,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id="create_connections_dag",
)
def create_connections_dag():
    @task
    def get_configurations(**kwargs) -> dict[str, object]:
        """Get configurations from presets.json, airflow variables"""
        dag_params: dict = (
            kwargs.get("dag_run").conf
            if kwargs.get("dag_run")
            else kwargs["dag"].params
        )

        get_configurations_task_result: dict[str, object] = get_configs(
            namespace=dag_params.get("namespace", "etlcraft"),
            config_names=["datasources", "presets", "connectors"],
        )
        return get_configurations_task_result

    @task
    def prepare(base_configuration: dict, **kwargs):
        """
        Prepare gets workspace_id, airbyte_conn_id from the received configs from get_configuration
        """
        base: dict[str, Any] = base_configuration.get("base", {})
        datasources: dict[str, dict[str, Any]] = base_configuration.get("datasources", {})
        presets: dict[str, Any] = base_configuration.get("presets", {})
        connectors: dict[str, list[dict]] = base_configuration.get("connectors", {})

        if not base:
            raise Exception(
                "Base config not provided. Ensure that you have created base.json "
                "and set up Airflow variables for the source, format, and path accordingly."
            )

        if not datasources:
            raise Exception(
                "Datasources config not provided, make sure airflow variable "
                "from_datacraft is created, and source, path, format respectively"
            )

        if not presets:
            raise Exception(
                "Presets config not provided. Make sure presets.json file is created "
                "and source, format, path variables are specified in airflow"
            )

        datasources: list[dict[str, Any]] = validate_and_transform_datasources(datasources)

        workspace_id: str = base.get("workspace_id")
        airbyte_conn_id: str = base.get("airbyte_conn_id") or "airbyte_default"

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

        # Get all existing source_definitions, destination_definitions and pass it to other tasks
        airbyte_list_source_definitions_operator = AirbyteListSourceDefinitionsOperator(
            task_id="get_list_airbyte_source_definitions",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id,
        )
        airbyte_list_destination_definitions_operator = (
            AirbyteListDestinationDefinitionsOperator(
                task_id="get_list_airbyte_destination_definitions",
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
            )
        )
        airbyte_list_sources_operator = AirbyteListSourcesOperator(
            task_id="get_list_airbyte_sources",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id,
        )
        airbyte_list_destinations_operator = AirbyteListDestinationsOperator(
            task_id="get_list_airbyte_destinations",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id,
        )

        # These are the received objects in the form of a Pydantic model
        existing_source_definitions_pydantic: list[SourceDefinitionSpec] = (
            airbyte_list_source_definitions_operator.execute(context=kwargs)
        )
        existing_destination_definitions_pydantic: list[DestinationDefinitionSpec] = (
            airbyte_list_destination_definitions_operator.execute(context=kwargs)
        )
        existing_sources_pydantic: list[SourceSpec] = (
            airbyte_list_sources_operator.execute(context=kwargs)
        )
        existing_destinations_pydantic: list[DestinationSpec] = (
            airbyte_list_destinations_operator.execute(context=kwargs)
        )

        # Convert the resulting pydantic models to python dict
        existing_source_definitions: list[dict] = pydantic_models_to_dict(
            existing_source_definitions_pydantic
        )
        existing_destination_definitions: list[dict] = pydantic_models_to_dict(
            existing_destination_definitions_pydantic
        )
        existing_sources: list[dict] = pydantic_models_to_dict(
            existing_sources_pydantic
        )
        existing_destinations: list[dict] = pydantic_models_to_dict(
            existing_destinations_pydantic
        )

        # We pass variables to xcom to use them in other tasks
        kwargs["ti"].xcom_push(key="workspace_id", value=workspace_id)
        kwargs["ti"].xcom_push(key="airbyte_conn_id", value=airbyte_conn_id)

        # Define what source we will update, and which we will create
        filtered_sources: dict[str, list[dict]] = (
            filter_sources_for_update_and_creation(
                base,
                datasources,
                presets,
                connectors,
                existing_sources,
                existing_source_definitions,
            )
        )

        # Define what destination we will update, and which we will create
        filtered_destinations: dict[str, list[dict]] = (
            filter_destinations_for_update_and_creation(
                base,
                connectors,
                existing_destinations,
                existing_destination_definitions,
            )
        )

        kwargs["ti"].xcom_push(key="datasources", value=datasources)
        kwargs["ti"].xcom_push(key="presets", value=presets)

        object_map = {
            "sources_to_create": filtered_sources["sources_to_create"],
            "sources_to_update": filtered_sources["sources_to_update"],
            "destinations_to_create": filtered_destinations["destinations_to_create"],
            "destinations_to_update": filtered_destinations["destinations_to_update"],
        }

        return object_map

    @task
    def get_sources_to_create(prepared_sources: dict) -> list[dict]:
        """Get prepare data for create_sources"""
        return prepared_sources.get("sources_to_create", [])

    @task
    def get_destinations_to_create(prepared_destinations: dict) -> list[dict]:
        """Get prepare data for create_destinations"""
        return prepared_destinations.get("destinations_to_create", [])

    @task
    def get_connections_to_create(prepared_connections: dict) -> list[dict]:
        """Get prepare data for create_connections"""
        return prepared_connections.get("connections_to_create", [])

    @task
    def get_sources_to_update(prepared_sources: dict) -> list[dict]:
        """Get prepare data for update_sources"""
        return prepared_sources.get("sources_to_update", [])

    @task
    def get_destinations_to_update(prepared_destinations: dict) -> list[dict]:
        """Get prepare data for update_destinations"""
        return prepared_destinations.get("destinations_to_update", [])

    @task
    def get_connections_to_update(prepared_connections: dict) -> list[dict]:
        """Get prepare data for update_connections"""
        return prepared_connections.get("connections_to_update", [])

    @task(trigger_rule="none_failed")
    def prepare_connections_to_create_and_update(**kwargs):
        """
        In this task we prepare data for creating or updating connections.
        We will also convert the incoming pydantic models with ListOperators into python dictionaries
        to make it convenient to compare existing connections with those in presets.json

        Also, here we refill the existing airbyte(ListOperator) objects,
        since during previous tasks new objects could have been created, and we need relevance
        """
        workspace_id: str = kwargs["ti"].xcom_pull(
            key="workspace_id", task_ids="prepare"
        )
        airbyte_conn_id: str = kwargs["ti"].xcom_pull(
            key="airbyte_conn_id", task_ids="prepare"
        )
        presets: dict[str, Any] = kwargs["ti"].xcom_pull(key="presets", task_ids="prepare")
        datasources: list[dict[str, Any]] = kwargs["ti"].xcom_pull(
            key="datasources", task_ids="prepare"
        )

        airbyte_list_connections_operator = AirbyteListConnectionsOperator(
            task_id="get_list_airbyte_connections",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id,
        )
        airbyte_updated_list_sources_operator = AirbyteListSourcesOperator(
            task_id="get_updated_list_airbyte_sources",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id,
        )
        airbyte_updated_list_destinations_operator = AirbyteListDestinationsOperator(
            task_id="get_updated_list_airbyte_destinations",
            airbyte_conn_id=airbyte_conn_id,
            workspace_id=workspace_id,
        )

        # Pydantic models. Needed to convert to dict
        existing_sources_pydantic: list[SourceSpec] = (
            airbyte_updated_list_sources_operator.execute(context=kwargs)
        )
        existing_destinations_pydantic: list[DestinationSpec] = (
            airbyte_updated_list_destinations_operator.execute(context=kwargs)
        )
        existing_connections_pydantic: list[ConnectionSpec] = (
            airbyte_list_connections_operator.execute(context=kwargs)
        )

        # Converted pydantic models to python dict
        existing_sources: list[dict] = pydantic_models_to_dict(
            existing_sources_pydantic
        )
        existing_destinations: list[dict] = pydantic_models_to_dict(
            existing_destinations_pydantic
        )
        existing_connections: list[dict] = pydantic_models_to_dict(
            existing_connections_pydantic
        )

        filtered_connections: dict[str, list[dict]] = (
            filter_connections_for_update_and_creation(
                presets,
                datasources,
                existing_connections,
                existing_sources,
                existing_destinations,
            )
        )
        return filtered_connections

    @task
    def create_source(source_configuration: dict, **kwargs):
        """
        Create source in Airbyte
        :param source_configuration: A dict with configuration info for airbyte source creation
        """
        if source_configuration:
            # We get variables from prepare that we use in the operator
            workspace_id: str = kwargs["ti"].xcom_pull(
                key="workspace_id", task_ids="prepare"
            )
            airbyte_conn_id: str = kwargs["ti"].xcom_pull(
                key="airbyte_conn_id", task_ids="prepare"
            )
            random_int: int = random.randint(1000, 9999)

            # Making up a name for the source in Airbyte
            source_name: str = source_configuration.pop("name_for_source_creation")
            source_definition_id = source_configuration.pop("sourceDefinitionId")

            create_source_task = AirbyteCreateSourceOperator(
                task_id=f"create_source_{source_name}_{random_int}",
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                source_definition_id=source_definition_id,
                connection_configuration=source_configuration,
                name=source_name,
            )

            create_source_task_result = create_source_task.execute(context=kwargs)
            return create_source_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def create_destination(
        destination_configuration: dict | ClickhouseDestinationConfiguration, **kwargs
    ):
        if destination_configuration:

            destination_definition_id: str = destination_configuration.pop(
                "destinationDefinitionId"
            )
            workspace_id: str = kwargs["ti"].xcom_pull(
                key="workspace_id", task_ids="prepare"
            )
            airbyte_conn_id: str = kwargs["ti"].xcom_pull(
                key="airbyte_conn_id", task_ids="prepare"
            )
            random_int: int = random.randint(1000, 9999)

            # If other DBs will be used, then the hardcode will need to be redone
            destination_name = "clickhouse_default"

            create_destination_task = AirbyteCreateDestinationOperator(
                task_id=f"create_destination_{destination_name}_{random_int}",
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                destination_definition_id=destination_definition_id,
                connection_configuration=destination_configuration,
                name=destination_name,
            )

            create_destination_task_result = create_destination_task.execute(
                context=kwargs
            )
            return create_destination_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def update_source(source_configuration: dict, **kwargs):
        if source_configuration:
            # We get variables from prepare that we use in the operator
            workspace_id: str = kwargs["ti"].xcom_pull(
                key="workspace_id", task_ids="prepare"
            )
            airbyte_conn_id: str = kwargs["ti"].xcom_pull(
                key="airbyte_conn_id", task_ids="prepare"
            )
            random_int: int = random.randint(1000, 9999)

            # Making up a name for the source in Airbyte
            source_name: str = source_configuration.pop("name_for_source_creation")
            source_id = source_configuration.pop("sourceId")

            update_source_task = AirbyteUpdateSourceOperator(
                task_id=f"update_source_{source_name}_{random_int}",
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                source_id=source_id,
                connection_configuration=source_configuration,
                name=source_name,
            )

            update_source_task_result = update_source_task.execute(context=kwargs)
            return update_source_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def update_destination(
        destination_configuration: dict | ClickhouseDestinationConfiguration, **kwargs
    ):
        if destination_configuration:
            destination_id: str = destination_configuration.pop("destinationId")
            workspace_id: str = kwargs["ti"].xcom_pull(
                key="workspace_id", task_ids="prepare"
            )
            airbyte_conn_id: str = kwargs["ti"].xcom_pull(
                key="airbyte_conn_id", task_ids="prepare"
            )
            random_int: int = random.randint(1000, 9999)

            # If other DBs will be used, then the hardcode will need to be redone
            destination_name = "clickhouse_default"

            update_destination_task = AirbyteUpdateDestinationOperator(
                task_id=f"update_destination_{destination_name}_{random_int}",
                airbyte_conn_id=airbyte_conn_id,
                workspace_id=workspace_id,
                destination_id=destination_id,
                connection_configuration=destination_configuration,
                name=destination_name,
            )

            update_destination_task_result = update_destination_task.execute(
                context=kwargs
            )
            return update_destination_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def create_connection(connection_configuration: dict, **kwargs):
        if connection_configuration:
            source_id = connection_configuration.pop("sourceId")
            destination_id: str = connection_configuration.pop("destinationId")
            connection_name = connection_configuration.pop("name")
            connection_name_for_task_id = connection_name.split(" ")[0]

            airbyte_conn_id: str = kwargs["ti"].xcom_pull(
                key="airbyte_conn_id", task_ids="prepare"
            )
            random_int: int = random.randint(1000, 9999)

            create_connection_task = AirbyteCreateConnectionOperator(
                task_id=(
                    f"create_connection_definition_"
                    f"{connection_name_for_task_id}_{random_int}"
                ),
                name=connection_name,
                airbyte_conn_id=airbyte_conn_id,
                source_id=source_id,
                destination_id=destination_id,
                params=connection_configuration,
            )

            create_connection_task_result = create_connection_task.execute(
                context=kwargs
            )
            return create_connection_task_result
        else:
            return "No configuration provided. Task skipped"

    @task
    def update_connection(connection_configuration: dict, **kwargs):
        if connection_configuration:
            connection_id = connection_configuration.pop("connectionId")
            connection_name = connection_configuration.pop("name")
            connection_name_for_task_id = connection_name.split(" ")[0]

            airbyte_conn_id: str = kwargs["ti"].xcom_pull(
                key="airbyte_conn_id", task_ids="prepare"
            )
            random_int: int = random.randint(1000, 9999)

            update_connection_task = AirbyteUpdateConnectionOperator(
                task_id=(
                    f"update_connection_definition_"
                    f"{connection_name_for_task_id}_{random_int}"
                ),
                name=connection_name,
                airbyte_conn_id=airbyte_conn_id,
                connection_id=connection_id,
                params=connection_configuration,
            )

            update_connection_task_result = update_connection_task.execute(
                context=kwargs
            )
            return update_connection_task_result
        else:
            return "No configuration provided. Task skipped"

    """
    Taskflow API:
    1. Get Configurations (base, connectors, presets, datasources)
    2. Prepare configs and data for creating | updating
    3. Create | Update sources
    4. Create | Update destinations
    5. prepare and update data for connections
    6. Create | Update connections
    """
    get_configurations_task = get_configurations()
    prepare_task = prepare(base_configuration=get_configurations_task)

    get_sources_to_create_task = get_sources_to_create(prepare_task)
    get_destinations_to_create_task = get_destinations_to_create(prepare_task)
    get_sources_to_update_task = get_sources_to_update(prepare_task)
    get_destinations_to_update_task = get_destinations_to_update(prepare_task)

    create_sources_tasks = create_source.expand(
        source_configuration=get_sources_to_create_task
    )

    create_destinations_tasks = create_destination.expand(
        destination_configuration=get_destinations_to_create_task
    )

    update_sources_tasks = update_source.expand(
        source_configuration=get_sources_to_update_task
    )

    update_destinations_tasks = update_destination.expand(
        destination_configuration=get_destinations_to_update_task
    )

    prepare_connections_to_create_and_update_task = (
        prepare_connections_to_create_and_update()
    )

    get_connections_to_create_task = get_connections_to_create(
        prepare_connections_to_create_and_update_task
    )
    get_connections_to_update_task = get_connections_to_update(
        prepare_connections_to_create_and_update_task
    )

    create_connection_tasks = create_connection.expand(
        connection_configuration=get_connections_to_create_task
    )
    update_connection_tasks = update_connection.expand(
        connection_configuration=get_connections_to_update_task
    )

    # Prepare data for create/update definitions
    get_configurations_task >> prepare_task
    prepare_task >> get_sources_to_create_task
    prepare_task >> get_destinations_to_create_task
    prepare_task >> get_sources_to_update_task
    prepare_task >> get_destinations_to_update_task

    # Create/Update definitions
    get_sources_to_create_task >> create_sources_tasks
    get_destinations_to_create_task >> create_destinations_tasks
    get_sources_to_update_task >> update_sources_tasks
    get_destinations_to_update_task >> update_destinations_tasks

    [
        create_sources_tasks,
        create_destinations_tasks,
        update_sources_tasks,
        update_destinations_tasks,
    ] >> prepare_connections_to_create_and_update_task

    prepare_connections_to_create_and_update_task >> get_connections_to_create_task
    prepare_connections_to_create_and_update_task >> get_connections_to_update_task

    get_connections_to_create_task >> create_connection_tasks
    get_connections_to_update_task >> update_connection_tasks


dag_instance = create_connections_dag()
