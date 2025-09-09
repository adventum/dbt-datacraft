import base64

from airflow.models import Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook

"""Object deleter functions for Airbyte tests"""


def general_airfbyte_hook_func(
    # airbyte_conn_id: str,
    endpoint: str,
    request_params: dict[str, any],
    airbyte_conn: Connection | str,
):
    """
    e.g. it used in deleter object func's
    :param endpoint: Define an endpoint for interaction
    :param request_params: It's a dict, that has object ids, e.g. {"sourceId": "33ds-566t-323"}
    :param airbyte_conn: Airbyte connection, you place here a func from conftest.py, that named airbyte_conn
    """
    airbyte_conn_id = airbyte_conn
    hook = AirbyteHook(airbyte_conn_id=airbyte_conn_id)
    conn: Connection = hook.get_connection(airbyte_conn_id)
    token: str = base64.b64encode(
        bytes(f"{conn.login}:{conn.password}", "utf-8")
    ).decode("utf-8")

    response = hook.run(
        endpoint=f"api/v1/{endpoint}",
        json=request_params,
        headers={
            "accept": "application/json",
            "Authorization": f"Basic {token}",
        },
    )
    return response


def source_definitions_deleter(
    source_definitions_list: list | None, airbyte_conn: Connection | str
):
    """
    :param source_definitions_list: Source Definitions IDs to delete, like ["4243ab-33vv-43dd", "767hb-87vcc-2424"]
    :param airbyte_conn: Airbyte connection, you place here a func from conftest.py, that named airbyte_conn
    """
    endpoint = "source_definitions/delete"

    if not source_definitions_list:
        pass

    for source_definition in source_definitions_list:
        request_param = {"sourceDefinitionId": f"{source_definition}"}
        general_airfbyte_hook_func(endpoint, request_param, airbyte_conn)


def destination_definitions_deleter(
    destination_definitions_list: list | None, airbyte_conn: Connection | str
):
    """
    :param destination_definitions_list: Destination Definitions IDs to delete, like ["43ab-33vv-43dd", "77hb-87cc-244"]
    :param airbyte_conn: Airbyte connection, you place here a func from conftest.py, that named airbyte_conn
    """
    endpoint = "destination_definitions/delete"

    if not destination_definitions_list:
        pass

    for destination_definition in destination_definitions_list:
        request_param = {"destinationDefinitionId": f"{destination_definition}"}
        general_airfbyte_hook_func(endpoint, request_param, airbyte_conn)


def sources_deleter(sources_list: list | None, airbyte_conn: Connection | str):
    """
    :param sources_list: Source IDs to delete, like ["4243ab-33vv-43dd", "767hb-87vcc-2424"]
    :param airbyte_conn: Airbyte connection, you place here a func from conftest.py, that named airbyte_conn
    """
    endpoint = "sources/delete"

    if not sources_list:
        pass

    for source in sources_list:
        request_param = {"sourceId": f"{source}"}
        general_airfbyte_hook_func(endpoint, request_param, airbyte_conn)


def destinations_deleter(
    destinations_list: list | None, airbyte_conn: Connection | str
):
    """
    :param destinations_list: Destinations IDs to delete, like ["43ab-33vv-43dd", "77hb-87cc-244"]
    :param airbyte_conn: Airbyte connection, you place here a func from conftest.py, that named airbyte_conn
    """
    endpoint = "destinations/delete"

    if not destinations_list:
        pass

    for destination in destinations_list:
        request_param = {"destinationId": f"{destination}"}
        general_airfbyte_hook_func(endpoint, request_param, airbyte_conn)


def connections_deleter(connections_list: list | None, airbyte_conn: Connection | str):
    """
    :param connections_list: Connections IDs to delete, like ["43ab-33vv-43dd", "77hb-87cc-244"]
    :param airbyte_conn: Airbyte connection, you place here a func from conftest.py, that named airbyte_conn
    """
    endpoint = "connections/delete"

    if not connections_list:
        pass

    for connection in connections_list:
        request_param = {"connectionId": f"{connection}"}
        general_airfbyte_hook_func(endpoint, request_param, airbyte_conn)


def collect_airbyte_objects(
    get_workspace_id: str, airbyte_conn: Connection | str
) -> dict[str, list[str]]:
    """
    The function is used by the fixture to clean up test objects
    Specifically, this function collects all airbyte objects and returns a dictionary where the key is an object,
    for example source_definitions, and the value is a list of IDs of such objects
    """

    # sourceDefinitions
    source_definitions: list[str] = [
        source_definition["sourceDefinitionId"]
        for source_definition in general_airfbyte_hook_func(
            endpoint="source_definitions/list",
            request_params={"workspaceId": get_workspace_id},
            airbyte_conn=airbyte_conn,
        ).json()["sourceDefinitions"]
    ]

    # destinationDefinitions
    destination_definitions: list[str] = [
        destination_definition["destinationDefinitionId"]
        for destination_definition in general_airfbyte_hook_func(
            endpoint="destination_definitions/list",
            request_params={"workspaceId": get_workspace_id},
            airbyte_conn=airbyte_conn,
        ).json()["destinationDefinitions"]
    ]

    # sources
    sources: list[str] = [
        source["sourceId"]
        for source in general_airfbyte_hook_func(
            endpoint="sources/list",
            request_params={"workspaceId": get_workspace_id},
            airbyte_conn=airbyte_conn,
        ).json()["sources"]
    ]

    # destinations
    destinations: list[str] = [
        destination["destinationId"]
        for destination in general_airfbyte_hook_func(
            endpoint="destinations/list",
            request_params={"workspaceId": get_workspace_id},
            airbyte_conn=airbyte_conn,
        ).json()["destinations"]
    ]

    # connections
    connections: list[str] = [
        connection["connectionId"]
        for connection in general_airfbyte_hook_func(
            endpoint="connections/list",
            request_params={"workspaceId": get_workspace_id},
            airbyte_conn=airbyte_conn,
        ).json()["connections"]
    ]

    airbyte_objects_dict = {
        "source_definitions": source_definitions,
        "destination_definitions": destination_definitions,
        "sources": sources,
        "destinations": destinations,
        "connections": connections,
    }

    return airbyte_objects_dict
