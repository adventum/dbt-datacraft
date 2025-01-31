from .models import WorkspaceSpec, ConnectionSpec


def get_workspace_id_by_name(name: str, workspaces: list[WorkspaceSpec]) -> str | None:
    """Get workspace_id by name and list of workspaces"""
    workspace_id: str | None = None
    for ws in workspaces:
        if ws.name == name:
            workspace_id = ws.id
            break
    return workspace_id


def get_workspace(
    workspace_id: str | None,
    workspace_name: str | None,
    workspaces: list[WorkspaceSpec] | None,
    allow_none: bool = False,
) -> str | None:
    if not workspace_id:
        if not workspace_name or not workspaces:
            if allow_none:
                return None
            else:
                raise ValueError(
                    "workspace_id or workspace_name and workspaces are required"
                )
        workspace_id = get_workspace_id_by_name(workspace_name, workspaces)
        if not allow_none and not workspace_id:
            raise ValueError("workspace not found")
    return workspace_id


def get_connection_id_by_name(
    name: str, connections: list[ConnectionSpec]
) -> str | None:
    """Get connection_id by name and list of workspaces"""
    connection_id: str | None = None
    for conn in connections:
        if conn.name == name:
            connection_id = conn.id
            break
    return connection_id


def get_connection(
    connection_id: str | None,
    connection_name: str | None,
    connections: list[ConnectionSpec] | None,
    allow_none: bool = False,
) -> str | None:
    if not connection_id:
        if not connection_name or not connections:
            if allow_none:
                return None
            else:
                raise ValueError(
                    "connection_id or connection_name and connections are required"
                )
        connection_id = get_connection_id_by_name(connection_name, connections)
        if not allow_none and not connection_id:
            raise ValueError("connection not found")
    return connection_id
