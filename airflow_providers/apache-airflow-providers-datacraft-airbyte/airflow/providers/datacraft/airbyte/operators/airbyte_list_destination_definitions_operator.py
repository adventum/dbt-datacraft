from ..models import DestinationDefinitionSpec, WorkspaceSpec
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteListDestinationDefinitionsOperator(AirByteGeneralOperator):
    """
    List AirByte source definitions
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="destination_definitions/list_for_workspace",
            request_params={"workspaceId": workspace_id},
            use_legacy=True,
            **kwargs,
        )
        self._workspace_id = workspace_id

    def execute(self, context: Context) -> list[DestinationDefinitionSpec] | None:
        resp: dict[str, any] = super().execute(context)
        res: list[DestinationDefinitionSpec] = [
            DestinationDefinitionSpec.model_validate(spec)
            for spec in resp["destinationDefinitions"]
        ]
        return res
