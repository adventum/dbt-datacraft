from ..models import (
    DestinationSpec,
    WorkspaceSpec,
)
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteCreateDestinationOperator(AirByteGeneralOperator):
    """
    Create AirByte destination
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    :param destination_definition_id: Airbyte destination definition id
    :param connection_configuration: Configuration of destination
    :param name: Name of destination
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        destination_definition_id: str | None = None,
        connection_configuration: dict[str, any] | None = None,
        name: str | None = None,
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="destinations/create",
            request_params={
                "workspaceId": self._workspace_id,
                "destinationDefinitionId": destination_definition_id,
                "connectionConfiguration": connection_configuration,
                "name": name,
            },
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> DestinationSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: DestinationSpec = DestinationSpec.model_validate(resp)
        return res
