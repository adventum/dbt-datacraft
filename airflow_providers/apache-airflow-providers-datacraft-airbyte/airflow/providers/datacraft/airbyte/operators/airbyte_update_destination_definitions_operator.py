from ..models import (
    BaseDestinationDefinition,
    DestinationDefinitionSpec,
    WorkspaceSpec,
)
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteUpdateDestinationDefinitionsOperator(AirByteGeneralOperator):
    """
    Update AirByte destination definition
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    :param destination_definition_id: Destination Definition id to update
    :param destination_definition_configuration: Airbyte destination definition params include destinationDefinitionId
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        destination_definition_id: str | None = None,
        destination_definition_configuration: BaseDestinationDefinition | None = None,
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="destination_definitions/update",
            request_params={
                "workspaceId": self._workspace_id,
                "destinationDefinitionId": destination_definition_id,
                **destination_definition_configuration,
            },
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> DestinationDefinitionSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: DestinationDefinitionSpec = DestinationDefinitionSpec.model_validate(resp)
        return res
