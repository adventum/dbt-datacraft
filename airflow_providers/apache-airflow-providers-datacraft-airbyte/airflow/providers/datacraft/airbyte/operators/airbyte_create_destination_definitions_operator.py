from ..models import BaseDestinationDefinition, DestinationDefinitionSpec, WorkspaceSpec
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteCreateDestinationDefinitionsOperator(AirByteGeneralOperator):
    """
    Create AirByte destination definitions
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    :param destination_definition: Airbyte destination definition params
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        destination_definition_configuration: BaseDestinationDefinition | None = None,
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="destination_definitions/create_custom",
            request_params={
                "workspaceId": self._workspace_id,
                "destinationDefinition": destination_definition_configuration,
            },
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> DestinationDefinitionSpec | None:
        resp: dict[str, any] = super().execute(context)
        print(resp)
        res: DestinationDefinitionSpec = DestinationDefinitionSpec.model_validate(resp)
        return res
