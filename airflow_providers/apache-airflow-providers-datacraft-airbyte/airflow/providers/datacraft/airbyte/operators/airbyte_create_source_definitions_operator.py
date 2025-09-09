from ..models import (
    BaseSourceDefinition,
    SourceDefinitionSpec,
    WorkspaceSpec,
)
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteCreateSourceDefinitionsOperator(AirByteGeneralOperator):
    """
    Create AirByte source definition
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    :param source_definition: Airbyte source definition params
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        source_definition_configuration: BaseSourceDefinition | None = None,
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="source_definitions/create_custom",
            request_params={
                "workspaceId": self._workspace_id,
                "sourceDefinition": source_definition_configuration,
            },
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> SourceDefinitionSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: SourceDefinitionSpec = SourceDefinitionSpec.model_validate(resp)
        return res
