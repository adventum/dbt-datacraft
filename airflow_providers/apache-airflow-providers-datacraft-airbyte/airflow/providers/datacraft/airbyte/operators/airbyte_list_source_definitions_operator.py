from ..models import SourceDefinitionSpec, WorkspaceSpec
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteListSourceDefinitionsOperator(AirByteGeneralOperator):
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
            endpoint="source_definitions/list_for_workspace",
            request_params={"workspaceId": self._workspace_id},
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> list[SourceDefinitionSpec] | None:
        resp: dict[str, any] = super().execute(context)
        res: list[SourceDefinitionSpec] = [
            SourceDefinitionSpec.model_validate(spec)
            for spec in resp["sourceDefinitions"]
        ]
        return res
