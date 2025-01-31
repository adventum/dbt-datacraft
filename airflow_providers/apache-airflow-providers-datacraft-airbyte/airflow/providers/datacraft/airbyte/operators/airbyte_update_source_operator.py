from ..models import (
    SourceSpec,
    WorkspaceSpec,
)
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteUpdateSourceOperator(AirByteGeneralOperator):
    """
    Update AirByte source
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    :param source_id: Airbyte source id
    :param connection_configuration: Configuration of source
    :param name: Name of source
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        source_id: str | None = None,
        connection_configuration: dict[str, any] | None = None,
        name: str | None = None,
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="sources/update",
            request_params={
                "workspaceId": self._workspace_id,
                "sourceId": source_id,
                "connectionConfiguration": connection_configuration,
                "name": name,
            },
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> SourceSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: SourceSpec = SourceSpec.model_validate(resp)
        return res
