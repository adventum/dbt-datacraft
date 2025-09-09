from ..models import SourceSpec, WorkspaceSpec
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context

from ..utils import get_workspace


class AirbyteListSourcesOperator(AirByteGeneralOperator):
    """
    List AirByte existing sources
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        # use_legacy: bool = False,
        use_legacy: bool = True,  # TODO: new api is not supported yet
        **kwargs,
    ):
        self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="sources/list",
            request_params={"workspaceId": self._workspace_id},
            use_legacy=use_legacy,
            **kwargs,
        )

    def execute_new(self, context: Context) -> list[SourceSpec] | None:
        raise NotImplementedError()

    def execute_legacy(self, context: Context) -> list[SourceSpec] | None:
        resp: dict[str, any] = super().execute(context)
        res: list[SourceSpec] = [
            SourceSpec.model_validate(spec) for spec in resp["sources"]
        ]
        return res

    def execute(self, context: Context) -> list[SourceSpec] | None:
        if self.use_legacy:
            return self.execute_legacy(context)
        else:
            return self.execute_new(context)
