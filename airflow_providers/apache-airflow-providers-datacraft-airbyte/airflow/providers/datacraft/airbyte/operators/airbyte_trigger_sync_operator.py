from ..models import (
    JobSyncSpec,
    WorkspaceSpec,
    TriggerSyncResponseSpec,
)
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context


class AirbyteTriggerSyncOperator(AirByteGeneralOperator):
    """
    Trigger a sync
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    :param connection_id: Airbyte connection id to sync.
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        workspace_id: str | None = None,
        workspace_name: str | None = None,
        workspaces: list[WorkspaceSpec] | None = None,
        connection_id: str | None = None,
        **kwargs,
    ):
        # self._workspace_id = get_workspace(workspace_id, workspace_name, workspaces)
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="connections/sync",
            request_params={
                # "workspaceId": self._workspace_id,
                "connectionId": connection_id,
            },
            use_legacy=True,
            **kwargs,
        )

    def execute(self, context: Context) -> JobSyncSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: TriggerSyncResponseSpec = TriggerSyncResponseSpec.model_validate(resp)
        return res
