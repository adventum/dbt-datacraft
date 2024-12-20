from ..models import WorkspaceSpec
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)
from airflow.utils.context import Context


class AirbyteListWorkspacesOperator(AirByteGeneralOperator):
    """
    List AirByte source definitions
    :param airbyte_conn_id: Required. Airbyte connection id
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        # use_legacy: bool = False,
        use_legacy: bool = True,  # TODO: new api is not supported yet
        **kwargs,
    ):
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="workspaces/list",
            use_legacy=use_legacy,
            **kwargs,
        )

    def execute_new(self, context: Context) -> list[WorkspaceSpec] | None:
        raise NotImplementedError()

    def execute_legacy(self, context: Context) -> list[WorkspaceSpec] | None:
        resp: dict[str, any] = super().execute(context)
        res: list[WorkspaceSpec] = [
            WorkspaceSpec.model_validate(spec) for spec in resp["workspaces"]
        ]
        return res

    def execute(self, context: Context) -> list[WorkspaceSpec] | None:
        if self.use_legacy:
            return self.execute_legacy(context)
        else:
            return self.execute_new(context)
