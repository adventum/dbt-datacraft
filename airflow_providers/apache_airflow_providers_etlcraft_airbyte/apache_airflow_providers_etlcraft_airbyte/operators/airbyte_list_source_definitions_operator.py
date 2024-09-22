from typing import TYPE_CHECKING

from ..models import SourceDefinitionSpec
from .airbyte_general_operator import (
    AirByteGeneralOperator,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AirbyteListSourceDefinitionsOperator(AirByteGeneralOperator):
    """
    List AIrByte source definitions
    :param airbyte_conn_id: Required. Airbyte connection id
    :param workspace_id: AirByte workspace id.
    """

    def __init__(self, airbyte_conn_id: str, workspace_id: str, **kwargs):
        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="source_definitions/list_for_workspace",
            request_params={"workspace_id": workspace_id},
            **kwargs,
        )
        self._workspace_id = workspace_id

    def execute(self, context: "Context") -> list[SourceDefinitionSpec] | None:
        resp: dict[str, any] = super().execute(context)
        res: list[SourceDefinitionSpec] = [
            SourceDefinitionSpec.model_validate(spec)
            for spec in resp["sourceDefinitions"]
        ]
        return res
