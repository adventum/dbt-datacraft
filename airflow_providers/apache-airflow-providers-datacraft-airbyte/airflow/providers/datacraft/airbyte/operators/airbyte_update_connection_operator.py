from ..operators import AirByteGeneralOperator

from airflow.utils.context import Context
from ..models import (
    ConnectionSpec,
)


class AirbyteUpdateConnectionOperator(AirByteGeneralOperator):
    """
    Create connection operator
    :param airbyte_conn_id: Airflow connection object for AirByte
    :param connection_id: Update existing connection by ID
    :param name: Required. Name of new connection
    :param params: Provide connection configuration, e.g. syncCatalog, prefix etc
    """

    def __init__(
        self,
        name: str,
        airbyte_conn_id: str | None = None,
        connection_id: str | None = None,
        params: dict | None = None,
        **kwargs,
    ):
        self.name = name

        self.connection_id = connection_id
        if not self.connection_id:
            raise ValueError("To update connection you must specify connection_id")

        self.params = params
        self.kwargs = kwargs

        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="connections/update",
            request_params={
                "name": self.name,
                "connectionId": self.connection_id,
                **self.params,
            },
            use_legacy=True,
            **self.kwargs,
        )

    def execute(self, context: Context) -> ConnectionSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: ConnectionSpec = ConnectionSpec.model_validate(resp)
        return res
