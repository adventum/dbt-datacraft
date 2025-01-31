from ..operators import AirByteGeneralOperator

from airflow.utils.context import Context
from ..models import (
    ConnectionSpec,
)


class AirbyteCreateConnectionOperator(AirByteGeneralOperator):
    """
    Create connection operator
    :param airbyte_conn_id: Airflow connection object for AirByte
    :param name: Required. Name of new connection
    :param source_id: Source id to create connection
    :param destination_id: Destination id to create connection
    :param params: Provide connection configuration e.g. syncCatalog, prefix etc
    """

    def __init__(
        self,
        name: str,
        airbyte_conn_id: str | None = None,
        source_id: str | None = None,
        destination_id: str | None = None,
        params: dict | None = None,
        **kwargs,
    ):
        self.name = name
        self.source_id = source_id
        if not self.source_id:
            raise ValueError(
                "To create connection you must specify source_id or source_name"
            )

        self.destination_id = destination_id
        if not self.destination_id:
            raise ValueError(
                "To create connection you must specify destination_id or destination_name"
            )

        self.params = params
        self.kwargs = kwargs

        super().__init__(
            airbyte_conn_id=airbyte_conn_id,
            endpoint="connections/create",
            request_params={
                "name": self.name,
                "sourceId": self.source_id,
                "destinationId": self.destination_id,
                **self.params,
            },
            use_legacy=True,
            **self.kwargs,
        )

    def execute(self, context: Context) -> ConnectionSpec | None:
        resp: dict[str, any] = super().execute(context)
        res: ConnectionSpec = ConnectionSpec.model_validate(resp)
        return res
