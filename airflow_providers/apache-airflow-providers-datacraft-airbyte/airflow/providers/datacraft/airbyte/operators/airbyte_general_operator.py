import base64
from airflow.models import BaseOperator, Connection
from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.utils.context import Context


class AirByteGeneralOperator(BaseOperator):
    """
    Operator that makes api requests to AirByte
    :param airbyte_conn_id: Required. ID of valid Airflow Connection of type AirByte (airbyte)
    :param method: Optional. HTTP method. POST is default.
    :param endpoint: Required. Air endpoint. Without /v1/ or host. example: sources.
    :param request_params: Optional. Request parameters.
    :param use_legacy: Optional. Use legacy API. Default is False.
    """

    def __init__(
        self,
        airbyte_conn_id: str,
        endpoint: str,
        endpoint_new: str | None = None,
        method: str = "POST",
        request_params: dict[str, any] | None = None,
        # use_legacy: bool = False,
        use_legacy: bool = True,  # TODO: new api is not supported yet
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.endpoint_legacy = endpoint
        self.endpoint_new = endpoint_new if endpoint_new else endpoint
        self.method = method
        self.request_params = request_params
        self.use_legacy = use_legacy
        self.api_version = "v1"

    def execute(self, context: Context) -> any:
        if self.use_legacy:
            hook = AirbyteHook(
                airbyte_conn_id=self.airbyte_conn_id,
                api_version=self.api_version,
            )
            conn: Connection = hook.get_connection(self.airbyte_conn_id)
            token: str = base64.b64encode(
                bytes(f"{conn.login}:{conn.password}", "utf-8")
            ).decode("utf-8")

            return hook.run(
                endpoint=f"api/{self.api_version}/{self.endpoint_legacy}",
                json=self.request_params,
                headers={
                    "accept": "application/json",
                    "Authorization": f"Basic {token}",
                },
            ).json()
        else:
            raise NotImplementedError()
