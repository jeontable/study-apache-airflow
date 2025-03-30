import requests
from airflow.hooks.base import BaseHook


class MovielensHook(BaseHook):
    """
    Custom hook to interact with the Movielens API.
    """
    DEFAULT_SCHEMA = "http"
    DEFAULT_HOST = "192.168.0.231"
    DEFAULT_PORT = 5000


    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._session = None
        self._base_url = None


    def get_conn(self):
        """
        Fetch movies from the Movielens API.
        """

        if self._session is None:
            config = self.get_connection(self._conn_id)

            schema = config.schema or self.DEFAULT_SCHEMA
            host = config.host or self.DEFAULT_HOST
            port = config.port or self.DEFAULT_PORT

            self._base_url = f"{schema}://{host}:{port}"
            self._session = requests.Session()

        if config.login:
            self._session.auth = (config.login, config.password) 

        return self._session, self._base_url



    def close(self):
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None


    def _get_with_pagination(self, endpoint, params, batch_size=100):

        session, base_url = self.get_conn()

        url = f"{base_url}{endpoint}"

        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(url,
                                params={
                                    **params,
                                    **{"offset": offset, "limit": batch_size}
                                })
            response.raise_for_status()
            response_json = response.json()

            yield from response_json["result"] # list

            offset += batch_size
            total = response_json["total"]



    def get_ratings(self, start_date, end_date, batch_size=100):

        yield from self._get_with_pagination(  # generator
            endpoint="/ratings",
            params={
                "start_date": start_date,
                "end_date": end_date,
            },
            batch_size=batch_size,
        )