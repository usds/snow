from typing import Dict
import logging
import servicenow_api_tools.mock_api_server.query as query
from servicenow_api_tools.clients import ServicenowRestEndpoint


class ServicenowRestEndpointLocalDataset(ServicenowRestEndpoint):
    def __init__(self, path: str, schema_dir: str):
        self.logger = logging.getLogger(__name__)
        self.runner = query.LocalDatasetQueryRunner(path, schema_dir)
        self.path = path

    def get(self, url: str) -> Dict:
        full_url = f"https://placeholder{url}"
        self.logger.debug(f"Running {full_url} against path {self.path}")
        return self.runner.query(url)

    def put(self, url: str, obj: Dict) -> Dict:
        raise Exception("PUT not yet implemented on local dataset rest endpoint")

    def post(self, url: str, obj: Dict) -> Dict:
        raise Exception("POST not yet implemented on local dataset rest endpoint")
