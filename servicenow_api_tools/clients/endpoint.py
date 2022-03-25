from servicenow_api_tools.clients import utils, runner
import urllib.request
import urllib3  # type: ignore
from typing import Dict


class ServicenowRestEndpoint:
    def __init__(self, base_url: str = None, instance: str = None,
                 username: str = None, password: str = None,
                 credentials_from_env: bool = True, max_connections: int = 10):
        if instance:
            assert not base_url, "Cannot set both instance and base_url"
            self.base_url = f'https://{instance}.servicenowservices.com'
        elif base_url:
            assert not instance, "Cannot set both instance and base_url"
            self.base_url = base_url
        else:
            raise Exception("Must set either instance or base_url")
        (self.username, self.password) = utils.load_credentials(
            username, password, credentials_from_env)
        self.proxies = urllib.request.getproxies()
        if 'https' not in self.proxies:
            self.http_client = urllib3.PoolManager(maxsize=max_connections)
        else:
            self.http_client = urllib3.ProxyManager(
                self.proxies['https'],
                maxsize=max_connections)

    def get(self, resource: str):
        full_url = f'{self.base_url}{resource}'
        return runner.make_request(
            self.http_client, self.username, self.password,
            "GET", full_url, None)

    def put(self, resource: str, obj: Dict):
        full_url = f'{self.base_url}{resource}'
        return runner.make_request(
            self.http_client, self.username, self.password,
            "PUT", full_url, obj)

    def post(self, resource: str, obj: Dict):
        full_url = f'{self.base_url}{resource}'
        return runner.make_request(
            self.http_client, self.username, self.password,
            "POST", full_url, obj)
