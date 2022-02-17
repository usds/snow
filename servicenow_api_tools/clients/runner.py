from urllib3.util import Retry, make_headers  # type: ignore
import json


def make_request(http_client, username: str, password: str, method: str, url: str, obj=None):
    headers = make_headers(basic_auth=f"{username}:{password}")
    headers["Accept"] = "application/json"
    headers["Content-Type"] = "application/json"

    if (method == "POST" or method == "PUT") and obj is None:
        raise ValueError(f"An obj must be provided for {method} requests")

    if obj:
        response = http_client.request(
            method,
            url,
            headers=headers,
            body=json.dumps(obj),
            retries=Retry(total=6, backoff_factor=0.2),
        )
    else:
        response = http_client.request(
            method,
            url,
            headers=headers,
            retries=Retry(total=6, backoff_factor=0.2),
        )
    return json.loads(response.data.decode("utf-8"))
