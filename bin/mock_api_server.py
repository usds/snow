from http.server import BaseHTTPRequestHandler, HTTPServer
from servicenow_api_tools.mock_api_server import ServicenowRestEndpointLocalDataset
from functools import partial
import json
import click
import traceback


class MockServicenowAPIServer(BaseHTTPRequestHandler):
    def __init__(self, data_directory, schemas_directory, *args, **kwargs):
        self.data_directory = data_directory
        self.schemas_directory = schemas_directory
        self.endpoint = ServicenowRestEndpointLocalDataset(
            path=self.data_directory, schema_dir=self.schemas_directory)
        # BaseHTTPRequestHandler calls do_GET **inside** __init__ !!!
        # So we have to call super().__init__ after setting attributes.
        super().__init__(*args, **kwargs)

    def do_GET(self):
        try:
            json_str = json.dumps(self.endpoint.get(self.path), indent=4, sort_keys=True)
        except Exception as e:
            print(f"Got exception: {str(e)}")
            print(" ======== START STACKTRACE ======== ")
            print(traceback.format_exc())
            print(" ======== END STACKTRACE ======== ")
            json_str = json.dumps({"error": str(e)})
            # TODO: We don't know if this is a client error, but it would take a lot more work to
            # classify errors in the mock query library based on whether they come from user input.
            self.send_response(400)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json_str.encode(encoding='utf_8'))
            return
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json_str.encode(encoding='utf_8'))


@click.command()
@click.option('--hostname', type=str, default="localhost", help="Hostname to listen on.")
@click.option('--port', type=int, default=8080, help="Port to listen on.")
@click.option('--data-directory', required=True, type=str, help="Directory containing the dataset.")
@click.option('--schemas-directory', required=True, type=str, help="Directory with table schemas.")
def mock_api_server(hostname, port, data_directory, schemas_directory):
    """Runs a mock Servicenow server against a local dataset"""
    # https://stackoverflow.com/a/52046062
    handler = partial(MockServicenowAPIServer, data_directory, schemas_directory)
    server = HTTPServer((hostname, port), handler)
    click.echo("Server started http://%s:%s" % (hostname, port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass

    server.server_close()
    click.echo("Server stopped.")


if __name__ == '__main__':
    mock_api_server()
