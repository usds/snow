from servicenow_api_tools.clients.clients import DescsribeAPIClient
from servicenow_api_tools.clients.endpoint import ServicenowRestEndpoint
import click
import json
import sys
from typing import Dict


def format_result(result):

    def build_value(result: Dict) -> str:
        value = f"{result['value'] if ('value' in result and result['value']) else '(EMPTY)'}"
        if ('display_value' in result and result['display_value']):
            value = f"{value}:({result['display_value']})"
        return value

    def build_key(groupby_fields):
        return ",".join(map(
            lambda x: f"{x['field']}={build_value(x)}",
            groupby_fields))

    if not result:
        return "{}"

    if 'error' in result:
        return json.dumps(result, indent=4, sort_keys=True)

    if 'stats' in result['result']:
        return json.dumps(
            {"total_count": result['result']['stats']['count']},
            indent=4, sort_keys=True)

    return json.dumps({
        build_key(p['groupby_fields']):
        p['stats']['count'] for p in result['result']},
        indent=4, sort_keys=True)


@click.command()
@click.option('--instance', type=str,
              help="ServiceNow instance to use, incompatible with --base-url.")
@click.option('--base-url', type=str, help="Full ServiceNow URL, incompatible with --instance.")
@click.option('--query', type=str,
              help="ServiceNow instance to use, incompatible with --base-url.")
@click.option('--field', type=str, default=["name"], multiple=True,
              help="fields to retrieve.")
def list_tables(instance, base_url, query, field):
    """Helper script to quickly get all the possible values for a field."""
    if instance and base_url:
        click.echo("Cannot pass both --instance or --base-url")
        sys.exit(1)
    if not (instance or base_url):
        click.echo("Must pass either --instance or --base-url")
        sys.exit(1)
    dc = DescsribeAPIClient(
        endpoint=ServicenowRestEndpoint(
            instance=instance,
            base_url=base_url))
    tables = dc.query(query=query, fields=field)
    click.echo(tables.to_string())


if __name__ == '__main__':
    list_tables()
