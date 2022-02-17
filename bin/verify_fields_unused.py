import click
from servicenow_api_tools.schema.schema import load_fields_catalog, update_fields_catalog
from servicenow_api_tools.clients import ServicenowRestEndpoint, AggregateAPIClient


@click.command()
@click.option('--instance', type=str,
              help="ServiceNow instance to use, incompatible with --base-url.")
@click.option('--base-url', type=str, help="Full ServiceNow URL, incompatible with --instance.")
@click.option('--exit-on-fail/--no-exit-on-fail',
              help="Whether to exit if a field didn't match what should be in the catalog",
              default=True)
@click.option('--tables', required=True, type=str, help="Comma separated list of tables to verify.")
@click.option('--catalog-directory', required=True,
              type=click.Path(exists=True, file_okay=False, dir_okay=True))
def verify_fields_unused(instance, base_url, exit_on_fail, tables, catalog_directory):
    """Helper script to scan the fields catalogs and verify they are unused."""
    ac = AggregateAPIClient(
        endpoint=ServicenowRestEndpoint(
            instance=instance,
            base_url=base_url))

    def get_count(result: dict) -> int:
        if not result:
            raise Exception("No result?")

        if 'error' in result:
            raise Exception(f"Got error: {result}")

        return int(result['result']['stats']['count'])

    def is_unused(table: str, field: str) -> bool:
        result = ac.query(table=table, query=f"{field}ISNOTEMPTY")
        return get_count(result) == 0

    for table in tables.split(","):
        catalog = load_fields_catalog(table, catalog_directory)
        for field, field_info in catalog.items():
            if field == 'sys_tags':
                # This returns a strange result, no count of zero even, just an empty result object.
                # Just skip it in this check.
                continue

            if 'unused' in field_info and field_info['unused']:
                if is_unused(table, field):
                    click.echo(f"\nField {table}/{field} is unused, as expected.\n")
                else:
                    msg = f"\nField {table}/{field} is actually in use!\n"
                    if exit_on_fail:
                        assert False, msg
                    else:
                        click.echo(msg)
                    click.echo(f"\nMarking field {table}/{field} as in use!\n")
                    catalog[field]["unused"] = False
            else:
                if is_unused(table, field):
                    msg = f"\nField {table}/{field} is actually unused!\n"
                    if exit_on_fail:
                        assert False, msg
                    else:
                        click.echo(msg)
                    click.echo(f"\nMarking field {table}/{field} as unused!\n")
                    catalog[field]["unused"] = True
                else:
                    click.echo(f"\nField {table}/{field} is in use, as expected.\n")
        update_fields_catalog(table, catalog, catalog_directory)


if __name__ == '__main__':
    verify_fields_unused()
