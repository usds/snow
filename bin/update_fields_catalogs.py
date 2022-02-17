from servicenow_api_tools.clients.endpoint import ServicenowRestEndpoint
from servicenow_api_tools.introspection.introspection import get_updated_fields_catalog
from servicenow_api_tools.schema.schema import update_fields_catalog
from typing import Dict
import click
import json


def get_in_use_and_accessible_tables(tables_data: Dict):
    in_use_and_accessible_tables = []
    for table_name, table_info in tables_data.items():
        if "in_use" in table_info and not table_info["in_use"]:
            continue
        if "have_permission" in table_info and not table_info["have_permission"]:
            continue
        in_use_and_accessible_tables.append(table_name)
    return in_use_and_accessible_tables


@click.command()
@click.option('--instance', type=str,
              help="ServiceNow instance to use, incompatible with --base-url.")
@click.option('--base-url', type=str, help="Full ServiceNow URL, incompatible with --instance.")
@click.option('--tables-file', required=True, type=click.File('r'))
@click.option('--extra-filters-file', required=False, type=click.File('r'))
@click.option('--extra-mapping-fields-file', required=False, type=click.File('r'))
@click.option('--table-display-values-file', required=False, type=click.File('r'))
@click.option('--enum-tables-file', required=False, type=click.File('r'))
@click.option('--catalog-directory', required=True,
              type=click.Path(exists=True, file_okay=False, dir_okay=True))
def update_fields_catalogs(
        instance, base_url, tables_file, extra_filters_file,
        extra_mapping_fields_file, catalog_directory,
        table_display_values_file, enum_tables_file):
    """Helper script to update the fields catalogs for the server tables.

    This is so we have a place for field-level documentation that we can verify is still in sync
    with the actual system.

    We can also mark fields as "unused" if they are never set to anything, and verify that is the
    case as well.
    """
    tables = get_in_use_and_accessible_tables(json.loads(tables_file.read()))
    extra_filters = {}
    if extra_filters_file:
        extra_filters = json.loads(extra_filters_file.read())
    extra_mapping_fields = {}
    if extra_mapping_fields_file:
        extra_mapping_fields = json.loads(extra_mapping_fields_file.read())
    table_display_values = {}
    if table_display_values_file:
        table_display_values = json.loads(table_display_values_file.read())
    enum_tables = []
    if enum_tables_file:
        enum_tables = json.loads(enum_tables_file.read())
    endpoint = ServicenowRestEndpoint(instance=instance, base_url=base_url)
    schemas = get_updated_fields_catalog(
        catalog_directory, endpoint, tables, extra_filters, extra_mapping_fields,
        table_display_values, enum_tables)
    for table, catalog in schemas.items():
        update_fields_catalog(table, catalog, catalog_directory)


if __name__ == '__main__':
    update_fields_catalogs()
