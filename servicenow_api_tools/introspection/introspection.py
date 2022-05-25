from typing import Dict, List, Union
from servicenow_api_tools.clients.endpoint import ServicenowRestEndpoint
from servicenow_api_tools.clients.clients import AggregateAPIClient, TableAPIClient
from servicenow_api_tools.schema.schema import load_fields_catalog
import pandas as pd
import logging
from servicenow_api_tools.schema.schema import merge_fields_catalog

logging.basicConfig()
logger = logging.getLogger(__name__)


def introspect_api_for_schema(
        endpoint: ServicenowRestEndpoint,
        tables: List[str],
        filters: Dict[str, str],
        extra_mapping_fields: Dict[str, List[str]],
        table_display_values: Dict[str, str],
        enum_tables: List[str],
        set_unused: bool = True,
        set_references: bool = True,
        set_mappings: bool = True,
        mapping_threshold: int = 25) -> Dict[str, Dict]:
    """
    Given an endpoint and a set of tables, pulls as much information as it can about those tables.

    It always at least pull one record to get the list of fields, and optionally (on by default)
    will detect other things, like whether the field is unused, whether it's a link field, and
    whether it has a small set of enum-like values.

    The filters and extra_mapping_fields arguments are for situations where you don't want to use
    the entire dataset (for example if you have a way to mark records as "archived"), and for
    generating mappings for fields that aren't "link" fields (by default mappings are generated only
    for "link" fields).
    """
    ac = AggregateAPIClient(endpoint=endpoint)
    tc = TableAPIClient(endpoint=endpoint)

    def _is_unused(table: str, field: str, filters: Dict[str, str]) -> bool:
        result = ac.query(table=table, query=f"{field}ISNOTEMPTY")
        return int(result['result']['stats']['count']) == 0

    def _total_rows(table: str, filters: Dict[str, str]) -> int:
        result = ac.query(table=table)
        return int(result['result']['stats']['count'])

    def _get_valid_values(table: str, field: str, filters: Dict[str, str]) -> List[str]:
        result = _run_count_query(table=table, group_by=[field], filters=filters)
        return [group['groupby_fields'][0]['display_value'] for group in result['result']]

    def _get_mappings(table: str, field: str, filters: Dict[str, str]) -> Dict[str, str]:
        result = _run_count_query(table=table, group_by=[field], filters=filters)
        return {group['groupby_fields'][0]['value']: group['groupby_fields'][0]['display_value']
                for group in result['result']}

    def _get_enum_field_values(table: str, field: str, filters: Dict[str, str]) -> List[str]:
        result = _run_count_query(table=table, group_by=[field], filters=filters)
        non_unique = [group for group in result['result'] if group['stats']['count'] != 1]
        assert len(non_unique) == 0, f"Duplicates in enum table: {non_unique}"
        return [group['groupby_fields'][0]['display_value'] for group in result['result']]

    def _run_query(table: str, limit: Union[int, None], filters: Dict[str, str],
                   fields: List[str]) -> pd.DataFrame:
        if table in filters:
            result = tc.query(table=table, limit=limit, query=filters[table],
                              unpack_link_fields=False, fields=fields)
        else:
            result = tc.query(table=table, limit=limit, unpack_link_fields=False, fields=fields)
        return result

    def _run_count_query(table: str, filters: Dict[str, str], group_by: List[str]) -> Dict:
        if table in filters:
            result = ac.query(table=table, query=filters[table], group_by=group_by,
                              display_value="all")
        else:
            result = ac.query(table=table, group_by=group_by, display_value="all")
        return result

    schemas = {}
    for table in tables:
        # XXX: If a field is a link field, does that imply every row has "link" set on it, or can
        # there be rows that do not have "link" set? If that's the case, maybe we should fetch more
        # rows to be sure we actually found all the link fields. Also this should probably allow the
        # user to pass extra link fields explicitly, just in case.
        result = _run_query(table=table, limit=10, filters=filters, fields=[])
        logger.debug(f'Representative row: {table}:{result.to_markdown()}')

        fields = list(result.columns.values)
        logger.debug(f'Fields: {table}:{fields}')

        catalog: Dict[str, Dict] = {field: {} for field in fields}

        if table not in table_display_values:
            logger.info(f"No display value for {table} in {table_display_values}, using sys_id")
            table_display_values[table] = "sys_id"

        for field, field_info in catalog.items():

            # Set table display values for links from other tables
            if field == table_display_values[table]:
                catalog[field]['is_table_display_value'] = True

                # Check if this is an enum table
                if table in enum_tables:
                    total_rows = _total_rows(table, filters)
                    if total_rows > 1000:
                        # I'm assuming this is a programming error to say something is an enum table
                        # when it's not
                        raise Exception(
                            f"Table {table} has {total_rows} rows, but is set as an enum table")
                    catalog[field]['valid_values'] = _get_enum_field_values(table, field, filters)

            if set_unused:
                if field == 'sys_tags':
                    # This returns a strange result, no count of zero even, just an empty result
                    # object. Just skip it in this check.
                    pass
                else:
                    if _is_unused(table, field, filters):
                        catalog[field]["unused"] = True
            if set_references:
                if "link" in result[field][0]:
                    link = result[field][0]["link"]
                    if link.startswith("https"):
                        link_table = link.split("/")[-2]
                        catalog[field]["reference"] = link_table
                    else:
                        assert link.startswith("sys_"), f"{link}"
            if set_mappings:
                # Link fields are not "mappings", instead their display name has a set of "valid
                # values", which signals to every linking table that it's an enum table.
                #
                # So these are specifically for non link fields.
                if table in extra_mapping_fields and field in extra_mapping_fields[table]:
                    catalog[field]["mappings"] = _get_mappings(table, field, filters)
        schemas[table] = catalog
    return schemas


def get_mappings_for_table(
        table: str,
        endpoint: ServicenowRestEndpoint,
        schemas_dir: str) -> Dict:
    catalog = load_fields_catalog(table, schemas_dir)
    display_value_fields = []
    for field, field_info in catalog.items():
        if 'is_table_display_value' in field_info and field_info['is_table_display_value']:
            display_value_fields.append(field)
    assert len(display_value_fields) == 1, (
        f"Table must have exactly one display value, found: {display_value_fields}")
    tc = TableAPIClient(endpoint=endpoint)
    rows = tc.query(
        table=table,
        fields=["sys_id", display_value_fields[0]])
    result = {}
    for row in rows.to_dict('records'):
        assert type(row) is dict
        result[row['sys_id']] = row[display_value_fields[0]]
    return result


def get_updated_fields_catalog(catalog_directory: str, *args, **kwargs) -> Dict[str, Dict]:
    schemas = introspect_api_for_schema(*args, **kwargs)
    updated_schema = {}
    for table, catalog in schemas.items():
        updated_schema[table] = merge_fields_catalog(table, catalog, catalog_directory)
    return updated_schema


def name_to_id(
        table: str,
        endpoint: ServicenowRestEndpoint,
        schemas_dir: str,
        name: str) -> str:
    mappings = get_mappings_for_table(table, endpoint, schemas_dir)
    for sys_id, found_display_name in mappings.items():
        if name == found_display_name:
            return sys_id
    raise Exception(f"Could not map {name} to sys_id")


def id_to_name(
        table: str,
        endpoint: ServicenowRestEndpoint,
        schemas_dir: str,
        id: str) -> str:
    mappings = get_mappings_for_table(table, endpoint, schemas_dir)
    return mappings[id]
