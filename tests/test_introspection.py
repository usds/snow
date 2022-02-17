from servicenow_api_tools.introspection import (
    get_mappings_for_table, introspect_api_for_schema,
    id_to_name, name_to_id)
from servicenow_api_tools.schema.schema import merge_fields_catalog
from unittest import TestCase
from .utils import (
    get_local_endpoint, SCHEMAS_DIR,
    write_result_or_print, read_result, get_introspection_parameters)


def test_get_mappings_for_table():
    mappings = get_mappings_for_table(
        "activity_type",
        endpoint=get_local_endpoint(),
        schemas_dir=SCHEMAS_DIR)
    expected = read_result()
    write_result_or_print(mappings, expected)
    TestCase().assertDictEqual(mappings, expected)


def test_name_to_id():
    id = name_to_id(
        "activity_type",
        endpoint=get_local_endpoint(),
        schemas_dir=SCHEMAS_DIR,
        name="Escalate To Manager")
    result = {"id": id}
    expected = read_result()
    write_result_or_print(result, expected)
    TestCase().assertDictEqual(result, expected)


def test_id_to_name():
    name = id_to_name(
        "activity_type",
        endpoint=get_local_endpoint(),
        schemas_dir=SCHEMAS_DIR,
        id="ef9a311e507edf450ce82fe4ecedb231")
    result = {"name": name}
    expected = read_result()
    write_result_or_print(result, expected)
    TestCase().assertDictEqual(result, expected)


def test_introspect_api_for_schema():
    params = get_introspection_parameters()
    result = introspect_api_for_schema(
        endpoint=get_local_endpoint(),
        **params,
        set_unused=True,
        set_references=True,
        set_mappings=True,
        mapping_threshold=4)
    expected = read_result()
    write_result_or_print(result, expected)
    TestCase().assertDictEqual(result, expected)


def test_introspect_api_for_schema_and_update_catalog():
    params = get_introspection_parameters()
    result = introspect_api_for_schema(
        endpoint=get_local_endpoint(),
        **params,
        set_unused=True,
        set_references=True,
        set_mappings=True,
        mapping_threshold=4)
    updated_result = {}
    for table, catalog in result.items():
        updated_result[table] = merge_fields_catalog(table, catalog, SCHEMAS_DIR)
    expected = read_result()
    write_result_or_print(updated_result, expected)
    TestCase().assertDictEqual(updated_result, expected)
