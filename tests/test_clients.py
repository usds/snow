from unittest import TestCase
from servicenow_api_tools.utils import dataframe_to_api_results
from servicenow_api_tools.clients import TableAPIClient, AggregateAPIClient
from .utils import (
    get_local_endpoint, write_result_or_print, read_result,
    write_count_result_or_print, read_count_result,
    ACTIVITY_TYPE_TEST_SYS_ID,
    PERSON_TEST_SYS_IDS
)


def test_table_client():
    client = TableAPIClient(endpoint=get_local_endpoint())
    result = client.query(
        table="activity",
        query=f"activity_typeIN{ACTIVITY_TYPE_TEST_SYS_ID}",
        fields=["activity_type"],
        limit=10)
    api_result = dataframe_to_api_results(result)
    expected = read_result()
    write_result_or_print(api_result, expected)
    TestCase().assertDictEqual(api_result, expected)


def test_table_client_display_value():
    client = TableAPIClient(endpoint=get_local_endpoint())
    result = client.query(
        table="activity",
        query=f"activity_typeIN{ACTIVITY_TYPE_TEST_SYS_ID}",
        fields=["activity_type"],
        limit=10,
        display_value="true",
        unpack_link_fields=False)
    api_result = dataframe_to_api_results(result)
    expected = read_result()
    write_result_or_print(api_result, expected)
    TestCase().assertDictEqual(api_result, expected)


def test_aggregate_client():
    ac = AggregateAPIClient(endpoint=get_local_endpoint())
    result = ac.query(
        table="activity", group_by=["activity_type"],
        query=f"person={PERSON_TEST_SYS_IDS[0]}^active=true", display_value=True)
    expected = read_count_result()
    write_count_result_or_print(result, expected)
    TestCase().assertDictEqual(expected, result)
