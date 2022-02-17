from servicenow_api_tools.clients.querybuilder import TableQueryBuilder, AggregateQueryBuilder
from typing import Dict, Any
from unittest import TestCase
import hashlib
import json
import os
import servicenow_api_tools.mock_api_server.query as query
from .utils import (
    SCHEMAS_DIR, TEST_DATASET, write_result_or_print, read_result,
    write_count_result_or_print, read_count_result,
    ACTIVITY_TYPE_TEST_SYS_ID, PERSON_TEST_SYS_IDS)

TestCase.maxDiff = None

TEST_RESULTS_DIR = os.path.join(
    os.path.dirname(__file__), "data", "synthetic_data_query", "results")


def _assert_dicts_equal(result: Dict, expected: Dict):
    # Handy to have as a helper if I want to print/check anything else before comparing
    def _hash_dict(row: Dict[str, Any]) -> str:
        """MD5 hash of a dictionary."""
        dhash = hashlib.md5()
        # We need to sort arguments so {'a': 1, 'b': 2} is
        # the same as {'b': 2, 'a': 1}
        encoded = json.dumps(row, sort_keys=True).encode()
        dhash.update(encoded)
        return dhash.hexdigest()
    result['result'] = sorted(result['result'], key=_hash_dict)
    expected['result'] = sorted(expected['result'], key=_hash_dict)
    TestCase().assertDictEqual(result, expected)


def test_single_matcher_table_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    table_query = TableQueryBuilder(
        table="person",
        query=f'sys_id={sys_id}',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_single_dotted_matcher_table_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    table_query = TableQueryBuilder(
        table="activity",
        query=f"person.sys_id={sys_id}",
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_and_matcher_table_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    table_query = TableQueryBuilder(
        table="activity",
        query=f'activity_type={ACTIVITY_TYPE_TEST_SYS_ID}^ORperson.sys_id={sys_id}',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_or_matcher_table_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    table_query = TableQueryBuilder(
        table="activity",
        query=f'activity_type={ACTIVITY_TYPE_TEST_SYS_ID}^ORperson.sys_id={sys_id}',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_not_equal_matcher_table_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    table_query = TableQueryBuilder(
        table="person",
        query=f'sys_id!={sys_id}',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_not_equal_matcher_limit_offset_table_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    table_query = TableQueryBuilder(
        table="person",
        query=f'sys_id!={sys_id}',
        fields=[],
        limit=1,
        offset=1,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_fields_table_query():
    table_query = TableQueryBuilder(
        table="activity",
        query='activity_type={ACTIVITY_TYPE_TEST_SYS_ID}',
        fields=['activity_type'],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_isnotempty_matcher_table_query():
    table_query = TableQueryBuilder(
        table="person",
        query='sys_idISNOTEMPTY',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_isempty_matcher_table_query():
    table_query = TableQueryBuilder(
        table="person",
        query='sys_idISEMPTY',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_in_matcher_table_query():
    sys_ids = PERSON_TEST_SYS_IDS
    table_query = TableQueryBuilder(
        table="person",
        query=f'sys_idIN{",".join(sys_ids)}',
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_date_between_matcher_table_query():
    table_query = TableQueryBuilder(
        table="person",
        query="date_of_birthBETWEEN1950-10-01@2021-10-25",
        fields=[],
        limit=None,
        offset=None,
        display_value="true")
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(table_query))
    expected = read_result()
    write_result_or_print(result, expected)
    _assert_dicts_equal(result, expected)


def test_no_matcher_stats_query():
    stats_query = AggregateQueryBuilder(
        table="activity",
        group_by=[],
        query="",
        having="",
        display_value="true",
        return_count=True)
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(stats_query))
    expected = read_count_result()
    write_count_result_or_print(result, expected)
    TestCase().assertDictEqual(expected, result)


def test_single_matcher_stats_query():
    sys_id = PERSON_TEST_SYS_IDS[0]
    stats_query = AggregateQueryBuilder(
        table="activity",
        query=f"person.sys_id={sys_id}",
        group_by=[],
        having="",
        display_value="true",
        return_count=True)
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(stats_query))
    expected = read_count_result()
    write_count_result_or_print(result, expected)
    TestCase().assertDictEqual(expected, result)


def test_group_by_stats_query():
    stats_query = AggregateQueryBuilder(
        table="activity",
        group_by=["activity_type"],
        query="",
        having="",
        display_value="true",
        return_count=True)
    runner = query.LocalDatasetQueryRunner(TEST_DATASET, SCHEMAS_DIR)
    result = runner.query(str(stats_query))
    expected = read_count_result()
    write_count_result_or_print(result, expected)
    TestCase().assertDictEqual(expected, result)
