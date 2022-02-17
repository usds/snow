from servicenow_api_tools.mock_api_server import ServicenowRestEndpointLocalDataset
from servicenow_api_tools.reporting.build import get_debug_mode
import pandas as pd
from typing import Dict, Any
from unittest import TestCase
import hashlib
import inspect
import json
import os

SCHEMAS_DIR = os.path.join(os.path.dirname(__file__), "schemas")
TEST_DATASET = os.path.join(os.path.dirname(__file__), "dataset")
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")

PERSON_TEST_SYS_IDS = ["8b710d7deab278bce168a3eafc0ee6fe", "c946bd9009a71d223ea603e44654ddbd"]
ACTIVITY_TYPE_TEST_SYS_ID = "aac8c3d8a1266c50ba7dc49f23ebe9da"

WRITE_RESULT = False


def load_introspection_file(file_name: str) -> Dict:
    result_file = os.path.join(SCHEMAS_DIR, "introspection", file_name)
    with open(result_file, 'r') as f:
        return json.loads(f.read())


def get_introspection_parameters() -> Dict:
    return {
        "tables": load_introspection_file("tables.json"),
        "filters": load_introspection_file("filters.json"),
        "extra_mapping_fields": load_introspection_file("extra-mapping-fields.json"),
        "table_display_values": load_introspection_file("table-display-values.json"),
        "enum_tables": load_introspection_file("enum-tables.json"),
    }


def get_local_endpoint() -> ServicenowRestEndpointLocalDataset:
    return ServicenowRestEndpointLocalDataset(path=TEST_DATASET, schema_dir=SCHEMAS_DIR)


def write_result_dataframe_or_print(result: pd.DataFrame, expected: pd.DataFrame):
    caller_name = inspect.stack()[1].function
    if WRITE_RESULT:
        if get_debug_mode():
            result_file = os.path.join(RESULTS_DIR, f'{caller_name}_debug.csv')
        else:
            result_file = os.path.join(RESULTS_DIR, f'{caller_name}.csv')
        result.to_csv(result_file, index=False)
    else:
        print("EXPECTED:")
        print(expected.to_markdown())
        print("RESULT:")
        print(result.to_markdown())
        pass


def read_result_dataframe() -> pd.DataFrame:
    if WRITE_RESULT:
        return pd.DataFrame()
    else:
        caller_name = inspect.stack()[1].function
        if get_debug_mode():
            result_file = os.path.join(RESULTS_DIR, f'{caller_name}_debug.csv')
        else:
            result_file = os.path.join(RESULTS_DIR, f'{caller_name}.csv')
        # Since sex can be "N/A":
        # https://stackoverflow.com/questions/33952142/prevent-pandas-from-interpreting-na-as-nan-in-a-string
        return pd.read_csv(result_file, keep_default_na=False)


def write_result_or_print(result: Dict, expected: Dict):
    """
    A function that's at the bottom of every test, to make it easy to print the expected result when
    refactoring, and write it to the expected result file once it is confirmed to be correct.
    """
    if WRITE_RESULT:
        caller_name = inspect.stack()[1].function
        result_file = os.path.join(RESULTS_DIR, f'{caller_name}.json')
        with open(result_file, 'w') as f:
            f.write(json.dumps(result, indent=4, sort_keys=True))
    else:
        print("EXPECTED:")
        print(expected)
        print("RESULT:")
        print(result)
        pass


def read_result() -> Dict:
    # Assuming the test will fail anyway, don't try to read a result file if we are writing it out.
    if WRITE_RESULT:
        return {"results": ["placeholder"]}
    caller_name = inspect.stack()[1].function
    result_file = os.path.join(RESULTS_DIR, f'{caller_name}.json')
    with open(result_file, 'r') as f:
        return json.loads(f.read())


def write_count_result_or_print(result: Dict, expected: Dict):
    if WRITE_RESULT:
        caller_name = inspect.stack()[1].function
        result_file = os.path.join(RESULTS_DIR, f'{caller_name}.json')
        with open(result_file, 'w') as f:
            f.write(json.dumps(result, indent=4, sort_keys=True))
    else:
        print("EXPECTED:")
        print(json.dumps(expected, indent=4, sort_keys=True))
        print("RESULT:")
        print(json.dumps(result, indent=4, sort_keys=True))
        pass


def read_count_result() -> Dict:
    # Assuming the test will fail anyway, don't try to read a result file if we are writing it out.
    if WRITE_RESULT:
        return {"results": ["placeholder"]}
    caller_name = inspect.stack()[1].function
    result_file = os.path.join(RESULTS_DIR, f'{caller_name}.json')
    with open(result_file, 'r') as f:
        return json.loads(f.read())


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
