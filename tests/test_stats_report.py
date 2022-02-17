import os
import pandas as pd
from servicenow_api_tools.reporting import run_stats_report
from servicenow_api_tools.reporting.build import get_debug_mode, set_debug_mode
from .utils import get_local_endpoint, write_result_dataframe_or_print, read_result_dataframe

DATA_TEST_DEFINITION_FILE = os.path.join(
    os.path.dirname(__file__), "reporting", "stats-report-definition.json")


def test_stats_report():
    (report_name, results) = run_stats_report(
        stats_report_definition_file=DATA_TEST_DEFINITION_FILE,
        endpoint=get_local_endpoint())
    # This breaks the test because it changes
    results['Date (UTC)'] = 'X'
    expected = read_result_dataframe()
    write_result_dataframe_or_print(result=results, expected=expected)
    pd.testing.assert_frame_equal(results, expected)
    assert report_name == "Location Breakdown"


def test_stats_report_debug_mode():
    set_debug_mode(True)
    assert get_debug_mode()
    (report_name, results) = run_stats_report(
        stats_report_definition_file=DATA_TEST_DEFINITION_FILE,
        endpoint=get_local_endpoint())
    # This breaks the test because it changes
    results['Date (UTC)'] = 'X'
    expected = read_result_dataframe()
    write_result_dataframe_or_print(result=results, expected=expected)
    pd.testing.assert_frame_equal(results, expected)
    assert report_name == "Location Breakdown"
