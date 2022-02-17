import pandas as pd
from servicenow_api_tools.reporting.data_quality import (
    filter_dates_func, filter_dups_func, ReportColumn, build_data_quality_counts_table)
from servicenow_api_tools import operators as ops
from servicenow_api_tools.mock_api_server import ServicenowRestEndpointLocalDataset
from servicenow_api_tools.clients import TableAPIClient
from .utils import get_local_endpoint, write_result_dataframe_or_print, read_result_dataframe


def _run_example_data_quality_report(endpoint: ServicenowRestEndpointLocalDataset):
    tc = TableAPIClient(endpoint=endpoint)

    return build_data_quality_counts_table("Data Quality Report", [
        ReportColumn(
            "Empty First Name",
            tc.query(table="person",
                     fields=["sys_id"],
                     query=ops.AND(
                         "first_nameISEMPTY",
                     ),
                     ),
            None),
        ReportColumn(
            "Duplicate Case Numbers",
            tc.query(table="case",
                     fields=["sys_id", "number"], query=ops.AND(),
                     ),
            filter_dups_func("number")),
        ReportColumn(
            "Invalid Date of Birth",
            tc.query(table="person",
                     fields=["sys_id", "date_of_birth"], query=ops.AND(),
                     ),
            filter_dates_func(
                "date_of_birth",
                pd.Timestamp(1910, 1, 1).strftime("%Y-%m-%d"),
                pd.Timestamp.now().strftime("%Y-%m-%d")))])


def test_data_quality_report_v1():
    result = _run_example_data_quality_report(endpoint=get_local_endpoint())
    for issue, rows_with_issue in result:
        assert rows_with_issue is not None
    result_df = pd.DataFrame(columns=["Issue", "Count"], data=result)
    expected = read_result_dataframe()
    write_result_dataframe_or_print(result=result_df, expected=expected)
    pd.testing.assert_frame_equal(result_df, expected)
