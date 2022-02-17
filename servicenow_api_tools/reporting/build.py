from servicenow_api_tools.clients import AggregateAPIClient
from servicenow_api_tools.clients import ServicenowRestEndpoint
from servicenow_api_tools.reporting.definitions import (
    StatsReportColumn, StatsReportOperation, StatsReport)
from typing import List, Dict, Tuple
import datetime
import pandas as pd

DEBUG_MODE = False


def set_debug_mode(enabled: bool):
    global DEBUG_MODE
    DEBUG_MODE = enabled


def get_debug_mode() -> bool:
    return DEBUG_MODE


def _move_column_to_first_column(df: pd.DataFrame, column: str) -> pd.DataFrame:
    cols = list(df)
    cols.insert(0, cols.pop(cols.index(column)))
    return df.loc[:, cols]


def _convert_non_label_columns_to_int(df: pd.DataFrame, label_column: str) -> pd.DataFrame:
    new_df = df.copy()
    new_df = _move_column_to_first_column(new_df, label_column)
    new_df = new_df.fillna(0)
    cols = new_df.columns[1:]
    new_df[cols] = new_df[cols].astype(int)
    return new_df


def _sort_by_group_name_column(df: pd.DataFrame, group_name: str) -> pd.DataFrame:
    return df.sort_values(by=group_name).reset_index(drop=True)


def _add_totals_row(df: pd.DataFrame) -> pd.DataFrame:
    new_df = df.copy()
    new_df = new_df.append(new_df.sum(numeric_only=True, axis=0), ignore_index=True)
    new_df.iloc[-1, 0] = 'Total'
    return new_df


def process_column_results_ungrouped(
        results: List[Tuple[StatsReportColumn, List[Tuple[str, Dict]]]]) -> pd.DataFrame:

    def process_column_result(
            column: StatsReportColumn, query_results: List[Tuple[str, Dict]]) -> pd.DataFrame:
        column_heading = f"{column.name}"
        count = 0
        column_result = {}
        column_result["Count"] = "Total"
        for column_op, (op, query_result) in zip(
                column.operations,
                query_results):
            assert op in ["PLUS", "MINUS"]
            if DEBUG_MODE:
                column_result[f'{column_heading}\n{column_op.operation}\n{column_op.query}'] = (
                    str(int(query_result["result"]["stats"]["count"])))
            if op == "PLUS":
                count = count + int(query_result["result"]["stats"]["count"])
            else:
                count = count - int(query_result["result"]["stats"]["count"])
        column_result[column_heading] = str(count)

        return pd.DataFrame([column_result], dtype="object")

    result = pd.DataFrame(["Total"])
    result = result.set_axis(["Count"], axis=1)
    for column, query_results in results:
        column_dataframe = process_column_result(column, query_results)
        result = pd.merge(result, column_dataframe, on="Count", how="left")
        # If no results came back for a particular count, they will show up as "nan" after the merge
        # unless we do this.
        result = result.fillna(0)
    result = _convert_non_label_columns_to_int(result, label_column="Count")
    return result


def process_column_results_grouped(
        results: List[Tuple[StatsReportColumn, List[Tuple[str, Dict]]]],
        group_name: str,
        group_mappings: Dict[str, str] = None) -> pd.DataFrame:

    def merge_and_apply_operation(
            op: str, left: pd.DataFrame, right: pd.DataFrame, on: str,
            op_column: str) -> pd.DataFrame:
        df = pd.merge(left, right, on=on, how="outer")
        df = df.fillna(0)
        op_column_x = f'{op_column}_x'
        op_column_y = f'{op_column}_y'
        assert op in ["PLUS", "MINUS"]
        if op == "PLUS":
            df[op_column] = df[op_column_x].astype(int) + df[op_column_y].astype(int)
        else:
            df[op_column] = df[op_column_x].astype(int) - df[op_column_y].astype(int)
        df = df.drop(op_column_x, axis=1)
        df = df.drop(op_column_y, axis=1)
        return df

    result = pd.DataFrame(columns=[group_name])
    for column, query_results in results:
        column_heading = f"{column.name}"
        column_dataframe = pd.DataFrame(columns=[group_name, column_heading])

        for column_op, (op, query_result) in zip(
                column.operations,
                query_results):
            restructured_query_results = []
            if DEBUG_MODE:
                debug_results = []
                if len(query_result['result']) == 0:
                    debug_heading = (
                        'QUERY RETURNED NO RESULTS, INVALID?: '
                        f'{column_heading}\n{column_op.operation}\n{column_op.query}')
                else:
                    debug_heading = f'{column_heading}\n{column_op.operation}\n{column_op.query}'
            for counts in query_result['result']:
                groupby_result = counts['groupby_fields'][0]['value']
                column_result = {}
                column_result[group_name] = groupby_result if groupby_result else "(EMPTY)"
                column_result[column_heading] = counts['stats']['count']
                restructured_query_results.append(column_result)
                if DEBUG_MODE:
                    debug_result = {}
                    debug_result[group_name] = groupby_result if groupby_result else "(EMPTY)"
                    debug_result[debug_heading] = counts['stats']['count']
                    debug_results.append(debug_result)
            if DEBUG_MODE:
                if debug_results:
                    debug_dataframe = pd.DataFrame(debug_results, dtype="object")
                else:
                    debug_dataframe = pd.DataFrame(
                        columns=[group_name, debug_heading], dtype="object")
                column_dataframe = pd.merge(
                    debug_dataframe, column_dataframe, on=group_name, how="outer")

            if restructured_query_results:
                column_dataframe = merge_and_apply_operation(
                    op=op,
                    left=column_dataframe,
                    right=pd.DataFrame(restructured_query_results, dtype="object"),
                    on=group_name,
                    op_column=column_heading)
        result = pd.merge(result, column_dataframe, on=group_name, how="outer")
        # If no results came back for a particular count, they will show up as "nan" after the merge
        # unless we do this.
        result[column_heading] = result[column_heading].fillna(0)

    if group_mappings is not None:
        group_mappings_df = pd.DataFrame(columns=[group_name], data=group_mappings.values())
        result[group_name] = result[group_name].replace(group_mappings)
        result = pd.merge(result, group_mappings_df, on=group_name, how="outer")
        result[column_heading] = result[column_heading].fillna(0)
    result = _convert_non_label_columns_to_int(result, label_column=group_name)
    result = _add_totals_row(result)
    # Adding the totals row converts everything to float, see:
    # https://github.com/pandas-dev/pandas/issues/26219
    result = _convert_non_label_columns_to_int(result, label_column=group_name)
    result = _sort_by_group_name_column(result, group_name)
    return result


def process_column_results(results: List[Tuple[StatsReportColumn, List[Tuple[str, Dict]]]],
                           group_name: str = "",
                           group_mappings: Dict[str, str] = None) -> pd.DataFrame:
    if group_name:
        return process_column_results_grouped(results, group_name, group_mappings)
    else:
        return process_column_results_ungrouped(results)


def run_query(
        endpoint: ServicenowRestEndpoint,
        column: StatsReportColumn,
        groupby_fields: Dict[str, List[str]] = {}) -> List[Tuple[str, Dict]]:
    ac = AggregateAPIClient(endpoint=endpoint)
    if groupby_fields:
        results = []
        for op in column.operations:
            result = ac.query(table=op.table, query=op.query,
                              group_by=groupby_fields[op.table],
                              display_value="true")
            results.append((op.operation, result))
        return results
    else:
        results = []
        for op in column.operations:
            result = ac.query(table=op.table, query=op.query, display_value="true")
            results.append((op.operation, result))
        return results


def build_report(
        endpoint: ServicenowRestEndpoint,
        report: StatsReport) -> pd.DataFrame:
    query_results = []
    if report.groupby_options:
        for column in report.columns:
            query_results.append(
                (column,
                 run_query(endpoint, column,
                           groupby_fields=report.groupby_options.fields)))
        return process_column_results(
            query_results,
            group_name=report.groupby_options.column_name,
            group_mappings=report.groupby_options.mappings)
    else:
        for column in report.columns:
            query_results.append((column, run_query(endpoint, column)))
        return process_column_results(query_results)


def build_per_day_report(
        endpoint: ServicenowRestEndpoint,
        query: str, time_field: str, table: str) -> pd.DataFrame:
    past_week_days = [
        (datetime.datetime.now() - datetime.timedelta(i)).strftime('%Y-%m-%d') for i in range(0, 7)]
    columns = []
    for past_week_day in past_week_days:
        columns.append(StatsReportColumn(
            past_week_day,
            [StatsReportOperation("PLUS", f"{query}^{time_field}={past_week_day}", table)]))
    return build_report(
        endpoint,
        StatsReport(
            report_name="Last 7 Days ({query})",
            columns=columns))
