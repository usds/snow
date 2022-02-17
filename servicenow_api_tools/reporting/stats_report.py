from servicenow_api_tools.clients import ServicenowRestEndpoint
from servicenow_api_tools.reporting import build_report
from servicenow_api_tools.reporting.build import get_debug_mode
from servicenow_api_tools.reporting.definitions import load_stats_report
from typing import Tuple, List
import datetime
import json
import pandas as pd


def run_stats_report(
        stats_report_definition_file: str,
        endpoint: ServicenowRestEndpoint) -> Tuple[str, pd.DataFrame]:
    definition = None
    with open(stats_report_definition_file, 'r') as f:
        definition = load_stats_report(json.loads(f.read()))
    results = build_report(
        endpoint=endpoint,
        report=definition)

    def add_date_to_first_column(df: pd.DataFrame) -> pd.DataFrame:
        date = datetime.datetime.utcnow().isoformat()
        df["Date (UTC)"] = date
        cols = list(df)
        cols.insert(0, cols.pop(cols.index("Date (UTC)")))
        return df.loc[:, cols]

    def handle_all_columns(df: pd.DataFrame, all_columns: List[str],
                           group_name: str) -> pd.DataFrame:
        columns = [group_name] + all_columns
        for column in columns:
            if column not in df.columns:
                df[column] = "TBD"
        for column in df.columns:
            assert column in columns, (
                f"Found unexpected column not in the full column list: {column}")
        df = df.reindex(columns, axis=1)
        return df

    # In debug mode we don't care about things without queries and sorting according to a specific
    # final format.
    if definition.all_columns and not get_debug_mode():
        group_name = (
            definition.groupby_options.column_name
            if definition.groupby_options is not None else "Count")
        results = handle_all_columns(results, definition.all_columns, group_name)
    results = add_date_to_first_column(results)
    return (definition.report_name, results)
