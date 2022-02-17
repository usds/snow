from dataclasses import dataclass
from typing import List, Callable, Optional, Tuple
import pandas as pd
import functools


@dataclass
class ReportColumn:
    name: str
    data: pd.DataFrame
    post_filter: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None


def _filter_dates(df: pd.DataFrame, date_field: str, start: str, end: str):
    """
    Returns everything NOT in the given range.
    """
    # Ignore errors, because the whole point is to get bad dates
    df[date_field + "_temp"] = pd.to_datetime(df[date_field], format="%Y-%m-%d", errors='coerce')
    converted_date_field = date_field + "_temp"
    df_null = df[pd.isnull(df[converted_date_field])]
    df_non_null = df.dropna()
    df_out_of_range = df_non_null[(start > df_non_null[converted_date_field])
                                  | (df_non_null[converted_date_field] > end)]
    df = pd.concat([df_null, df_out_of_range])
    return df.drop(converted_date_field, axis=1)


def filter_dates_func(date_field: str, start: str, end: str):
    """
    Returns everything NOT in the given range.
    """
    return functools.partial(_filter_dates, date_field=date_field, start=start, end=end)


def _filter_dups(df: pd.DataFrame, field: str):
    """
    Returns only rows that have duplicate values for field in the given data frame.
    """
    sizes = df.groupby([field], as_index=False).size()
    df = df[df[field].isin(sizes[sizes['size'] > 1][field])]
    return df


def filter_dups_func(field: str):
    """
    Returns only rows that have duplicate values for field.
    """
    return functools.partial(_filter_dups, field=field)


def build_data_quality_counts_table(
        report_name: str,
        columns: List[ReportColumn]) -> List[Tuple[str, int]]:
    all_counts: List[Tuple[str, int]] = []
    for column in columns:
        final_data = column.data
        if column.post_filter:
            final_data = column.post_filter(column.data)
        if final_data.empty:
            all_counts.append((column.name, 0))
            continue
        all_counts.append((column.name, final_data.shape[0]))
    return all_counts
