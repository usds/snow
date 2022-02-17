from dataclasses import dataclass, field
from typing import List, Dict, Optional
from dacite import from_dict, Config


@dataclass
class StatsReportOperation:
    operation: str
    query: str
    table: str


@dataclass
class StatsReportColumn:
    name: str
    operations: List[StatsReportOperation]


@dataclass
class StatsReportGroupByOptions:
    column_name: str
    fields: Dict[str, List[str]]
    mappings: Dict[str, str] = field(default_factory=dict)


@dataclass
class StatsReport:
    report_name: str
    columns: List[StatsReportColumn]
    groupby_options: Optional[StatsReportGroupByOptions] = None
    # TODO: Change this to just use the column names in order, with a placeholder for
    # the ones that have no operations.
    all_columns: List[str] = field(default_factory=list)


def load_stats_report(report: Dict) -> StatsReport:
    return from_dict(data_class=StatsReport, data=report, config=Config(strict=True))
