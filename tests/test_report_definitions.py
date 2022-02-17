from servicenow_api_tools.reporting import (
    StatsReport, StatsReportColumn, StatsReportOperation, StatsReportGroupByOptions,
    load_stats_report)
from dataclasses import asdict
from unittest import TestCase

TestCase.maxDiff = None


def test_stats_report_definition_no_groupby():
    report_object = StatsReport(
        report_name="Report Name",
        columns=[
            StatsReportColumn(
                name="Column 1",
                operations=[
                    StatsReportOperation(
                        operation="PLUS",
                        query="active=true",
                        table="activity"
                    )
                ]
            )
        ],
        all_columns=["Column 1"],
    )
    report_dict = {
        'report_name': 'Report Name',
        'columns': [{
            'name': 'Column 1',
            'operations': [{
                'operation': 'PLUS',
                'query': 'active=true',
                'table': 'activity',
            }],
        }],
        'all_columns': ["Column 1"]}
    converted_report_dict = asdict(report_object)
    roundtripped_report_dict = asdict(load_stats_report(report_dict))
    assert roundtripped_report_dict['groupby_options'] is None
    assert converted_report_dict['groupby_options'] is None
    del roundtripped_report_dict['groupby_options']
    del converted_report_dict['groupby_options']
    TestCase().assertDictEqual(roundtripped_report_dict, converted_report_dict)
    TestCase().assertDictEqual(roundtripped_report_dict, report_dict)


def test_stats_report_definition_with_groupby():
    report_object = StatsReport(
        report_name="Report Name",
        columns=[
            StatsReportColumn(
                name="Column 1",
                operations=[
                    StatsReportOperation(
                        operation="PLUS",
                        query="active=true",
                        table="activity"
                    )
                ]
            )
        ],
        groupby_options=StatsReportGroupByOptions(
            column_name="Location",
            fields={
                "activity": [
                    "person.assigned_case.location"
                ],
                "person": [
                    "assigned_case.location"
                ],
                "case": [
                    "location"
                ]
            },
            mappings={
                "Main Office": "The Big Office",
                "Branch Office 1": "The Tiny Office",
                "Branch Office 2": "Lumberg"
            },
        ),
        all_columns=["Column 1"],
    )
    report_dict = {
        'report_name': 'Report Name',
        'columns': [{
            'name': 'Column 1',
            'operations': [{
                'operation': 'PLUS',
                'query': 'active=true',
                'table': 'activity'
            }]
        }],
        "groupby_options": {
            "column_name": "Location",
            "fields": {
                "activity": [
                    "person.assigned_case.location"
                ],
                "person": [
                    "assigned_case.location"
                ],
                "case": [
                    "location"
                ]
            },
            "mappings": {
                "Main Office": "The Big Office",
                "Branch Office 1": "The Tiny Office",
                "Branch Office 2": "Lumberg"
            },
        },
        'all_columns': ["Column 1"]}
    converted_report_dict = asdict(report_object)
    roundtripped_report_dict = asdict(load_stats_report(report_dict))
    TestCase().assertDictEqual(roundtripped_report_dict, converted_report_dict)
    TestCase().assertDictEqual(roundtripped_report_dict, report_dict)
