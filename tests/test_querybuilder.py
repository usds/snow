from servicenow_api_tools.clients import AggregateQueryBuilder, TableQueryBuilder


def test_aggregate_query():
    result = (
        "/api/now/stats/activity?"
        "sysparm_group_by=person.primary_case,person.sys_id,activity_type&"
        "sysparm_query=activity_type=0123456789abcdef&"
        "sysparm_count=true&"
        "sysparm_display_value=true&"
        "sysparm_having=count^person.sys_id^!=^1")

    built = AggregateQueryBuilder(
        table="activity",
        group_by=["person.primary_case", "person.sys_id", "activity_type"],
        query="activity_type=0123456789abcdef",
        having="count^person.sys_id^!=^1",
        display_value="true",
        return_count=True)
    assert built.query == result
    assert str(built) == result


def test_aggregate_query_no_groupby():
    result = (
        "/api/now/stats/activity?"
        "sysparm_query=activity_type=0123456789abcdef&"
        "sysparm_count=true&"
        "sysparm_display_value=true&"
        "sysparm_having=count^person.sys_id^!=^1")

    built = AggregateQueryBuilder(
        table="activity",
        query="activity_type=0123456789abcdef",
        having="count^person.sys_id^!=^1",
        display_value="true",
        return_count=True)
    assert built.query == result
    assert str(built) == result


def test_aggregate_query_no_groupby_no_having():
    result = (
        "/api/now/stats/activity?"
        "sysparm_query=activity_type=0123456789abcdef&"
        "sysparm_count=true&"
        "sysparm_display_value=true")

    built = AggregateQueryBuilder(
        table="activity",
        query="activity_type=0123456789abcdef",
        display_value="true",
        return_count=True)
    assert built.query == result
    assert str(built) == result


def test_table_query():
    result = (
        "/api/now/table/activity?"
        "sysparm_display_value=true&"
        "sysparm_query=activity_type=0123456789abcdef^"
        "person.primary_case=0a9b8c7d6e5f4321&"
        "sysparm_fields=person.primary_case&"
        "sysparm_offset=1&"
        "sysparm_limit=1")

    built = TableQueryBuilder(
        table="activity",
        query=(
            "activity_type=0123456789abcdef^"
            "person.primary_case=0a9b8c7d6e5f4321"),
        fields=["person.primary_case"],
        limit=1,
        offset=1,
        display_value="true")
    assert built.query == result
    assert str(built) == result


def test_table_query_no_explicit_display_value():
    result = (
        "/api/now/table/activity?"
        "sysparm_display_value=all&"
        "sysparm_exclude_reference_link=False&"
        "sysparm_query=activity_type=0123456789abcdef^"
        "person.primary_case=0a9b8c7d6e5f4321&"
        "sysparm_fields=person.primary_case&"
        "sysparm_offset=1&"
        "sysparm_limit=1")

    built = TableQueryBuilder(
        table="activity",
        query=(
            "activity_type=0123456789abcdef^"
            "person.primary_case=0a9b8c7d6e5f4321"),
        fields=["person.primary_case"],
        limit=1,
        offset=1)
    assert built.query == result
    assert str(built) == result
