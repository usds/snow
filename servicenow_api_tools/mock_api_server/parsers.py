from lark import Lark, Tree
import urllib.parse as parse
from typing import Dict
from servicenow_api_tools.utils import remove_prefix


def parse_sysparm_query(query: str) -> Tree:
    # NOTE: The value for "is" is optional. "=" with nothing is a synonym for EMPTY.
    parser = Lark(r"""
        matcher: empty
               | not_empty
               | contains
               | not_contains
               | is
               | is_not
               | date_between
               | in

        query: matcher
             | or
             | and

        or: matcher ( "^OR" matcher )*
        and: query ( "^" query )*
        empty: DOTTED_FIELD "ISEMPTY"
        not_empty: DOTTED_FIELD "ISNOTEMPTY"
        contains: DOTTED_FIELD "LIKE" VALUE
        not_contains: DOTTED_FIELD "NOTLIKE" VALUE
        is: DOTTED_FIELD "=" VALUE?
        is_not: DOTTED_FIELD "!=" VALUE
        date_between: DOTTED_FIELD "BETWEEN" VALUE "@" VALUE
        in: DOTTED_FIELD "IN" NOCOMMA_VALUE ( "," NOCOMMA_VALUE )*

        DOTTED_FIELD: FIELD ( "." FIELD)*
        FIELD: /[a-z_]+/
        VALUE: /[^\^@]+/
        NOCOMMA_VALUE: /[^\^@^,]+/""", start='query')
    return parser.parse(query)


def parse_sysparm_fields(fields: str) -> Tree:
    parser = Lark(r"""
        fields: DOTTED_FIELD ( "," DOTTED_FIELD )*
        DOTTED_FIELD: FIELD ( "." FIELD)*
        FIELD: /[a-z_]+/
        """, start='fields')
    return parser.parse(fields)


def parse_sysparm_group_by(fields: str) -> Tree:
    parser = Lark(r"""
        fields: DOTTED_FIELD ( "," DOTTED_FIELD )*
        DOTTED_FIELD: FIELD ( "." FIELD)*
        FIELD: /[a-z_]+/
        """, start='fields')
    return parser.parse(fields)


def parse_sysparm_offset(offset: str) -> Tree:
    parser = Lark(r"""
        offset: OFFSET
        OFFSET: /[0-9]+/
        """, start='offset')
    return parser.parse(offset)


def parse_sysparm_limit(limit: str) -> Tree:
    parser = Lark(r"""
        limit: LIMIT
        LIMIT: /[0-9]+/
        """, start='limit')
    return parser.parse(limit)


def parse_stats_query_url(url: str) -> Dict:
    split = parse.urlsplit(url)
    assert split.path.startswith("/api/now/stats/")
    table = remove_prefix(split.path, "/api/now/stats/")
    params = parse.parse_qs(split.query)
    assert params['sysparm_count'] == ['true']
    return {
        "endpoint": "stats",
        "table": table,
        "group_by": (
            parse_sysparm_group_by(params['sysparm_group_by'][0]) if
            'sysparm_group_by' in params else None),
        "query": (
            parse_sysparm_query(params['sysparm_query'][0]) if
            'sysparm_query' in params else None),
        "display_value": (
            params['sysparm_display_value'][0] if
            'sysparm_display_value' in params else None),
    }


def parse_table_query_url(url: str) -> Dict:
    split = parse.urlsplit(url)
    assert split.path.startswith("/api/now/table/")
    table = remove_prefix(split.path, "/api/now/table/")
    params = parse.parse_qs(split.query)
    return {
        "endpoint": "table",
        "table": table,
        "limit": (
            parse_sysparm_limit(params['sysparm_limit'][0]) if
            'sysparm_limit' in params else None),
        "offset": (
            parse_sysparm_offset(params['sysparm_offset'][0]) if
            'sysparm_offset' in params else None),
        "fields": (
            parse_sysparm_fields(params['sysparm_fields'][0]) if
            'sysparm_fields' in params else None),
        "query": (
            parse_sysparm_query(params['sysparm_query'][0]) if
            'sysparm_query' in params else None),
        "display_value": (
            params['sysparm_display_value'][0] if
            'sysparm_display_value' in params else None),
    }


def parse_url(url: str) -> Dict:
    split = parse.urlsplit(url)
    if split.path.startswith("/api/now/table/"):
        return parse_table_query_url(url)
    elif split.path.startswith("/api/now/stats/"):
        return parse_stats_query_url(url)
    else:
        raise Exception(f"Invalid path: {split.path}")
