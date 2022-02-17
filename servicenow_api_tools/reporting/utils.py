from typing import Tuple, List, Dict
from urllib import parse
import pandas as pd
import re


def parse_servicenow_ui_url(url: str) -> List[Dict[str, str]]:
    """
    Given a servicenow url to the UI, parses it and gets the table and the query for that URL.
    """
    def _parse_table_and_query(url: str) -> Tuple[str, str]:
        # First parse the toplevel URL
        parsed_uri = parse.urlparse(url)
        uri_params = parse.parse_qs(parsed_uri.query)

        # The "uri" parameter has another url-like object in it that we need to parse
        if "uri" in uri_params:
            assert (len(uri_params.keys()) == 1
                    and "uri" in uri_params
                    and len(uri_params["uri"]) == 1)
            uri = uri_params["uri"][0]
            parsed_uri = parse.urlparse(uri)
            uri_params = parse.parse_qs(parsed_uri.query)

        # The "sysparm_query" param contains a query that works with the servicenow rest API
        assert "sysparm_query" in uri_params and len(uri_params["sysparm_query"]) == 1
        query = uri_params["sysparm_query"][0]

        # The path contains the table name, with "_list.do" appended on the end
        assert parsed_uri.path.startswith("/") and parsed_uri.path.endswith("_list.do")
        table_re = "/(.*)_list.do"
        match = re.search(table_re, parsed_uri.path)
        assert match and match.group(1)
        table = match.group(1)

        return table, query

    # TODO: Create a better format for describing this...
    def _handle_compound_query(url: str, op: str) -> List[Dict[str, str]]:
        operations = []
        if "PLUS" in url:
            (first, second) = url.split("PLUS", 1)
            if first.strip().startswith("https"):
                operations.extend(_handle_compound_query(first.strip(), op))
            else:
                print(f"INVALID QUERY: {first}")
            if second.strip().startswith("https"):
                operations.extend(_handle_compound_query(second.strip(), "PLUS"))
            else:
                print(f"INVALID QUERY: {second}")
        elif "MINUS" in url:
            (first, second) = url.split("MINUS", 1)
            if first.strip().startswith("https"):
                operations.extend(_handle_compound_query(first.strip(), op))
            else:
                print(f"INVALID QUERY: {first}")
            if second.strip().startswith("https"):
                operations.extend(_handle_compound_query(second.strip(), "MINUS"))
            else:
                print(f"INVALID QUERY: {second}")
        else:
            table, query = _parse_table_and_query(url)
            operations = [{"operation": op, "table": table, "query": query}]
        return operations
    return _handle_compound_query(url, "PLUS")


def parse_servicenow_ui_urls_file(
        urls_file: str, name_column: str, url_column: str,
        include_filter: str = None) -> List[Dict[str, str]]:
    columns = pd.read_csv(urls_file, keep_default_na=False)
    results = []
    for _, row in columns.iterrows():
        if include_filter:
            include_filters = include_filter.split(",")
            should_skip = True
            for filter in include_filters:
                split_include_filter = filter.split("=")
                assert len(split_include_filter) == 2, "include filter must be key=value format"
                (include_column, include_value) = split_include_filter
                if row[include_column] == include_value:
                    should_skip = False
            if should_skip:
                continue
        if not row[url_column]:
            continue
        if not row[url_column].startswith("https"):
            continue
        operations = parse_servicenow_ui_url(row[url_column])
        results.append({
            "name": row[name_column],
            "operations": operations,
        })
    return results
