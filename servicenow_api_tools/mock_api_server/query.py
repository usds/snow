from lark import Tree, Token
from typing import Dict, List
import datetime
import json
import logging
import os
import pandas as pd
import servicenow_api_tools.mock_api_server.parsers as parsers
from servicenow_api_tools.schema.schema import load_fields_catalog
from servicenow_api_tools.utils import dataframe_to_api_results, api_results_to_dataframe

logging.basicConfig()
logger = logging.getLogger(__name__)


class LocalDatasetQueryRunner():
    def __init__(self, data_directory: str, schema_directory: str):
        self.dataset = self._load_data(data_directory)
        self.index = self._build_index(self.dataset)
        self.schema_directory = schema_directory

    def _build_index(self, dataset: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:

        index: Dict[str, pd.DataFrame] = {}
        for table, df in dataset.items():
            for column in df.columns:
                assert (df[column].apply(
                        lambda x: ((type(x) == dict)
                                   and ('value' in x)
                                   and ('display_value' in x))).all()), (
                    f"Column: {column} has malformed values!\n"
                    "Required to be objects with 'value' and 'display_value' keys!")
            index[table] = df[['sys_id']].applymap(lambda x: x['value'])
        return index

    def _load_data(self, data_directory: str) -> Dict[str, pd.DataFrame]:
        data_files = os.listdir(data_directory)
        dataset = {}
        for data_file in data_files:
            data_file_fullpath = os.path.join(data_directory, data_file)
            if not data_file.endswith(".json"):
                raise Exception(f"Found non json data file: {data_file_fullpath}")
            table_name = data_file.removesuffix(".json")
            with open(data_file_fullpath, 'r') as f:
                dataset[table_name] = api_results_to_dataframe(json.loads(f.read()))
        return dataset

    def _get_dotted_field_value(self, table: str, field: str, sys_id: str,
                                display_value: str = "false",
                                # We need this because when qe are actually doing a query we want to
                                # compare against the actual IDs.
                                id_only_for_link_fields: bool = True):
        assert type(sys_id) == str, f"Passed sys_id is not a string: {sys_id}"
        if table not in self.dataset:
            raise Exception(f"Table {table} not in dataset")
        if table not in self.index:
            raise Exception(f"Table {table} not in index")
        row = self.dataset[table].loc[self.index[table]['sys_id'] == sys_id]
        assert row.shape[0] == 1, f"Did not find exactly one row: {row}"
        if "." in field:
            link = field.split(".")[0]
            link_sys_id = row[link].values[0]['value']
            catalog = load_fields_catalog(table, self.schema_directory)
            assert 'reference' in catalog[link], f"Walking non-link field: {catalog[field]}"
            link_table = catalog[link]['reference']
            remaining_fields = ".".join(field.split(".")[1:])
            return self._get_dotted_field_value(
                link_table, remaining_fields, link_sys_id, display_value, False)
        else:
            # https://community.servicenow.com/community?id=community_question&sys_id=fd76cfe1db1cdbc01dcaf3231f9619cc
            if display_value == "false":
                if id_only_for_link_fields:
                    return row[field].apply(lambda x: x['value'])
                else:
                    return row[field].apply(
                        lambda x:
                        {
                            "value": x['value'],
                            "link": x['link'],
                        } if "link" in x else x['value']
                    )
            elif display_value == "true":
                return row[field].apply(
                    lambda x:
                    {
                        "display_value": x['display_value'],
                        "link": x['link'],
                    } if "link" in x else x['display_value']
                )
            elif display_value == "all":
                return row[field]
            else:
                raise Exception(f"Invalid parameter for display_value: {display_value}")

    def _matcher(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_matcher: {tree.data}")
        assert len(tree.children) == 1
        for child in tree.children:
            assert isinstance(child, Tree)
            if child.data == 'empty':
                return self._empty(table, child)
            elif child.data == 'not_empty':
                return self._not_empty(table, child)
            elif child.data == 'contains':
                return self._contains(table, child)
            elif child.data == 'not_contains':
                return self._not_contains(table, child)
            elif child.data == 'is':
                return self._is(table, child)
            elif child.data == 'is_not':
                return self._is_not(table, child)
            elif child.data == 'date_between':
                return self._date_between(table, child)
            elif child.data == 'in':
                return self._in(table, child)
            else:
                raise Exception(f"_matcher: Found invalid node in parse tree: {child.data}")
        assert False, "Should not be able to get here, should have at least one child."

    def _or(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_or: {tree.data}")
        dfs = []
        for child in tree.children:
            assert isinstance(child, Tree)
            if child.data == 'matcher':
                dfs.append(self._matcher(table, child))
            else:
                raise Exception(f"_or: Found invalid node in parse tree: {child.data}")
        return pd.concat(dfs).drop_duplicates().reset_index(drop=True)

    def _and(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_and: {tree.data}")
        result = None
        for child in tree.children:
            assert isinstance(child, Tree)
            if child.data == 'query':
                if result is None:
                    result = self._query(table, child)
                else:
                    result = result.merge(
                        self._query(table, child)['sys_id'], on="sys_id", how="inner")
            else:
                raise Exception(f"_and: Found invalid node in parse tree: {child.data}")
        assert result is not None, (
            "Did not find any children of and, this shouldn't be allowed in the grammar")
        return result

    def _empty(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_empty: {tree.data}")
        assert len(tree.children) == 1, "_empty should have two children: {tree.children}"

        def _empty_filter(row, field):
            # https://stackoverflow.com/a/26658301
            row_value = self._get_dotted_field_value(table, field, row['sys_id'])
            if row_value.values[0]:
                return False
            else:
                return True

        field = None
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "DOTTED_FIELD":
                field = str(child)
        assert field
        matcher = self.index[table].apply(_empty_filter, field=field, axis=1)
        return self.index[table][matcher]

    def _not_empty(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_not_empty: {tree.data}")
        assert len(tree.children) == 1, "_not_empty should have two children: {tree.children}"

        def _not_empty_filter(row, field):
            # https://stackoverflow.com/a/26658301
            row_value = self._get_dotted_field_value(table, field, row['sys_id'])
            if row_value.values[0]:
                return True
            else:
                return False

        field = None
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "DOTTED_FIELD":
                field = str(child)
        assert field
        matcher = self.index[table].apply(_not_empty_filter, field=field, axis=1)
        return self.index[table][matcher]

    def _contains(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_contains: {tree.data}")
        assert len(tree.children) == 2, "_contains should have two children: {tree.children}"
        raise Exception("Not Yet Implemented")

    def _not_contains(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_not_contains: {tree.data}")
        assert len(tree.children) == 2, "_not_contains should have two children: {tree.children}"
        raise Exception("Not Yet Implemented")

    def _is(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_is: {tree.data}")
        # Not having a value is a synonym for empty string
        assert len(tree.children) == 2 or len(tree.children) == 1, (
            "_is should have one or two children: {tree.children}")

        def _is_filter(row, field, value):
            # https://stackoverflow.com/a/26658301
            row_value = self._get_dotted_field_value(table, field, row['sys_id'])
            # XXX: I needed this string conversion for "active=true". Does it cause other problems?
            if str(row_value.values[0]).lower() == str(value).lower():
                return True
            else:
                return False

        field = None
        value = None
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "DOTTED_FIELD":
                field = str(child)
            if child.type == "VALUE":
                value = str(child)
        if value is None:
            value = ""
        assert field is not None
        assert value is not None
        matcher = self.index[table].apply(_is_filter, field=field, value=value, axis=1)
        return self.index[table][matcher]

    def _is_not(self, table: str, tree: Tree) -> pd.DataFrame:
        assert len(tree.children) == 2, "_is_not should have two children: {tree.children}"

        def _is_not_filter(row, field, value):
            # https://stackoverflow.com/a/26658301
            row_value = self._get_dotted_field_value(table, field, row['sys_id'])
            # XXX: I needed this string conversion for "active=true". Does it cause other problems?
            if str(row_value.values[0]).lower() != str(value).lower():
                return True
            else:
                return False

        field = None
        value = None
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "DOTTED_FIELD":
                field = str(child)
            if child.type == "VALUE":
                value = str(child)
        assert field
        assert value
        matcher = self.index[table].apply(_is_not_filter, field=field, value=value, axis=1)
        return self.index[table][matcher]

    def _date_between(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_date_between: {tree.data}")
        assert len(tree.children) == 3, "_date_between should have three children: {tree.children}"

        def _date_between_filter(row, field, values):
            assert len(values) == 2
            from_date = datetime.datetime.strptime(str(values[0]), "%Y-%m-%d")
            to_date = datetime.datetime.strptime(str(values[1]), "%Y-%m-%d")
            # https://stackoverflow.com/a/26658301
            row_value = self._get_dotted_field_value(table, field, row['sys_id'])
            field_date = datetime.datetime.strptime(str(row_value.values[0]), "%Y-%m-%d")
            if from_date < field_date and field_date < to_date:
                return True
            else:
                return False

        field = None
        values = []
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "DOTTED_FIELD":
                field = str(child)
            if child.type == "VALUE":
                values.append(str(child))
        assert field
        assert len(values) == 2
        matcher = self.index[table].apply(
            _date_between_filter, field=field, values=values, axis=1)
        return self.index[table][matcher]

    def _in(self, table: str, tree: Tree) -> pd.DataFrame:
        logger.debug(f"_in: {tree.data}")
        assert len(tree.children) > 1, "_in should have at least two children: {tree.children}"

        def _in_filter(row, field, values):
            # https://stackoverflow.com/a/26658301
            row_value = self._get_dotted_field_value(table, field, row['sys_id'])
            # XXX: I needed this string conversion for "active=true". Does it cause other problems?
            if str(row_value.values[0]).lower() in [str(v).lower() for v in values]:
                return True
            else:
                return False

        logger.debug(tree.children)
        field = None
        values = []
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "DOTTED_FIELD":
                assert field is None
                field = str(child)
            if child.type == "NOCOMMA_VALUE":
                values.append(str(child))
        assert field
        assert values
        matcher = self.index[table].apply(_in_filter, field=field, values=values, axis=1)
        return self.index[table][matcher]

    def _query(self, table: str, tree: Tree) -> pd.DataFrame:
        if not tree:
            return self.index[table]
        logger.debug(f"_query: {tree.data}")
        assert len(tree.children) == 1
        assert isinstance(tree.children[0], Tree)
        if tree.children[0].data == 'matcher':
            return self._matcher(table, tree.children[0])
        elif tree.children[0].data == 'or':
            return self._or(table, tree.children[0])
        elif tree.children[0].data == 'and':
            return self._and(table, tree.children[0])
        else:
            raise Exception(f"_query: Found invalid node in parse tree: {tree.children[0].data}")

    def _fields(self, table: str, tree: Tree, sys_ids: List[str],
                display_value: str) -> pd.DataFrame:
        fields = []
        if tree:
            logger.debug(f"_fields: {tree.data}")
            for child in tree.children:
                fields.append(str(child))
        else:
            fields = list(self.dataset[table].columns)
        rows = []
        for sys_id in sys_ids:
            row = []
            for field in fields:
                row.append(self._get_dotted_field_value(table, field, sys_id,
                           display_value=display_value, id_only_for_link_fields=False).values[0])
            rows.append(row)
        return pd.DataFrame(columns=fields, data=rows)

    def _offset(self, tree: Tree, df: pd.DataFrame) -> pd.DataFrame:
        if tree is None:
            return df
        logger.debug(f"_offset: {tree.data}")
        assert len(tree.children) == 1, "_offset should have one child: {tree.children}"
        offset = None
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "OFFSET":
                offset = int(str(child))
        assert offset is not None
        return df.iloc[offset:]

    def _limit(self, tree: Tree, df: pd.DataFrame) -> pd.DataFrame:
        if tree is None:
            return df
        logger.debug(f"_limit: {tree.data}")
        assert len(tree.children) == 1, "_limit should have one child: {tree.children}"
        limit = None
        for child in tree.children:
            assert isinstance(child, Token)
            if child.type == "LIMIT":
                limit = int(str(child))
        assert limit
        return df.iloc[:limit]

    def _group_by(self, tree: Tree, df: pd.DataFrame, display_value: str) -> Dict:
        logger.debug(f"_group_by: {tree.data}")
        fields = []
        for child in tree.children:
            fields.append(str(child))
        # If we passed "all" for display value, these are dictionaries, so we must convert to JSON
        # to avoid "unhashable type" errors when trying to group with pandas.
        #
        # If we pass "true" or "false" for display value, we actually still have trouble because
        # link fields are still dictionaries from the api, but other things aren't.
        value_counts = df.applymap(
            lambda x: json.dumps({"x": x})
        ).value_counts(subset=fields, sort=False)
        result: Dict = {}
        result['result'] = []
        for item in value_counts.items():
            groupby_fields = []
            for field, value in zip(fields, list(item[0])):
                groupby_field = {"field": field}
                unpacked_values = json.loads(value)["x"]
                if display_value == "all":
                    groupby_field["display_value"] = unpacked_values["display_value"]
                    groupby_field["value"] = unpacked_values["value"]
                elif display_value == "true":
                    if type(unpacked_values) == dict:
                        groupby_field["value"] = unpacked_values["display_value"]
                    else:
                        groupby_field["value"] = unpacked_values
                elif display_value == "false":
                    if type(unpacked_values) == dict:
                        groupby_field["value"] = unpacked_values["value"]
                    else:
                        groupby_field["value"] = unpacked_values
                else:
                    raise Exception(f"Invalid display value: {display_value}")
                groupby_fields.append(groupby_field)
            result['result'].append({
                "stats": {
                    "count": item[1]
                },
                "groupby_fields": groupby_fields
            })
        return result

    def query(self, url: str) -> Dict:
        parsed = parsers.parse_url(url)
        display_value = (
            "false" if
            'display_value' not in parsed or parsed['display_value'] is None else
            parsed['display_value'])
        assert display_value is not None
        if parsed['endpoint'] == "table":
            result = self._query(parsed['table'], parsed['query'])
            result = self._offset(parsed['offset'], result)
            result = self._limit(parsed['limit'], result)
            result = self._fields(
                parsed['table'], parsed['fields'], result['sys_id'].to_list(),
                display_value=display_value)
            result = result.reset_index(drop=True)
            return dataframe_to_api_results(result)
        elif parsed['endpoint'] == "stats":
            result = self._query(parsed['table'], parsed['query'])
            assert 'having' not in parsed or parsed['having'] is None
            if parsed['group_by'] is None:
                return {"result": {"stats": {
                        "count": result.shape[0]
                        }}}
            else:
                result = self._fields(
                    parsed['table'], parsed['group_by'], result['sys_id'].to_list(),
                    display_value=display_value)
                return self._group_by(parsed['group_by'], result, display_value)
        else:
            raise Exception(f"Invalid endpoint {parsed['endpoint']}")
