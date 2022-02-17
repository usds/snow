from typing import List


class AggregateQueryBuilder:
    def __init__(self, table: str, group_by: List[str] = None, query: str = None,
                 having: str = None, display_value: str = None, return_count: bool = True):
        api_endpoint = f'/api/now/stats/{table}'
        query_params = []
        if group_by:
            query_params.append(f'sysparm_group_by={",".join(group_by)}')
        if query:
            query_params.append(f'sysparm_query={ query }')
        query_params.append(f'sysparm_count={ "true" if return_count else "false" }')
        query_params.append(
            f'sysparm_display_value={ display_value if display_value else "false" }')
        if having:
            query_params.append(f'sysparm_having={ having }')
        self.query = f'{api_endpoint}?{ "&".join(query_params) }'

    def __str__(self):
        return self.query


class TableQueryBuilder:
    def __init__(self, table: str, query: str = None,
                 fields: List[str] = None, limit: int = None, offset: int = None,
                 display_value: str = None):
        self.query = self._build_entity_query_url(
            table_name=table, query_params=query,
            offset=offset, limit=limit, fields=fields, display_value=display_value)

    def _table_endpoint(self):
        return "/api/now/table"

    def _build_entity_query_url(
            self,
            table_name,
            query_params,
            offset=None,
            limit=None,
            fields=[],
            display_value=None):

        combined_query_params = ""
        if display_value is None:
            expand_reference_value_params = (
                "sysparm_display_value=all&sysparm_exclude_reference_link=False")
            combined_query_params = f"{expand_reference_value_params}"
        else:
            combined_query_params = (
                f'sysparm_display_value={ display_value if display_value else "false" }')

        if query_params:
            combined_query_params += f"&sysparm_query={query_params}"

        if fields:
            joined_fields = ",".join(fields)
            combined_query_params += f"&sysparm_fields={joined_fields}"

        if offset is not None:
            combined_query_params += f"&sysparm_offset={offset}"

        if limit is not None:
            combined_query_params += f"&sysparm_limit={limit}"

        return f"{self._table_endpoint()}/{table_name}?{combined_query_params}"

    def __str__(self):
        return self.query


class UpdateQueryBuilder:
    def __init__(self, table: str, sys_id: str):
        self.query = f'/api/now/table/{table}/{sys_id}'

    def __str__(self):
        return self.query
