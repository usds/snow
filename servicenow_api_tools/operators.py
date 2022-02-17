# Query helper functions -- use these to construct valid servicenow queries
# https://docs.servicenow.com/bundle/rome-platform-user-interface/page/use/common-ui-elements/reference/r_OpAvailableFiltersQueries.html
OR = lambda *subqueries: "^OR".join(subqueries) # noqa
AND = lambda *subqueries: "^".join(subqueries) # noqa
EMPTY = lambda field: f"{field}ISEMPTY" # noqa
NOT_EMPTY = lambda field: f"{field}ISNOTEMPTY" # noqa
CONTAINS = lambda field, value: f"{field}LIKE{value}" # noqa
NOT_CONTAINS = lambda field, value: f"{field}NOTLIKE{value}" # noqa
IS = lambda field, value: f"{field}={value}" # noqa
IS_NOT = lambda field, value: f"{field}!={value}" # noqa
DATE_BETWEEN = lambda field, date_start, date_end: f"{field}BETWEEN{date_start}@{date_end}" # noqa
