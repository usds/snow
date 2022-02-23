# Servicenow API Tools

Tools to help you build applications against the [ServiceNow REST
API](https://docs.servicenow.com/bundle/rome-application-development/page/build/applications/concept/api-rest.html).
This is written in python, so most of the tools will be useful for building
python applications.

## Setup

Install [poetry](https://python-poetry.org), and run `poetry install`.

To verify everything is working, run `tox` to run all the tests

## Python Client Libraries

> NOTE: If you look on github, you'll find many other python client libraries,
> such as [pysnow](https://github.com/rbw/pysnow). Those might be great as well.
> These have integration with the [Mock API Server](#mock-api-server) below, but
> you could still use other client libraries with the [Mock API Server
> Standalone Mode](#standalone-mode).

> NOTE: The [Mock API Server](#mock-api-server) is used to test the client
> libraries, but it is currently read-only, so any of the client code that does
> updates is not currently well tested.

```python
from servicenow_api_tools.clients import (
    TableAPIClient, AggregateAPIClient, ServicenowRestEndpoint)

# https://developer.servicenow.com/dev.do#!/reference/api/rome/rest/c_TableAPI
tc = TableAPIClient(
    # Expands to https://example.servicenowservices.com
    endpoint=ServicenowRestEndpoint("example"))
tc.query(table="activity_type", fields=["activity_name"])

# https://developer.servicenow.com/dev.do#!/reference/api/rome/rest/c_AggregateAPI
ac = AggregateAPIClient(
    # Expands to https://example.servicenowservices.com
    endpoint=ServicenowRestEndpoint("example"))
ac.query(table="activity_type", group_by=["activity_name"])
```

## Server Schema Introspection

[Introspection](https://graphql.org/learn/introspection/) (also known as [Reflection](https://docs.sqlalchemy.org/en/14/core/reflection.html)) is the process of extracting information about the server from the server itself, such as database schemas.

> NOTE: The introspection scripts will extract some information, even if you
> don't have permission to the `sys_dictionary` table. However, the scripts
> should have an option to use the `sys_dictionary` table directly if the user
> has permission to access that table, since that [appears to be the real way to
> get schema information from the
> API](https://community.servicenow.com/community?id=community_question&sys_id=c8aa472ddb5cdbc01dcaf3231f96190a).

To quickly try this out, use the [Mock API Server Standalone
Mode](#standalone-mode):

```shell
poetry run python bin/mock_api_server.py \
    --data-directory tests/dataset \
    --schemas-directory tests/schemas
```

Then, in another tab, run:

```
mkdir newschemas
poetry run python bin/update_fields_catalogs.py \
    --base-url http://localhost:8080 \
    --tables-file tests/schemas/introspection/tables.json \
    --extra-filters-file tests/schemas/introspection/filters.json \
    --extra-mapping-fields-file tests/schemas/introspection/extra-mapping-fields.json \
    --table-display-values-file tests/schemas/introspection/table-display-values.json \
    --enum-tables-file tests/schemas/introspection/enum-tables.json \
    --catalog-directory newschemas
for i in newschemas/*.json; do
    echo $i
    cat $i | jq
done
```

This script takes a lot of human curated supplementary information, but can run
without it. See the JSON files referenced in the example above for the format.

Note that if `--catalog-directory` already has some json files in it, the script
will try to update those files with the new information found from the API, but
will try to preserve any human curated information.

## Fake Dataset Generator

The dataset generate can generate a fake dataset based on a schema file. For
links between tables, the reference fields will link to a random row in the
referenced table. You'll have to create a custom script similar to the one below
if you want to add more constraints to the generated data.

```shell
mkdir generated_dataset
poetry run python bin/generate_test_data.py \
    --output-directory generated_dataset \
    --schemas-directory tests/schemas
```

## Mock API Server

### Standalone Mode

```shell
poetry run python bin/mock_api_server.py \
    --data-directory tests/dataset \
    --schemas-directory tests/schemas
```

### Using The Python Client

Everything is the same as the [Python Client
Libraries](#python-client-libraries) for accessing a remote endpoint, except
that you pass in a path to a local dataset and schema instead of remote host
information:

```python
from servicenow_api_tools.mock_api_server import ServicenowRestEndpointLocalDataset
endpoint = ServicenowRestEndpointLocalDataset(
    path="tests/dataset",
    schema_dir="tests/schemas")
```

## Reporting Tools

There are some basic reporting tools to generate a report with some statistics
based on a list of queries. To do this, create a JSON file that defines some
attributes of your report. Try this example using the test definitions:

```shell
poetry run python bin/verify_stats_report_definition.py \
    --stats-report-definition-file \
    tests/reporting/stats-report-definition.json
mkdir published_reports
poetry run python bin/run_stats_report.py \
    --stats-report-definition-file \
    tests/reporting/stats-report-definition.json \
    --publish-directory published_reports \
    --base-url http://localhost:8080
ls published_reports
```

There is also a utility to parse a CSV with URLs for queries created in the
servicenow UI and turn that into a report definition:

```shell
poetry run python bin/generate_report_definition_columns_from_ui_urls.py \
    --ui-urls-file tests/reporting/parse_ui_url/urls.csv \
    --stats-report-definition-file generated-stats-report.json \
    --name-column column_name --url-column servicenow_ui_url
poetry run python bin/verify_stats_report_definition.py \
    --stats-report-definition-file generated-stats-report.json
```

There are tools to report on "data quality" that use the Table API instead of
the Aggregate API (so in theory, those tools in the future could dump datasets
of "malformed records" for manual correction). However, these don't have a nice
interface right now, so see the tests for usage details.

## Contributors

The initial code import represents the work of many people from:
- [U.S. Digital Service](https://usds.gov)
- [Center for Analytics](https://www.state.gov/about-us-office-of-management-strategy-and-solutions/#cfa)
