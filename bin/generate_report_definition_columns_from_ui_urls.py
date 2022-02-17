from servicenow_api_tools.reporting.utils import parse_servicenow_ui_urls_file
import click
import json
import os


@click.command()
@click.option('--ui-urls-file', required=True, type=click.Path(exists=True))
@click.option('--stats-report-definition-file', required=True, type=click.Path(),
              help=(
                  "Stats report definitions file to add queries to, "
                  "creates a new one if it doesn't exist"))
@click.option('--name-column', required=True, type=str, help="Column to for metric names in csv")
@click.option('--url-column', required=True, type=str, help="Column for query urls in csv")
@click.option('--include-filter', required=False, type=str,
              help="Extra column=value filter in csv to skip rows that don't match")
def generate_report_definition_columns_from_ui_urls(
        ui_urls_file, stats_report_definition_file, name_column, url_column, include_filter):
    columns = parse_servicenow_ui_urls_file(ui_urls_file, name_column, url_column, include_filter)
    if os.path.exists(stats_report_definition_file):
        with open(stats_report_definition_file, 'r') as f:
            report = json.loads(f.read())
    else:
        report = {"report_name": ui_urls_file}
    report["columns"] = columns
    with open(stats_report_definition_file, 'w') as f:
        f.write(json.dumps(report, indent=4, sort_keys=True))


if __name__ == '__main__':
    generate_report_definition_columns_from_ui_urls()
