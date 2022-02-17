import click
import json
from servicenow_api_tools.reporting import load_stats_report


@click.command()
@click.option('--stats-report-definition-file', required=True, type=click.Path(exists=True))
def verify_stats_report_definition(stats_report_definition_file):
    """
    Verifies that the given stats report definition is the correct structure.
    """
    # If the object gets created at all, we know the JSON file was structured correctly
    with open(stats_report_definition_file, 'r') as f:
        definition = load_stats_report(json.loads(f.read()))
    click.echo("Data test definition file is correct!")
    click.echo(definition)


if __name__ == '__main__':
    verify_stats_report_definition()
