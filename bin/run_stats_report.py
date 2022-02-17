import click
import sys
from servicenow_api_tools.reporting import run_stats_report, publish_report_files
from servicenow_api_tools.reporting.build import set_debug_mode
from servicenow_api_tools.clients import ServicenowRestEndpoint


@click.command()
@click.option('--instance', type=str,
              help="ServiceNow instance to use, incompatible with --base-url.")
@click.option('--base-url', type=str, help="Full ServiceNow URL, incompatible with --instance.")
@click.option('--stats-report-definition-file', required=True, type=click.Path(exists=True))
@click.option('--debug-mode/--no-debug-mode',
              help="Run in \"debug mode\" to add debug columns with info used to calculate report",
              default=False)
@click.option('--publish-directory', required=True, type=str, help="Directory to publish into")
def do_run_stats_report(instance, base_url, stats_report_definition_file, debug_mode,
                        publish_directory):
    """
    Runs the given stats report.
    """
    if instance and base_url:
        click.echo("Cannot pass both --instance or --base-url")
        sys.exit(1)
    if not (instance or base_url):
        click.echo("Must pass either --instance or --base-url")
        sys.exit(1)
    set_debug_mode(debug_mode)
    (report_name, results) = run_stats_report(
        stats_report_definition_file=stats_report_definition_file,
        endpoint=ServicenowRestEndpoint(
            instance=instance,
            base_url=base_url))
    if debug_mode:
        report_name = f'{report_name} Debug Mode'
    publish_report_files(results, report_name, publish_directory)


if __name__ == '__main__':
    do_run_stats_report()
