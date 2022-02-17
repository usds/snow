import click
from servicenow_api_tools.synthetic_dataset import generate_synthetic_dataset
import os
import json


@click.command()
@click.option('--num-records-per-table', type=int, default=10,
              help="Number of records to generate per table")
@click.option('--output-directory', required=True, type=str, help="Directory to write test data to")
@click.option('--schemas-directory', required=True, type=str, help="Directory with table schemas")
def generate_test_data(num_records_per_table, output_directory, schemas_directory):
    """Helper script to generate test data."""
    dataset = generate_synthetic_dataset(num_records_per_table, schemas_directory)
    for table, data in dataset.items():
        output_file = os.path.join(output_directory, f"{table}.json")
        click.echo(f"Outputting {len(data['result'])} rows of synthetic data to {output_file}")
        with open(output_file, 'w') as f:
            f.write(json.dumps(data, indent=4, sort_keys=True))


if __name__ == '__main__':
    generate_test_data()
