import datetime
import random
import os
from typing import Dict, List, Tuple, Optional
from faker import Faker
from servicenow_api_tools.schema.schema import load_fields_catalog


def non_link_field(hash_value: str, display_value: str):
    return {
        "display_value": display_value,
        "value": hash_value,
    }


def generate_row(table: str, references: Dict, prefix: str, schemas_dir: str) -> Dict:
    """
    Generates a row of synthetic data given a table.

    "references" is a dictionary of fields, for references to other tables.

    See tests/schema for the format of "field_info".
    """

    fake = Faker({"en_AF": 1})
    catalog = load_fields_catalog(table, schemas_dir)
    data = {}
    for field, field_info in catalog.items():
        # This has to be handled after we generate all the rows, so there's something to link to
        if 'reference' in field_info:
            continue
        elif 'unused' in field_info and field_info['unused']:
            continue
        # TODO: Rework this code so I don't need to continue here.
        elif 'mappings' in field_info and field_info['mappings']:
            mapping_hash = fake.random_element(**{"elements": list(field_info['mappings'].keys())})
            data[prefix + field] = non_link_field(
                hash_value=mapping_hash,
                display_value=field_info['mappings'][mapping_hash])
            continue
        elif 'valid_values' in field_info and field_info['valid_values']:
            faker_function = fake.random_element
            faker_args = {"elements": field_info['valid_values']}
        elif 'format' in field_info and field_info['format']:
            faker_function = fake.bothify
            faker_args = {"text": field_info['format']}
        elif 'valid_date_range' in field_info and field_info['valid_date_range']:
            faker_function = lambda *args, **kwargs: ( # noqa
                fake.date_between_dates(*args, **kwargs).strftime("%Y-%m-%d"))
            faker_args = {}
            faker_args['date_start'] = field_info['valid_date_range'].get('date_start', None)
            faker_args['date_end'] = field_info['valid_date_range'].get('date_end', None)
            for key, value in faker_args.items():
                if value:
                    faker_args[key] = datetime.datetime.strptime(value, "%Y-%m-%d")
        elif 'faker' in field_info:
            faker_function = getattr(fake, field_info["faker"]["method"])
            faker_args = field_info["faker"]["args"]
        elif getattr(fake, field, None):
            raise Exception("I should have already handled this: {field}")
        else:
            faker_function = lambda: "(PLACEHOLDER)" # noqa
            faker_args = {}
        fake_value = faker_function(**faker_args)
        data[prefix + field] = non_link_field(fake_value, fake_value)
    return data


def generate_synthetic_dataset(
        num_records_per_table: int, schemas_dir: str) -> Dict[str, Dict[str, List[Dict]]]:
    """
    Generate a dataset, suitable for use by the local test endpoint. Randomly links rows together if
    there are reference fields.
    """

    tables = []
    for schema in os.listdir(schemas_dir):
        if schema.endswith(".json"):
            tables.append(schema.removesuffix(".json"))
    print(f"Tables: {tables}")

    # The dataset dictionary, of the format "table" -> "api-like result set"
    result: Dict[str, Dict[str, List[Dict]]] = {}

    def get_display_value_field_for_table(table: str) -> str:
        catalog = load_fields_catalog(table, schemas_dir)
        display_values = []
        for field, field_info in catalog.items():
            if 'is_table_display_value' in field_info and field_info['is_table_display_value']:
                display_values.append(field)
        assert len(display_values) == 1, (
            f"Table must have exactly one display value, found: {display_values}")
        return display_values[0]

    table_display_value_fields = {}
    for table in tables:
        table_display_value_fields[table] = get_display_value_field_for_table(table)

    def generate_link_field(table: str, result: Dict[str, Dict[str, List[Dict]]]) -> Dict[str, str]:
        """
        For non system tables, generate a link to a random element in a given table.
        """
        if table.startswith("sys_"):
            return {
                "display_value": "(PLACEHOLDER)",
                "link": table,
                "value": "(PLACEHOLDER)",
            }
        rows = result[table]['result']
        row = random.choice(rows)
        return {
            "display_value": row[table_display_value_fields[table]]['display_value'],
            "link": f"https://localdataset.local/api/now/table/{table}/{row['sys_id']['value']}",
            "value": row['sys_id']['value'],
        }

    def get_table_enum_mappings(
            table: str, schemas_dir: str) -> Optional[Tuple[str, str]]:
        catalog = load_fields_catalog(table, schemas_dir)

        def is_enum_field(field_info):
            return (('is_table_display_value' in field_info
                     and field_info['is_table_display_value'])
                    and ('valid_values' in field_info and field_info['valid_values']))

        for field, field_info in catalog.items():
            if is_enum_field(field_info):
                return (field, field_info['valid_values'])
        return None

    # First, generate the records
    for table in tables:
        result[table] = {}
        result[table]["result"] = []
        enum_mappings = get_table_enum_mappings(table, schemas_dir)
        if enum_mappings:
            enum_field = enum_mappings[0]
            for enum_value in enum_mappings[1]:
                enum_row = generate_row(table, {}, "", schemas_dir)
                enum_row[enum_field] = non_link_field(enum_value, enum_value)
                result[table]["result"].append(enum_row)
        else:
            records_generated = 0
            while num_records_per_table > records_generated:
                result[table]["result"].append(
                    generate_row(table, {}, "", schemas_dir))
                records_generated = records_generated + 1

    # Then, fix the links
    for table in tables:
        catalog = load_fields_catalog(table, schemas_dir)
        for field, field_info in catalog.items():
            if 'reference' not in field_info:
                continue
            for row in result[table]["result"]:
                link_table = field_info["reference"]
                row[field] = generate_link_field(table=link_table, result=result)

    return result
