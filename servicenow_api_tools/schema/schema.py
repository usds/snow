import json
import os
from typing import Dict


def load_fields_catalog(table: str, catalog_directory: str) -> Dict:
    """
    Load the current fields at "catalog_directory".
    """
    catalog_file = os.path.join(catalog_directory, f"{table}.json")
    if os.path.exists(catalog_file):
        with open(catalog_file, 'r') as f:
            return json.loads(f.read())
    return {}


def merge_fields_catalog(table: str, catalog: Dict, catalog_directory: str) -> Dict:
    """
    Update the fields catalog at "catalog_directory", but not overwriting any human curated content.
    """
    catalog_file = os.path.join(catalog_directory, f"{table}.json")
    new_catalog = catalog
    if os.path.exists(catalog_file):
        new_catalog = load_fields_catalog(table, catalog_directory)
        keys_to_remove = list(
            set(new_catalog.keys())
            - set(catalog.keys()))
        for key, value in catalog.items():
            if key not in new_catalog:
                new_catalog[key] = catalog[key]
            else:
                if (("purpose" not in new_catalog[key] or new_catalog[key]["purpose"] == "TODO")
                   and "purpose" in catalog[key]):
                    new_catalog[key]["purpose"] = catalog[key]["purpose"]
                if 'unused' in catalog[key]:
                    new_catalog[key]["unused"] = catalog[key]["unused"]
                for subkey, subvalue in catalog[key].items():
                    if subkey not in new_catalog[key]:
                        new_catalog[key][subkey] = catalog[key][subkey]
        for key in keys_to_remove:
            del new_catalog[key]
    return new_catalog


def update_fields_catalog(table: str, catalog: Dict, catalog_directory: str):
    catalog_file = os.path.join(catalog_directory, f"{table}.json")
    with open(catalog_file, 'w') as f:
        f.write(json.dumps(catalog, indent=4, sort_keys=True))
