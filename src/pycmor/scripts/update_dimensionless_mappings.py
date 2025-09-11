#!/usr/bin/env python3
"""
Script to automatically update dimensionless_mappings.yaml file from CMIP6 Tables.

This script scans all CMIP6 table JSON files for variables with dimensionless units
or special unit conversions, and updates the dimensionless_mappings.yaml file with
proper formatting including standard names as comments.

The script keeps any existing values defined in the YAML file and leaves new values empty,
preserving human-defined special cases.
"""

import glob
import json
import os
import re

import click
import yaml

from ..core.logging import logger

# Units patterns that indicate dimensionless or special conversion variables
DIMENSIONLESS_PATTERNS = [
    r"1$",  # Pure dimensionless
    r"\d+\s?%",  # Percentage
    r"0.001",  # Salinity scaling factor
]

# Special keywords to check in units
SPECIAL_KEYWORDS = [
    "mol",  # May need carbon or other element specifications
    "%",  # Percentage units
]


def is_dimensionless_unit(unit):
    """Check if a unit string represents a dimensionless quantity"""
    for pattern in DIMENSIONLESS_PATTERNS:
        if re.match(pattern, unit):
            return True
    return False


def extract_variables_from_tables(tables_path):
    """
    Extract all variables with dimensionless units or special units from CMIP6 tables.
    Returns a dictionary of variable_name: {unit: unit, standard_name: standard_name}
    """
    variables = {}

    # Get all JSON files in the tables directory
    table_files = glob.glob(os.path.join(tables_path, "CMIP6_*.json"))

    for table_file in table_files:
        try:
            with open(table_file, "r") as f:
                table_data = json.load(f)

            # Skip files without variable entries
            if "variable_entry" not in table_data:
                continue

            for var_name, var_info in table_data["variable_entry"].items():
                # Skip if no units defined
                if "units" not in var_info:
                    continue

                unit = var_info["units"]

                # Check if this is a dimensionless unit or contains special keywords
                is_special = is_dimensionless_unit(unit) or any(
                    keyword in unit for keyword in SPECIAL_KEYWORDS
                )

                if is_special:
                    standard_name = var_info.get("standard_name", "not_specified")

                    # Only add if not already in our dictionary or if this has a standard_name and previous doesn't
                    if var_name not in variables or (
                        standard_name != "not_specified"
                        and variables[var_name]["standard_name"] == "not_specified"
                    ):

                        variables[var_name] = {
                            "unit": unit,
                            "standard_name": standard_name,
                        }
        except Exception as e:
            logger.error(f"Error processing {table_file}: {e}")

    return variables


def update_yaml_file(yaml_path, variables):
    """Update the dimensionless_mappings.yaml file with the extracted variables"""

    # If the file exists, load its current content
    existing_data = {}
    if os.path.exists(yaml_path):
        try:
            with open(yaml_path, "r") as f:
                existing_data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Could not read existing yaml file: {e}")

    # Create the new YAML content
    yaml_content = (
        "# In general:\n# model_variable_name:  # standard_name\n#   "
        "cmor_unit_string: pint_friendly_SI_units\n\n"
    )

    # Process all variables
    for var_name, var_info in sorted(variables.items()):
        standard_name = var_info["standard_name"]
        unit = var_info["unit"]

        # Format the YAML entry
        yaml_content += f"{var_name}:  # {standard_name}\n"

        # Check if this variable exists in the current YAML
        if var_name in existing_data and existing_data[var_name]:
            # Preserve existing values from the YAML file
            for unit_key, value in existing_data[var_name].items():
                # If value is None or empty or the string "None", leave just a space
                if value is None or value == "" or value == "None":
                    yaml_content += f'  "{unit_key}": \n'
                else:
                    yaml_content += f'  "{unit_key}": {value}\n'
        else:
            # All new entries get an empty value (just a space)
            yaml_content += f'  "{unit}": \n'

    # Write the updated content to the file
    with open(yaml_path, "w") as f:
        f.write(yaml_content)

    logger.info(f"Updated {yaml_path} with {len(variables)} variables")


@click.command(
    name="update-dimensionless-mappings",
    help="Update dimensionless_mappings.yaml file from CMIP6 Tables",
)
@click.option(
    "--yaml-path",
    type=click.Path(exists=False),
    required=True,
    help="Path to dimensionless_mappings.yaml file",
)
@click.option(
    "--tables-path",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    required=True,
    help="Path to CMIP6 Tables directory",
)
def update_dimensionless_mappings(yaml_path, tables_path):
    """Update dimensionless_mappings.yaml file from CMIP6 Tables."""
    logger.info(f"Extracting dimensionless variables from {tables_path}")
    variables = extract_variables_from_tables(tables_path)

    logger.info(f"Found {len(variables)} dimensionless variables")
    update_yaml_file(yaml_path, variables)

    logger.info("Done! The dimensionless_mappings.yaml file has been updated.")
    logger.info("Note: Existing human-defined values have been preserved.")
    logger.info("      New entries have empty values that may need manual review.")

    return 0
