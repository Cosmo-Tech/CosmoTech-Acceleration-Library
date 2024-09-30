# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import csv
import glob
import json
import os


def apply_simple_csv_parameter_to_simulator(
    simulator,
    parameter_name: str,
    target_attribute_name: str,
    csv_id_column: str = "id",
    csv_value_column: str = "value"
    ):
    """
    Accelerator used to apply CSV parameters directly to a simulator
    Will raise a ValueError if the parameter does not exist
    If an entity is not found, will skip the row in the CSV
    :param simulator: The simulator object to which the parameter will be applied
    :param parameter_name: The name of the parameter fetched from the API
    :param target_attribute_name: Target attribute of the entities listed in the CSV
    :param csv_id_column: Column in the CSV file used for the entity ID
    :param csv_value_column: Column in the CSV file used for the attribute value to change
    :return: None
    """
    parameter_path = os.path.join(os.environ.get("CSM_PARAMETERS_ABSOLUTE_PATH"), parameter_name)
    if os.path.exists(parameter_path):
        csv_files = glob.glob(os.path.join(parameter_path, "*.csv"))
        for csv_filename in csv_files:
            model = simulator.GetModel()
            with open(csv_filename, "r") as csv_file:
                for row in csv.DictReader(csv_file):
                    entity_name = row.get(csv_id_column)
                    value = json.loads(row.get(csv_value_column))
                    entity = model.FindEntityByName(entity_name)
                    if entity:
                        entity.SetAttributeAsString(target_attribute_name, json.dumps(value))
    else:
        raise ValueError(f"Parameter {parameter_name} does not exists.")


__all__ = [apply_simple_csv_parameter_to_simulator]
