import os
import csv
import glob
import json
from . import parametersPath


def apply_simple_csv_parameter_to_simulator(simulator,
                                            parameter_name: str,
                                            target_attribute_name: str,
                                            csv_id_column: str = "id",
                                            csv_value_column: str = "value"):
    parameter_path = os.path.join(parametersPath, parameter_name)
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
