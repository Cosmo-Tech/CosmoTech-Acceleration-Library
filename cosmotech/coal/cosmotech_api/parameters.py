# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.

"""
Parameter handling functions.

This module provides functions for handling parameters in solution templates.
"""

import json
import os
import pathlib
from csv import DictWriter
from typing import List, Dict, Any

from cosmotech_api.api.solution_api import RunTemplate
from cosmotech_api.api.solution_api import Solution

from cosmotech.coal.utils.api import get_solution
from cosmotech.coal.utils.api import read_solution_file
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def write_parameters(
    parameter_folder: str, parameters: List[Dict[str, Any]], write_csv: bool, write_json: bool
) -> None:
    """
    Write parameters to CSV and/or JSON files.

    Args:
        parameter_folder: The folder to write the parameters to
        parameters: The parameters to write
        write_csv: Whether to write the parameters to a CSV file
        write_json: Whether to write the parameters to a JSON file
    """
    if write_csv:
        tmp_parameter_file = os.path.join(parameter_folder, "parameters.csv")
        LOGGER.info(f"Generating {tmp_parameter_file}")
        with open(tmp_parameter_file, "w") as _file:
            _w = DictWriter(_file, fieldnames=["parameterId", "value", "varType", "isInherited"])
            _w.writeheader()
            _w.writerows(parameters)

    if write_json:
        tmp_parameter_file = os.path.join(parameter_folder, "parameters.json")
        LOGGER.info(f"Generating {tmp_parameter_file}")
        with open(tmp_parameter_file, "w") as _file:
            json.dump(parameters, _file, indent=2)


def generate_parameters(
    solution: Solution,
    run_template_id: str,
    output_folder: str,
    write_json: bool,
    write_csv: bool,
) -> int:
    """
    Generate parameter files from a solution.

    Args:
        solution: The solution object
        run_template_id: The ID of the run template to use
        output_folder: The folder to write the parameters to
        write_json: Whether to write the parameters to a JSON file
        write_csv: Whether to write the parameters to a CSV file

    Returns:
        0 on success, 1 on failure
    """
    LOGGER.info(f"Searching {run_template_id} in the solution")
    if _t := [t for t in solution.run_templates if t.id == run_template_id]:
        template: RunTemplate = _t[0]
    else:
        LOGGER.error(f"Run template {run_template_id} was not found.")
        return 1
    LOGGER.info(f"Found {run_template_id} in the solution generating json file")
    parameter_groups = template.parameter_groups
    parameter_names = []
    for param_group in solution.parameter_groups:
        if param_group.id in parameter_groups:
            parameter_names.extend(param_group.parameters)
    parameters = []
    dataset_parameters = []
    for param in solution.parameters:
        if param.id in parameter_names:
            parameter_name = param.id
            var_type = param.var_type
            if var_type == "%DATASETID%":
                dataset_parameters.append(parameter_name)
            parameters.append(
                {
                    "parameterId": parameter_name,
                    "value": f"{parameter_name}_value",
                    "varType": var_type,
                    "isInherited": False,
                }
            )
    if not (write_csv or write_json or dataset_parameters):
        LOGGER.warning(f"No parameters to write for {run_template_id} ")
        return 1
    output_folder_path = pathlib.Path(output_folder)
    output_folder_path.mkdir(parents=True, exist_ok=True)
    if dataset_parameters:
        LOGGER.info(f"Creating folders for dataset parameters")
    for d_param in dataset_parameters:
        dataset_parameters_folder = output_folder_path / d_param
        dataset_parameters_folder.mkdir(parents=True, exist_ok=True)
        LOGGER.info(f"- {dataset_parameters_folder}")
    write_parameters(str(output_folder_path), parameters, write_csv, write_json)
    return 0
