# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Parameter handling functions.
"""

import json
import os
import pathlib
from csv import DictWriter
from typing import List, Dict, Any

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def get_runner_parameters(runner_data: Any) -> Dict[str, Any]:
    """
    Extract parameters from runner data.

    Args:
        runner_data: Runner data object

    Returns:
        Dictionary mapping parameter IDs to values
    """
    content = dict()
    for parameter in runner_data.parameters_values:
        content[parameter.parameter_id] = parameter.value
    return content


def format_parameters_list(runner_data: Any) -> List[Dict[str, Any]]:
    """
    Format parameters from runner data as a list of dictionaries.

    Args:
        runner_data: Runner data object

    Returns:
        List of parameter dictionaries
    """
    parameters = []

    if not runner_data.parameters_values:
        return parameters

    max_name_size = max(map(lambda r: len(r.parameter_id), runner_data.parameters_values))
    max_type_size = max(map(lambda r: len(r.var_type), runner_data.parameters_values))

    for parameter_data in runner_data.parameters_values:
        parameter_name = parameter_data.parameter_id
        value = parameter_data.value
        var_type = parameter_data.var_type
        is_inherited = parameter_data.is_inherited

        parameters.append(
            {
                "parameterId": parameter_name,
                "value": value,
                "varType": var_type,
                "isInherited": is_inherited,
            }
        )

        LOGGER.debug(
            T("coal.cosmotech_api.runner.parameter_debug").format(
                param_id=parameter_name,
                max_name_size=max_name_size,
                var_type=var_type,
                max_type_size=max_type_size,
                value=value,
                inherited=" inherited" if is_inherited else "",
            )
        )

    return parameters


def write_parameters_to_json(parameter_folder: str, parameters: List[Dict[str, Any]]) -> str:
    """
    Write parameters to a JSON file.

    Args:
        parameter_folder: Folder to write the file to
        parameters: List of parameter dictionaries

    Returns:
        Path to the created file
    """
    pathlib.Path(parameter_folder).mkdir(exist_ok=True, parents=True)
    tmp_parameter_file = os.path.join(parameter_folder, "parameters.json")

    LOGGER.info(T("coal.cosmotech_api.runner.generating_file").format(file=tmp_parameter_file))

    with open(tmp_parameter_file, "w") as _file:
        json.dump(parameters, _file, indent=2)

    return tmp_parameter_file


def write_parameters_to_csv(parameter_folder: str, parameters: List[Dict[str, Any]]) -> str:
    """
    Write parameters to a CSV file.

    Args:
        parameter_folder: Folder to write the file to
        parameters: List of parameter dictionaries

    Returns:
        Path to the created file
    """
    pathlib.Path(parameter_folder).mkdir(exist_ok=True, parents=True)
    tmp_parameter_file = os.path.join(parameter_folder, "parameters.csv")

    LOGGER.info(T("coal.cosmotech_api.runner.generating_file").format(file=tmp_parameter_file))

    with open(tmp_parameter_file, "w") as _file:
        _w = DictWriter(_file, fieldnames=["parameterId", "value", "varType", "isInherited"])
        _w.writeheader()
        _w.writerows(parameters)

    return tmp_parameter_file


def write_parameters(
    parameter_folder: str,
    parameters: List[Dict[str, Any]],
    write_csv: bool = True,
    write_json: bool = False,
) -> Dict[str, str]:
    """
    Write parameters to files based on specified formats.

    Args:
        parameter_folder: Folder to write the files to
        parameters: List of parameter dictionaries
        write_csv: Whether to write a CSV file
        write_json: Whether to write a JSON file

    Returns:
        Dictionary mapping file types to file paths
    """
    result = {}

    if write_csv:
        result["csv"] = write_parameters_to_csv(parameter_folder, parameters)

    if write_json:
        result["json"] = write_parameters_to_json(parameter_folder, parameters)

    return result
