# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Parameter handling functions.

This module provides functions for handling parameters in solution templates.
"""

import json
import os
import pathlib
from csv import DictWriter
from typing import List, Dict, Any

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
        LOGGER.info(T("coal.cosmotech_api.runner.generating_file").format(file=tmp_parameter_file))
        with open(tmp_parameter_file, "w") as _file:
            _w = DictWriter(_file, fieldnames=["parameterId", "value", "varType", "isInherited"])
            _w.writeheader()
            _w.writerows(parameters)

    if write_json:
        tmp_parameter_file = os.path.join(parameter_folder, "parameters.json")
        LOGGER.info(T("coal.cosmotech_api.runner.generating_file").format(file=tmp_parameter_file))
        with open(tmp_parameter_file, "w") as _file:
            json.dump(parameters, _file, indent=2)
