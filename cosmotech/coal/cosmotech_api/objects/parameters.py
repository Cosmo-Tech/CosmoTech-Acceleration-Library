# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import json
import os
import pathlib
from csv import DictWriter
from typing import Any, Dict, List

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.logger import LOGGER


class Parameters:
    values: Dict[str, Any] = dict()
    parameters_list: List[Dict[str, Any]] = list()

    def __init__(self, runner_data: Any):
        """
        Extract parameters from runner data.

        Args:
            runner_data: Runner data object

        Returns:
            Dictionary mapping parameter IDs to values
        """
        for parameter in runner_data.parameters_values:
            self.values[parameter.parameter_id] = parameter.value
        self.parameters_list = self.format_parameters_list(runner_data)

    @staticmethod
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

    def write_parameters_to_json(
        self,
        parameter_folder: str,
    ) -> str:
        pathlib.Path(parameter_folder).mkdir(exist_ok=True, parents=True)
        tmp_parameter_file = os.path.join(parameter_folder, "parameters.json")

        LOGGER.info(T("coal.cosmotech_api.runner.generating_file").format(file=tmp_parameter_file))

        with open(tmp_parameter_file, "w") as _file:
            json.dump(self.parameters_list, _file, indent=2)

        return tmp_parameter_file

    def write_parameters_to_csv(
        self,
        parameter_folder: str,
    ) -> str:
        pathlib.Path(parameter_folder).mkdir(exist_ok=True, parents=True)
        tmp_parameter_file = os.path.join(parameter_folder, "parameters.csv")

        LOGGER.info(T("coal.cosmotech_api.runner.generating_file").format(file=tmp_parameter_file))

        with open(tmp_parameter_file, "w") as _file:
            _w = DictWriter(_file, fieldnames=["parameterId", "value", "varType", "isInherited"])
            _w.writeheader()
            _w.writerows(self.parameters_list)

        return tmp_parameter_file

    def write_parameters(
        self,
        parameter_folder: str,
        write_csv: bool = True,
        write_json: bool = False,
    ) -> Dict[str, str]:
        result = {}

        if write_csv:
            result["csv"] = self.write_parameters_to_csv(parameter_folder)

        if write_json:
            result["json"] = self.write_parameters_to_json(parameter_folder)

        return result
