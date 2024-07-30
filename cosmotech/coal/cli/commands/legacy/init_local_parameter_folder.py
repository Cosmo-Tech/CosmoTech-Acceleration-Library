# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import pathlib
from csv import DictWriter

from cosmotech_api.api.solution_api import RunTemplate
from cosmotech_api.api.solution_api import Solution

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.api import get_solution
from cosmotech.coal.utils.api import read_solution_file
from cosmotech.coal.utils.logger import LOGGER


def write_parameters(parameter_folder, parameters, write_csv, write_json):
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


@click.group()
@web_help("csm-data/legacy/init-local-parameter-folder")
def init_local_parameter_folder():
    """Base command to initialize parameter folders  
Will create:    
- A `parameters.json`/`parameters.csv` in the folder with all parameters  
- A folder per `%DATASETID%` datasets with the name of the parameter  
Check the help of the sub commands for more information:  
- `cloud` requires access to a fully deployed solution  
- `solution` requires a `Solution.yaml` file"""
    pass


@init_local_parameter_folder.command()
@click.argument("solution_file",
                type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
                required=True,
                nargs=1)
@click.argument("output_folder",
                type=click.Path(dir_okay=True, readable=True, writable=True),
                required=True,
                envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
                nargs=1)
@click.argument("run_template_id",
                required=True)
@click.option("--write-json/--no-write-json",
              envvar="WRITE_JSON",
              show_envvar=True,
              default=False,
              show_default=True,
              help="Toggle writing of parameters in json format")
@click.option("--write-csv/--no-write-csv",
              envvar="WRITE_CSV",
              show_envvar=True,
              default=True,
              show_default=True,
              help="Toggle writing of parameters in csv format")
@web_help("csm-data/legacy/init-local-parameter-folder/solution")
def solution(
    solution_file: str,
    run_template_id: str,
    output_folder: str,
    write_json: bool,
    write_csv: bool
):
    """Initialize parameter folder for given run template from a Solution yaml file"""
    if sol := read_solution_file(solution_file):
        return generate_parameters(sol, run_template_id, output_folder, write_json, write_csv)
    return 1


@init_local_parameter_folder.command()
@click.argument("output_folder",
                envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
                type=click.Path(dir_okay=True, readable=True, writable=True),
                required=True,
                nargs=1)
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              help="The id of an organization in the cosmotech api",
              metavar="o-##########",
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              help="The id of a solution in the cosmotech api",
              metavar="w-##########",
              required=True)
@click.option("--run-template-id",
              envvar="CSM_RUN_TEMPLATE_ID",
              show_envvar=True,
              help="The name of the run template in the cosmotech api",
              metavar="NAME",
              required=True)
@click.option("--write-json/--no-write-json",
              envvar="WRITE_JSON",
              show_envvar=True,
              default=False,
              show_default=True,
              help="Toggle writing of parameters in json format")
@click.option("--write-csv/--no-write-csv",
              envvar="WRITE_CSV",
              show_envvar=True,
              default=True,
              show_default=True,
              help="Toggle writing of parameters in csv format")
@web_help("csm-data/legacy/init-local-parameter-folder/cloud")
def cloud(
    workspace_id: str,
    organization_id: str,
    run_template_id: str,
    output_folder: str,
    write_json: bool,
    write_csv: bool,
):
    """Initialize parameter folder for given run template from CosmoTech cloud API"""
    if sol := get_solution(organization_id=organization_id,
                           workspace_id=workspace_id):
        return generate_parameters(sol, run_template_id, output_folder, write_json, write_csv)
    return 1


def generate_parameters(
    solution: Solution,
    run_template_id: str,
    output_folder: str,
    write_json: bool,
    write_csv: bool
):
    LOGGER.info(f"Searching [green bold]{run_template_id}[/] in the solution")
    if _t := [t for t in solution.run_templates if t.id == run_template_id]:
        template: RunTemplate = _t[0]
    else:
        LOGGER.error(f"Run template [green bold]{run_template_id}[/] was not found.")
        raise click.Abort()
    LOGGER.info(f"Found [green bold]{run_template_id}[/] in the solution generating json file")
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
            parameters.append({
                "parameterId": parameter_name,
                "value": f'{parameter_name}_value',
                "varType": var_type,
                "isInherited": False
            })
    if not (write_csv or write_json or dataset_parameters):
        LOGGER.warning(f"No parameters to write for [green bold]{run_template_id}[/]")
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


if __name__ == "__main__":
    init_local_parameter_folder()
