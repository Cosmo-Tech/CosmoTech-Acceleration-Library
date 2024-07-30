# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import pathlib
from shutil import copytree

from cosmotech_api.api.dataset_api import DatasetApi
from cosmotech_api.api.runner_api import RunnerApi
from cosmotech_api.api.workspace_api import WorkspaceApi

from CosmoTech_Acceleration_Library.Accelerators.scenario_download.scenario_downloader import ScenarioDownloader
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import require_env
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


def download_runner_data(organization_id: str, workspace_id: str, runner_id: str, parameter_folder: str) -> None:
    """
    Download the datas from a scenario from the CosmoTech API to the local file system
    :param organization_id: The id of the Organization as defined in the CosmoTech API
    :param workspace_id: The id of the Workspace as defined in the CosmoTech API
    :param parameter_folder: a local folder where all parameters will be downloaded
    :return: Nothing
    """
    LOGGER.info("Starting the Run data download")
    parameters = list()
    _dl = ScenarioDownloader(workspace_id=workspace_id, organization_id=organization_id, read_files=False)
    with get_api_client()[0] as api_client:
        runner_api_instance = RunnerApi(api_client)
        dataset_api_instance = DatasetApi(api_client)
        workspace_api_instance = WorkspaceApi(api_client)
        runner_data = runner_api_instance.get_runner(organization_id=organization_id,
                                                     workspace_id=workspace_id,
                                                     runner_id=runner_id)
        LOGGER.info("Loaded run data")
        # Pre-read of all workspace files to ensure ready to download AZ storage files
        all_api_files = workspace_api_instance.find_all_workspace_files(
            organization_id=organization_id,
            workspace_id=workspace_id)

        max_name_size = max(map(lambda r: len(r.parameter_id), runner_data.parameters_values))
        max_type_size = max(map(lambda r: len(r.var_type), runner_data.parameters_values))
        # Loop over all parameters
        for parameter in runner_data.parameters_values:
            value = parameter.value
            var_type = parameter.var_type
            param_id = parameter.parameter_id
            is_inherited = parameter.is_inherited
            LOGGER.info(f"Found parameter '{param_id}' with value '{value}'")

            # Download "%DATASETID%" files if AZ storage + workspace file based
            if var_type == "%DATASETID%":
                _v = _dl.download_dataset(value)

                if isinstance(_v, tuple):
                    dataset_data = _v[0]
                    _dl.dataset_file_temp_path[_v[2]] = _v[1]
                else:
                    dataset_data = _v

                param_dir = os.path.join(parameter_folder, param_id)
                pathlib.Path(param_dir).mkdir(exist_ok=True, parents=True)
                copytree(_dl.dataset_to_file(value, dataset_data), param_dir, dirs_exist_ok=True)

                value = param_dir

            parameters.append({
                "parameterId": param_id,
                "value": value,
                "varType": var_type,
                "isInherited": is_inherited
            })
            LOGGER.debug(f"  - [yellow]{param_id:<{max_name_size}}[/] [cyan]{var_type:<{max_type_size}}[/] "
                         f"\"{value}\"{' [red bold]inherited[/]' if is_inherited else ''}")

        write_parameters(parameter_folder, parameters)


def write_parameters(parameter_folder, parameters):
    pathlib.Path(parameter_folder).mkdir(exist_ok=True, parents=True)
    tmp_parameter_file = os.path.join(parameter_folder, "parameters.json")
    LOGGER.info(f"Generating {tmp_parameter_file}")
    with open(tmp_parameter_file, "w") as _file:
        json.dump(parameters, _file, indent=2)


@click.command()
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              help="The id of an organization in the cosmotech api",
              metavar="o-##########",
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              help="The id of a workspace in the cosmotech api",
              metavar="w-##########",
              required=True)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              show_envvar=True,
              help="The id of a runner in the cosmotech api",
              metavar="s-##########",
              required=True)
@click.option("--parameters-absolute-path",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              metavar="PATH",
              show_envvar=True,
              help="A local folder to store the parameters content",
              required=True)
@require_env('CSM_API_SCOPE', "The identification scope of a Cosmotech API")
@require_env('CSM_API_URL', "The URL to a Cosmotech API")
@web_help("csm-data/api/run-load-data")
def run_load_data(
    runner_id: str,
    workspace_id: str,
    organization_id: str,
    parameters_absolute_path: str,
):
    """
Download a runner data from the Cosmo Tech API
Requires a valid Azure connection either with:
- The AZ cli command: **az login**
- A triplet of env var `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
    """
    return download_runner_data(organization_id, workspace_id, runner_id, parameters_absolute_path)


if __name__ == "__main__":
    run_load_data()
