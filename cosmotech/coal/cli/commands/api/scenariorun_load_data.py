# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import pathlib
import shutil
from csv import DictWriter

from CosmoTech_Acceleration_Library.Accelerators.scenario_download.scenario_downloader import ScenarioDownloader
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


def download_scenario_data(
    organization_id: str,
    workspace_id: str,
    scenario_id: str,
    dataset_folder: str,
    parameter_folder: str,
    write_json: bool,
    write_csv: bool,
    fetch_dataset: bool,
    parallel_download: bool,
) -> None:
    """
    Download the datas from a scenario from the CosmoTech API to the local file system
    :param scenario_id: The id of the Scenario as defined in the CosmoTech API
    :param organization_id: The id of the Organization as defined in the CosmoTech API
    :param workspace_id: The id of the Workspace as defined in the CosmoTech API
    :param dataset_folder: a local folder where the main dataset of the scenario will be downloaded
    :param parameter_folder: a local folder where all parameters will be downloaded
    :param write_json: should parameters be written as json file
    :param write_csv: should parameters be written as csv file
    :return: Nothing
    """
    LOGGER.info("Starting connector")
    dl = ScenarioDownloader(workspace_id=workspace_id,
                            organization_id=organization_id,
                            read_files=False,
                            parallel=parallel_download)

    LOGGER.info("Load scenario data")
    scenario_data = dl.get_scenario_data(scenario_id=scenario_id)
    LOGGER.info("Download datasets")
    if fetch_dataset:
        datasets = dl.get_all_datasets(scenario_id=scenario_id)
        datasets_parameters_ids = {param.value: param.parameter_id
                                   for param in scenario_data.parameters_values
                                   if param.var_type == "%DATASETID%"}

        LOGGER.info("Store datasets")
        pathlib.Path(dataset_folder).mkdir(parents=True, exist_ok=True)
        for k in datasets.keys():
            if k in scenario_data.dataset_list:
                shutil.copytree(dl.dataset_to_file(k, datasets[k]), dataset_folder, dirs_exist_ok=True)
                LOGGER.debug(f"  - [yellow]{dataset_folder}[/] ([green]{k}[/])")
            if k in datasets_parameters_ids.keys():
                param_dir = os.path.join(parameter_folder, datasets_parameters_ids[k])
                pathlib.Path(param_dir).mkdir(exist_ok=True, parents=True)
                shutil.copytree(dl.dataset_to_file(k, datasets[k]), param_dir, dirs_exist_ok=True)
                LOGGER.debug(f"  - [yellow]{datasets_parameters_ids[k]}[/] ([green]{k}[/])")
    else:
        LOGGER.info("No dataset write asked, skipping")

    pathlib.Path(parameter_folder).mkdir(parents=True, exist_ok=True)

    LOGGER.info("Prepare parameters")

    if not (write_csv or write_json):
        LOGGER.info("No parameters write asked, skipping")
        return

    parameters = []
    if scenario_data.parameters_values:
        max_name_size = max(map(lambda r: len(r.parameter_id), scenario_data.parameters_values))
        max_type_size = max(map(lambda r: len(r.var_type), scenario_data.parameters_values))
        for parameter_data in scenario_data.parameters_values:
            parameter_name = parameter_data.parameter_id
            value = parameter_data.value
            var_type = parameter_data.var_type
            is_inherited = parameter_data.is_inherited
            parameters.append({
                "parameterId": parameter_name,
                "value": value,
                "varType": var_type,
                "isInherited": is_inherited
            })
            LOGGER.debug(f"  - [yellow]{parameter_name:<{max_name_size}}[/] [cyan]{var_type:<{max_type_size}}[/] "
                         f"\"{value}\"{' [red bold]inherited[/]' if is_inherited else ''}")
        write_parameters(parameter_folder, parameters, write_csv, write_json)


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
@click.option("--scenario-id",
              envvar="CSM_SCENARIO_ID",
              show_envvar=True,
              help="The id of a scenario in the cosmotech api",
              metavar="s-##########",
              required=True)
@click.option("--dataset-absolute-path",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              help="A local folder to store the main dataset content",
              metavar="PATH",
              required=True)
@click.option("--parameters-absolute-path",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              metavar="PATH",
              show_envvar=True,
              help="A local folder to store the parameters content",
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
@click.option("--fetch-dataset/--no-fetch-dataset",
              envvar="FETCH_DATASET",
              show_envvar=True,
              default=True,
              show_default=True,
              help="Toggle fetching datasets")
@click.option("--parallel/--no-parallel",
              envvar="FETCH_DATASETS_IN_PARALLEL",
              show_envvar=True,
              default=True,
              show_default=True,
              help="Toggle parallelization while fetching datasets,")
@web_help("csm-data/api/scenariorun-load-data")
def scenariorun_load_data(
    scenario_id: str,
    workspace_id: str,
    organization_id: str,
    dataset_absolute_path: str,
    parameters_absolute_path: str,
    write_json: bool,
    write_csv: bool,
    fetch_dataset: bool,
    parallel: bool
):
    """
Uses environment variables to call the download_scenario_data function
Requires a valid Azure connection either with:
- The AZ cli command: **az login**
- A triplet of env var `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
    """
    return download_scenario_data(organization_id, workspace_id, scenario_id, dataset_absolute_path,
                                  parameters_absolute_path, write_json, write_csv, fetch_dataset, parallel)


if __name__ == "__main__":
    scenariorun_load_data()
