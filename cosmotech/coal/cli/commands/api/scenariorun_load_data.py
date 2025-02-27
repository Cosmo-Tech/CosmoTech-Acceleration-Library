# Copyright (C) - 2023 - 2025 - Cosmo Tech
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

from cosmotech.coal.cosmotech_api.runner.download import download_run_data as download_scenario
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def download_data(
    organization_id: str,
    workspace_id: str,
    scenario_id: str,
    dataset_folder: str,
    parameter_folder: str,
    write_json: bool,
    write_csv: bool,
    fetch_dataset: bool,
    parallel: bool,
) -> None:
    """
    Download the data from a runner from the CosmoTech API to the local file system
    
    Args:
        organization_id: The id of the Organization as defined in the CosmoTech API
        workspace_id: The id of the Workspace as defined in the CosmoTech API
        scenario_id: The id of the Runner as defined in the CosmoTech API (kept as scenario_id for backward compatibility)
        dataset_folder: a local folder where the main dataset of the runner will be downloaded
        parameter_folder: a local folder where all parameters will be downloaded
        write_json: should parameters be written as json file
        write_csv: should parameters be written as csv file
        fetch_dataset: whether to fetch datasets
        parallel: whether to download datasets in parallel
    """
    download_scenario(
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=scenario_id,  # Use runner_id parameter name with scenario_id value
        parameter_folder=parameter_folder,
        dataset_folder=dataset_folder,
        read_files=False,
        parallel=parallel,
        write_json=write_json,
        write_csv=write_csv,
        fetch_dataset=fetch_dataset
    )


@click.command()
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.organization_id"),
              metavar="o-##########",
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.workspace_id"),
              metavar="w-##########",
              required=True)
@click.option("--scenario-id",
              envvar="CSM_SCENARIO_ID",
              show_envvar=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.scenario_id"),
              metavar="s-##########",
              required=True)
@click.option("--dataset-absolute-path",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.dataset_absolute_path"),
              metavar="PATH",
              required=True)
@click.option("--parameters-absolute-path",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              metavar="PATH",
              show_envvar=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.parameters_absolute_path"),
              required=True)
@click.option("--write-json/--no-write-json",
              envvar="WRITE_JSON",
              show_envvar=True,
              default=False,
              show_default=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.write_json"))
@click.option("--write-csv/--no-write-csv",
              envvar="WRITE_CSV",
              show_envvar=True,
              default=True,
              show_default=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.write_csv"))
@click.option("--fetch-dataset/--no-fetch-dataset",
              envvar="FETCH_DATASET",
              show_envvar=True,
              default=True,
              show_default=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.fetch_dataset"))
@click.option("--parallel/--no-parallel",
              envvar="FETCH_DATASETS_IN_PARALLEL",
              show_envvar=True,
              default=True,
              show_default=True,
              help=T("coal-help.commands.api.scenariorun_load_data.parameters.parallel"))
@web_help("csm-data/api/scenariorun-load-data")
@translate_help("coal-help.commands.api.scenariorun_load_data.description")
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
    return download_data(organization_id, workspace_id, scenario_id, dataset_absolute_path,
                        parameters_absolute_path, write_json, write_csv, fetch_dataset, parallel)


if __name__ == "__main__":
    scenariorun_load_data()
