# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import os
import pathlib
from shutil import copytree

from cosmotech_api.api.runner_api import RunnerApi
from cosmotech_api.api.workspace_api import WorkspaceApi

from cosmotech.coal.scenario.download import download_runner_data
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import require_env
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def download_data(organization_id: str, workspace_id: str, runner_id: str, parameter_folder: str) -> None:
    """
    Download the data from a runner from the CosmoTech API to the local file system
    
    Args:
        organization_id: The id of the Organization as defined in the CosmoTech API
        workspace_id: The id of the Workspace as defined in the CosmoTech API
        runner_id: The id of the Runner as defined in the CosmoTech API
        parameter_folder: a local folder where all parameters will be downloaded
    """
    download_runner_data(
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=runner_id,
        parameter_folder=parameter_folder,
        read_files=False,
        write_json=True,
        write_csv=False,
        fetch_dataset=True
    )


@click.command()
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              help=T("coal-help.commands.api.run_load_data.parameters.organization_id"),
              metavar="o-##########",
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              help=T("coal-help.commands.api.run_load_data.parameters.workspace_id"),
              metavar="w-##########",
              required=True)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              show_envvar=True,
              help=T("coal-help.commands.api.run_load_data.parameters.runner_id"),
              metavar="s-##########",
              required=True)
@click.option("--parameters-absolute-path",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              metavar="PATH",
              show_envvar=True,
              help=T("coal-help.commands.api.run_load_data.parameters.parameters_absolute_path"),
              required=True)
@require_env('CSM_API_SCOPE', "The identification scope of a Cosmotech API")
@require_env('CSM_API_URL', "The URL to a Cosmotech API")
@web_help("csm-data/api/run-load-data")
@translate_help("coal-help.commands.api.run_load_data.description")
def run_load_data(
    runner_id: str,
    workspace_id: str,
    organization_id: str,
    parameters_absolute_path: str,
):
    return download_data(organization_id, workspace_id, runner_id, parameters_absolute_path)


if __name__ == "__main__":
    run_load_data()
