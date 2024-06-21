# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib

import cosmotech_api

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.workspace import download_workspace_file
from cosmotech.coal.cosmotech_api.workspace import list_workspace_files


@click.command()
@click.option("--api-url",
              envvar="CSM_API_URL",
              help="An URL to a Cosmo Tech API tenant",
              metavar="URL",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key",
              envvar="CSM_API_KEY",
              help="An API key configured in your Cosmo Tech tenant allowed to access runs/data/send",
              metavar="API_KEY",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key-header",
              envvar="CSM_API_KEY_HEADER",
              help="An API key configured in your Cosmo Tech tenant allowed to access runs/data/send",
              metavar="HEADER",
              type=str,
              show_envvar=True,
              show_default=True,
              default="X-CSM-API-KEY")
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help="An organization id for the Cosmo Tech API",
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              help="A workspace id for the Cosmo Tech API",
              metavar="w-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--workspace-path",
              help="Path inside the workspace to load (end with '/' for a folder)",
              metavar="PATH",
              default="/",
              type=str)
@click.option("--target-folder",
              help="Folder in which to send the downloaded file",
              metavar="PATH",
              default="./",
              type=str,
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              required=True)
def wsf_load_file_command(
    api_url,
    api_key,
    api_key_header,
    organization_id,
    workspace_id,
    workspace_path: str,
    target_folder: str
):
    """Load files from a workspace inside the API

Requires a valid connection to the API to send the data

This implementation make use of an API Key
    """
    configuration = cosmotech_api.Configuration(
        host=api_url,
    )

    with cosmotech_api.ApiClient(configuration,
                                 api_key_header,
                                 api_key) as api_client:
        target_list = list_workspace_files(api_client,
                                           organization_id,
                                           workspace_id,
                                           workspace_path)

        for target in target_list:
            download_workspace_file(api_client,
                                    organization_id,
                                    workspace_id,
                                    target,
                                    pathlib.Path(target_folder))
