# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import cosmotech_api

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.workspace import upload_workspace_file


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
              help="An API key configured in your Cosmo Tech tenant allowed to access workspace/files",
              metavar="API_KEY",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key-header",
              envvar="CSM_API_KEY_HEADER",
              help="The header configured in your api to send an API key",
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
@click.option("--file-path",
              help="Path to the file to send as a workspace file",
              metavar="PATH",
              type=str,
              required=True)
@click.option("--workspace-path",
              help="Path inside the workspace to store the file (end with '/' for a folder)",
              metavar="PATH",
              default="/",
              type=str,
              required=True)
@click.option("--overwrite/--keep",
              help="Flag to overwrite the target file",
              is_flag=True,
              default=True,
              show_default=True,
              type=bool)
def wsf_send_file_command(
    api_url,
    api_key,
    api_key_header,
    organization_id,
    workspace_id,
    file_path,
    workspace_path: str,
    overwrite: bool
):
    """Send a file to a workspace inside the API

Requires a valid connection to the API to send the data

This implementation make use of an API Key
    """

    configuration = cosmotech_api.Configuration(
        host=api_url,
    )

    with cosmotech_api.ApiClient(configuration,
                                 api_key_header,
                                 api_key) as api_client:
        upload_workspace_file(api_client,
                              organization_id,
                              workspace_id,
                              file_path,
                              workspace_path,
                              overwrite)
