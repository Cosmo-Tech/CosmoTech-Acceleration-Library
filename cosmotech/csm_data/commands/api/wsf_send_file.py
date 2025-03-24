# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.workspace import upload_workspace_file
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm-data.commands.api.wsf_send_file.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm-data.commands.api.wsf_send_file.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--file-path",
    help=T("csm-data.commands.api.wsf_send_file.parameters.file_path"),
    metavar="PATH",
    type=str,
    required=True,
)
@click.option(
    "--workspace-path",
    help=T("csm-data.commands.api.wsf_send_file.parameters.workspace_path"),
    metavar="PATH",
    default="/",
    type=str,
    required=True,
)
@click.option(
    "--overwrite/--keep",
    help=T("csm-data.commands.api.wsf_send_file.parameters.overwrite"),
    is_flag=True,
    default=True,
    show_default=True,
    type=bool,
)
@web_help("csm-data/api/wsf-send-file")
@translate_help("csm-data.commands.api.wsf_send_file.description")
def wsf_send_file(organization_id, workspace_id, file_path, workspace_path: str, overwrite: bool):
    with get_api_client()[0] as api_client:
        upload_workspace_file(
            api_client,
            organization_id,
            workspace_id,
            file_path,
            workspace_path,
            overwrite,
        )
