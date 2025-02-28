# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.workspace import download_workspace_file
from cosmotech.coal.cosmotech_api.workspace import list_workspace_files
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm-data.commands.api.wsf_load_file.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm-data.commands.api.wsf_load_file.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-path",
    help=T("csm-data.commands.api.wsf_load_file.parameters.workspace_path"),
    metavar="PATH",
    default="/",
    type=str,
)
@click.option(
    "--target-folder",
    help=T("csm-data.commands.api.wsf_load_file.parameters.target_folder"),
    metavar="PATH",
    default="./",
    type=str,
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    required=True,
)
@web_help("csm-data/api/wsf-load-file")
@translate_help("csm-data.commands.api.wsf_load_file.description")
def wsf_load_file(organization_id, workspace_id, workspace_path: str, target_folder: str):
    with get_api_client()[0] as api_client:
        target_list = list_workspace_files(api_client, organization_id, workspace_id, workspace_path)

        for target in target_list:
            download_workspace_file(
                api_client,
                organization_id,
                workspace_id,
                target,
                pathlib.Path(target_folder),
            )
