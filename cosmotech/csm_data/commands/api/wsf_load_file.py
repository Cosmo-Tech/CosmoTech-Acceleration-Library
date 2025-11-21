# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib

from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.command()
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm_data.commands.api.wsf_load_file.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm_data.commands.api.wsf_load_file.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-path",
    help=T("csm_data.commands.api.wsf_load_file.parameters.workspace_path"),
    metavar="PATH",
    default="/",
    type=str,
)
@click.option(
    "--target-folder",
    help=T("csm_data.commands.api.wsf_load_file.parameters.target_folder"),
    metavar="PATH",
    default="./",
    type=str,
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    required=True,
)
@web_help("csm-data/api/wsf-load-file")
@translate_help("csm_data.commands.api.wsf_load_file.description")
def wsf_load_file(organization_id, workspace_id, workspace_path: str, target_folder: str):
    from cosmotech.coal.cosmotech_api.apis.workspace import WorkspaceApi

    ws_api = WorkspaceApi()
    target_list = ws_api.list_filtered_workspace_files(organization_id, workspace_id, workspace_path)

    for target in target_list:
        ws_api.download_workspace_file(
            organization_id,
            workspace_id,
            target,
            pathlib.Path(target_folder),
        )
