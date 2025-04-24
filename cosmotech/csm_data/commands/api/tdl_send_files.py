# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--api-url",
    envvar="CSM_API_URL",
    help=T("csm-data.commands.api.tdl_send_files.parameters.api_url"),
    metavar="URI",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm-data.commands.api.tdl_send_files.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm-data.commands.api.tdl_send_files.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("csm-data.commands.api.tdl_send_files.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--dir",
    "directory_path",
    help=T("csm-data.commands.api.tdl_send_files.parameters.dir"),
    metavar="PATH",
    default="./",
    type=str,
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    required=True,
)
@click.option(
    "--clear/--keep",
    help=T("csm-data.commands.api.tdl_send_files.parameters.clear"),
    is_flag=True,
    default=True,
    show_default=True,
    type=bool,
)
@web_help("csm-data/api/tdl-send-files")
@translate_help("csm-data.commands.api.tdl_send_files.description")
def tdl_send_files(api_url, organization_id, workspace_id, runner_id, directory_path, clear: bool):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api import send_files_to_tdl

    send_files_to_tdl(
        api_url=api_url,
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=runner_id,
        directory_path=directory_path,
        clear=clear,
    )
