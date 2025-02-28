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
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm-data.commands.api.tdl_load_files.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm-data.commands.api.tdl_load_files.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--scenario-id",
    envvar="CSM_SCENARIO_ID",
    help=T("csm-data.commands.api.tdl_load_files.parameters.scenario_id"),
    metavar="s-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=False,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("csm-data.commands.api.tdl_load_files.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=False,
)
@click.option(
    "--dir",
    "directory_path",
    help=T("csm-data.commands.api.tdl_load_files.parameters.dir"),
    metavar="PATH",
    default="./",
    type=str,
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    show_envvar=True,
    required=True,
)
@web_help("csm-data/api/tdl-load-file")
@translate_help("csm-data.commands.api.tdl_load_files.description")
def tdl_load_files(organization_id, workspace_id, scenario_id, runner_id, directory_path):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api import load_files_from_tdl

    try:
        load_files_from_tdl(
            organization_id=organization_id,
            workspace_id=workspace_id,
            directory_path=directory_path,
            runner_id=runner_id,
            scenario_id=scenario_id,
        )
    except ValueError as e:
        raise click.Abort() from e
