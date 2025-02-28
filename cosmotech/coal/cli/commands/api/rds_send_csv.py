# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.orchestrator.utils.translate import T


@click.command()
@click.option(
    "--source-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("coal-help.commands.api.rds_send_csv.parameters.source_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--run-id",
    envvar="CSM_RUN_ID",
    help=T("coal-help.commands.api.rds_send_csv.parameters.run_id"),
    metavar="run-XXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@web_help("csm-data/api/rds-send-csv")
@translate_help("coal-help.commands.api.rds_send_csv.description")
def rds_send_csv(source_folder, organization_id, workspace_id, runner_id, run_id):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api import send_csv_to_run_data

    try:
        send_csv_to_run_data(
            source_folder=source_folder,
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
            run_id=run_id,
        )
    except FileNotFoundError:
        return 1
