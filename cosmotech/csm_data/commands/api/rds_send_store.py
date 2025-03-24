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
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.api.rds_send_store.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm-data.commands.api.rds_send_store.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm-data.commands.api.rds_send_store.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("csm-data.commands.api.rds_send_store.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--run-id",
    envvar="CSM_RUN_ID",
    help=T("csm-data.commands.api.rds_send_store.parameters.run_id"),
    metavar="run-XXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@web_help("csm-data/api/rds-send-store")
@translate_help("csm-data.commands.api.rds_send_store.description")
def rds_send_store(store_folder, organization_id, workspace_id, runner_id, run_id):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api import send_store_to_run_data

    try:
        send_store_to_run_data(
            store_folder=store_folder,
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
            run_id=run_id,
        )
    except FileNotFoundError:
        return 1
