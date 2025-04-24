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
    "--target-folder",
    envvar="CSM_DATASET_ABSOLUTE_PATH",
    help=T("csm-data.commands.api.rds_load_csv.parameters.target_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm-data.commands.api.rds_load_csv.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm-data.commands.api.rds_load_csv.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("csm-data.commands.api.rds_load_csv.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--run-id",
    envvar="CSM_RUN_ID",
    help=T("csm-data.commands.api.rds_load_csv.parameters.run_id"),
    metavar="run-XXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--file-name",
    help=T("csm-data.commands.api.rds_load_csv.parameters.file_name"),
    metavar="NAME",
    type=str,
    default="results",
    show_default=True,
    required=True,
)
@click.option(
    "--query",
    help=T("csm-data.commands.api.rds_load_csv.parameters.query"),
    metavar="SQL_QUERY",
    type=str,
    default="SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
    show_default=True,
)
@web_help("csm-data/api/rds-load-csv")
@translate_help("csm-data.commands.api.rds_load_csv.description")
def rds_load_csv(target_folder, organization_id, workspace_id, runner_id, run_id, file_name, query):
    # Import the function at the start of the command
    from cosmotech.coal.cosmotech_api import load_csv_from_run_data

    load_csv_from_run_data(
        target_folder=target_folder,
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=runner_id,
        run_id=run_id,
        file_name=file_name,
        query=query,
    )
