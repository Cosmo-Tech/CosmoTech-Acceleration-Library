# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.csm_data.commands.api.postgres_send_runner_metadata import (
    postgres_send_runner_metadata,
)
from cosmotech.csm_data.commands.api.rds_load_csv import rds_load_csv
from cosmotech.csm_data.commands.api.rds_send_csv import rds_send_csv
from cosmotech.csm_data.commands.api.rds_send_store import rds_send_store
from cosmotech.csm_data.commands.api.run_load_data import run_load_data
from cosmotech.csm_data.commands.api.runtemplate_load_handler import (
    runtemplate_load_handler,
)
from cosmotech.csm_data.commands.api.tdl_load_files import tdl_load_files
from cosmotech.csm_data.commands.api.tdl_send_files import tdl_send_files
from cosmotech.csm_data.commands.api.wsf_load_file import wsf_load_file
from cosmotech.csm_data.commands.api.wsf_send_file import wsf_send_file
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


@click.group(invoke_without_command=True)
@web_help("csm-data/api")
@click.pass_context
@translate_help("csm-data.commands.api.description")
def api(ctx: click.Context):
    if ctx.invoked_subcommand is None:
        try:
            api_client, description = get_api_client()
            LOGGER.info(T("coal.cosmotech_api.connection.found_valid").format(type=description))
        except EnvironmentError:
            raise click.Abort()


api.add_command(rds_send_csv, "rds-send-csv")
api.add_command(rds_send_store, "rds-send-store")
api.add_command(rds_load_csv, "rds-load-csv")
api.add_command(wsf_send_file, "wsf-send-file")
api.add_command(wsf_load_file, "wsf-load-file")
api.add_command(tdl_send_files, "tdl-send-files")
api.add_command(tdl_load_files, "tdl-load-files")
api.add_command(runtemplate_load_handler, "runtemplate-load-handler")
api.add_command(run_load_data, "run-load-data")
api.add_command(postgres_send_runner_metadata, "postgres-send-runner-metadata")
