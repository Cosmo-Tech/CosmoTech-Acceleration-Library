# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.csm_data.commands.api.postgres_send_runner_metadata import (
    postgres_send_runner_metadata,
)
from cosmotech.csm_data.commands.api.run_load_data import run_load_data
from cosmotech.csm_data.commands.api.wsf_load_file import wsf_load_file
from cosmotech.csm_data.commands.api.wsf_send_file import wsf_send_file
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.group(invoke_without_command=True)
@web_help("csm-data/api")
@click.pass_context
@translate_help("csm_data.commands.api.api.description")
def api(ctx: click.Context):
    if ctx.invoked_subcommand is None:
        try:
            connection = Connection()
            LOGGER.info(T("coal.cosmotech_api.connection.found_valid").format(type=connection.api_type))
        except EnvironmentError:
            raise click.Abort()


api.add_command(wsf_send_file, "wsf-send-file")
api.add_command(wsf_load_file, "wsf-load-file")
api.add_command(run_load_data, "run-load-data")
api.add_command(postgres_send_runner_metadata, "postgres-send-runner-metadata")
