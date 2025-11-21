# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.command()
@web_help("csm-data/api/postgres-send-runner-metadata")
@translate_help("csm_data.commands.api.postgres_send_runner_metadata.description")
@click.option(
    "--organization-id",
    envvar="CSM_ORGANIZATION_ID",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.organization_id"),
    metavar="o-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--workspace-id",
    envvar="CSM_WORKSPACE_ID",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.workspace_id"),
    metavar="w-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--runner-id",
    envvar="CSM_RUNNER_ID",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.runner_id"),
    metavar="r-XXXXXXXX",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--table-prefix",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.table_prefix"),
    metavar="PREFIX",
    type=str,
    default="Cosmotech_",
)
@click.option(
    "--postgres-host",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.postgres_host"),
    envvar="POSTGRES_HOST_URI",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-port",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.postgres_port"),
    envvar="POSTGRES_HOST_PORT",
    show_envvar=True,
    required=False,
    default=5432,
)
@click.option(
    "--postgres-db",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.postgres_db"),
    envvar="POSTGRES_DB_NAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-schema",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.postgres_schema"),
    envvar="POSTGRES_DB_SCHEMA",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-user",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.postgres_user"),
    envvar="POSTGRES_USER_NAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-password",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.postgres_password"),
    envvar="POSTGRES_USER_PASSWORD",
    show_envvar=True,
    required=True,
)
@click.option(
    "--encode-password/--no-encode-password",
    "force_encode",
    help=T("csm_data.commands.api.postgres_send_runner_metadata.parameters.encode_password"),
    envvar="CSM_PSQL_FORCE_PASSWORD_ENCODING",
    show_envvar=True,
    default=True,
    is_flag=True,
    show_default=True,
)
def postgres_send_runner_metadata(
    organization_id,
    workspace_id,
    runner_id,
    table_prefix: str,
    postgres_host,
    postgres_port,
    postgres_db,
    postgres_schema,
    postgres_user,
    postgres_password,
    force_encode: bool,
):
    # Import the function at the start of the command
    from cosmotech.coal.postgresql import send_runner_metadata_to_postgresql
    from cosmotech.coal.utils.configuration import Configuration

    _c = Configuration()
    _c.postgres.host = postgres_host
    _c.postgres.port = postgres_port
    _c.postgres.db_name = postgres_db
    _c.postgres.db_schema = postgres_schema
    _c.postgres.user_name = postgres_user
    _c.postgres.user_password = postgres_password
    _c.postgres.password_encoding = force_encode
    _c.postgres.table_prefix = table_prefix

    _c.cosmotech.organization_id = organization_id
    _c.cosmotech.workspace_id = workspace_id
    _c.cosmotech.runner_id = runner_id

    send_runner_metadata_to_postgresql(_c)
