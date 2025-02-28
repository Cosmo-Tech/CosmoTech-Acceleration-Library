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
@web_help("csm-data/store/dump-to-postgres")
@translate_help("csm-data.commands.store.dump_to_postgresql.description")
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--table-prefix",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.table_prefix"),
    metavar="PREFIX",
    type=str,
    default="Cosmotech_",
)
@click.option(
    "--postgres-host",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.postgres_host"),
    envvar="POSTGRES_HOST_URI",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-port",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.postgres_port"),
    envvar="POSTGRES_HOST_PORT",
    show_envvar=True,
    required=False,
    default=5432,
)
@click.option(
    "--postgres-db",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.postgres_db"),
    envvar="POSTGRES_DB_NAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-schema",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.postgres_schema"),
    envvar="POSTGRES_DB_SCHEMA",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-user",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.postgres_user"),
    envvar="POSTGRES_USER_NAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--postgres-password",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.postgres_password"),
    envvar="POSTGRES_USER_PASSWORD",
    show_envvar=True,
    required=True,
)
@click.option(
    "--replace/--append",
    "replace",
    help=T("csm-data.commands.store.dump_to_postgresql.parameters.replace"),
    default=True,
    is_flag=True,
    show_default=True,
)
def dump_to_postgresql(
    store_folder,
    table_prefix: str,
    postgres_host,
    postgres_port,
    postgres_db,
    postgres_schema,
    postgres_user,
    postgres_password,
    replace: bool,
):
    # Import the function at the start of the command
    from cosmotech.coal.postgresql import dump_store_to_postgresql

    dump_store_to_postgresql(
        store_folder=store_folder,
        table_prefix=table_prefix,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_db=postgres_db,
        postgres_schema=postgres_schema,
        postgres_user=postgres_user,
        postgres_password=postgres_password,
        replace=replace,
    )
