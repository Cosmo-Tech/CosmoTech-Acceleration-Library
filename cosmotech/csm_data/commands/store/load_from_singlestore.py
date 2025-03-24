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
@web_help("csm-data/store/load-from-singlestore")
@translate_help("csm-data.commands.store.load_from_singlestore.description")
@click.option(
    "--singlestore-host",
    "single_store_host",
    envvar="SINGLE_STORE_HOST",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.singlestore_host"),
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-port",
    "single_store_port",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.singlestore_port"),
    envvar="SINGLE_STORE_PORT",
    show_envvar=True,
    required=False,
    default=3306,
)
@click.option(
    "--singlestore-db",
    "single_store_db",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.singlestore_db"),
    envvar="SINGLE_STORE_DB",
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-user",
    "single_store_user",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.singlestore_user"),
    envvar="SINGLE_STORE_USERNAME",
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-password",
    "single_store_password",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.singlestore_password"),
    envvar="SINGLE_STORE_PASSWORD",
    show_envvar=True,
    required=True,
)
@click.option(
    "--singlestore-tables",
    "single_store_tables",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.singlestore_tables"),
    envvar="SINGLE_STORE_TABLES",
    show_envvar=True,
    required=True,
)
@click.option(
    "--store-folder",
    "store_folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.load_from_singlestore.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
def load_from_singlestore_command(
    single_store_host,
    single_store_port,
    single_store_db,
    single_store_user,
    single_store_password,
    store_folder,
    single_store_tables: str = "",
):
    # Import the function at the start of the command
    from cosmotech.coal.singlestore import load_from_singlestore

    load_from_singlestore(
        single_store_host=single_store_host,
        single_store_port=single_store_port,
        single_store_db=single_store_db,
        single_store_user=single_store_user,
        single_store_password=single_store_password,
        store_folder=store_folder,
        single_store_tables=single_store_tables,
    )
