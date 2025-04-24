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
@web_help("csm-data/store/list-tables")
@translate_help("csm-data.commands.store.list_tables.description")
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("csm-data.commands.store.list_tables.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--schema/--no-schema",
    help=T("csm-data.commands.store.list_tables.parameters.schema"),
    is_flag=True,
    type=bool,
    default=False,
)
def list_tables(store_folder, schema):
    # Import the modules and functions at the start of the command
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.utils.logger import LOGGER

    _s = Store(store_location=store_folder)
    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info(T("coal.services.database.store_tables"))
        for table_name in tables:
            LOGGER.info(T("coal.services.database.table_entry").format(table=table_name))
            if schema:
                LOGGER.info(
                    T("coal.services.database.table_schema").format(schema=str(_s.get_table_schema(table_name)))
                )
    else:
        LOGGER.info(T("coal.services.database.store_empty"))
