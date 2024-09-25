# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@web_help("csm-data/store/list-tables")
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help="The folder containing the store files",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--schema/--no-schema",
              help="Display the schema of the tables",
              is_flag=True,
              type=bool,
              default=False)
def list_tables(store_folder, schema):
    """Running this command will list the existing tables in your datastore"""
    _s = Store(store_location=store_folder)
    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info("Data store contains the following tables")
        for table_name in tables:
            LOGGER.info(f"  - {table_name}")
            if schema:
                LOGGER.info(_s.get_table_schema(table_name))
    else:
        LOGGER.info("Data store is empty")
