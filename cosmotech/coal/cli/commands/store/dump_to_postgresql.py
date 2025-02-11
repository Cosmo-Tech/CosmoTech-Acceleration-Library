# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from time import perf_counter

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.coal.utils.postgresql import send_pyarrow_table_to_postgresql


@click.command()
@web_help("csm-data/store/dump-to-postgres")
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help="The folder containing the store files",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--table-prefix",
              help="Prefix to add to the table name",
              metavar="PREFIX",
              type=str,
              default="Cosmotech_")
@click.option('--postgres-host',
              help='Postgresql host URI',
              envvar="POSTGRES_HOST_URI",
              show_envvar=True,
              required=True)
@click.option('--postgres-port',
              help='Postgresql database port',
              envvar="POSTGRES_HOST_PORT",
              show_envvar=True,
              required=False,
              default=5432)
@click.option('--postgres-db',
              help='Postgresql database name',
              envvar="POSTGRES_DB_NAME",
              show_envvar=True,
              required=True)
@click.option('--postgres-schema',
              help='Postgresql schema name',
              envvar="POSTGRES_DB_SCHEMA",
              show_envvar=True,
              required=True)
@click.option('--postgres-user',
              help='Postgresql connection user name',
              envvar="POSTGRES_USER_NAME",
              show_envvar=True,
              required=True)
@click.option('--postgres-password',
              help='Postgresql connection password',
              envvar="POSTGRES_USER_PASSWORD",
              show_envvar=True,
              required=True)
@click.option("--replace/--append",
              "replace",
              help="Append data on existing tables",
              default=True,
              is_flag=True,
              show_default=True)
def dump_to_postgresql(
    store_folder,
    table_prefix: str,
    postgres_host,
    postgres_port,
    postgres_db,
    postgres_schema,
    postgres_user,
    postgres_password,
    replace: bool
):
    """Running this command will dump your store to a given postgresql database

    Tables names from the store will be prepended with table-prefix in target database

    The postgresql user must have USAGE granted on the schema for this script to work due to the use of the command `COPY FROM STDIN`

    You can simply give him that grant by running the command :
    `GRANT USAGE ON SCHEMA <schema> TO <username>`
    """
    _s = Store(store_location=store_folder)

    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info(f"Sending tables to {postgres_db}.{postgres_schema} ")
        total_rows = 0
        _process_start = perf_counter()
        for table_name in tables:
            _s_time = perf_counter()
            target_table_name = f"{table_prefix}{table_name}"
            LOGGER.info(f"  - {target_table_name} :")
            data = _s.get_table(table_name)
            if not len(data):
                LOGGER.info(f"   -> 0 rows (skipping)")
                continue
            _dl_time = perf_counter()
            rows = send_pyarrow_table_to_postgresql(data,
                                                    target_table_name,
                                                    postgres_host,
                                                    postgres_port,
                                                    postgres_db,
                                                    postgres_schema,
                                                    postgres_user,
                                                    postgres_password,
                                                    replace)
            total_rows += rows
            _up_time = perf_counter()
            LOGGER.info(f"   -> {rows} rows")
            LOGGER.debug(f"   -> Load from datastore took {_dl_time - _s_time:0.3}s ")
            LOGGER.debug(f"   -> Send to postgresql took {_up_time - _dl_time:0.3}s ")
        _process_end = perf_counter()
        LOGGER.info(f"Sent {total_rows} rows "
                    f"in {_process_end - _process_start:0.3}s ")
    else:
        LOGGER.info("Data store is empty")
