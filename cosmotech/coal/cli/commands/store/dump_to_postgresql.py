# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from time import perf_counter

from adbc_driver_postgresql import dbapi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER


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

    postgresql_full_uri = (f'postgresql://'
                           f'{postgres_user}'
                           f':{postgres_password}'
                           f'@{postgres_host}'
                           f':{postgres_port}'
                           f'/{postgres_db}')

    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info(f"Sending tables to [green bold]{postgres_db}.{postgres_schema}[/]")
        total_rows = 0
        _process_start = perf_counter()
        with dbapi.connect(postgresql_full_uri, autocommit=True) as conn:
            for table_name in tables:
                with conn.cursor() as curs:
                    _s_time = perf_counter()
                    target_table_name = f"{table_prefix}{table_name}"
                    data = _s.get_table(table_name)
                    _dl_time = perf_counter()
                    rows = curs.adbc_ingest(
                        target_table_name,
                        data,
                        "replace" if replace else "create_append",
                        db_schema_name=postgres_schema)
                    total_rows += rows
                    _up_time = perf_counter()
                    LOGGER.info(f"  - [yellow]{target_table_name}[/] : [cyan bold]{rows}[/] rows")
                    LOGGER.debug(f"   -> Load from datastore took [blue]{_dl_time - _s_time:0.3}s[/]")
                    LOGGER.debug(f"   -> Send to postgresql took [blue]{_up_time - _dl_time:0.3}s[/]")
        _process_end = perf_counter()
        LOGGER.info(f"Sent [cyan bold]{total_rows}[/] rows "
                    f"in [blue]{_process_end - _process_start:0.3}s[/]")
    else:
        LOGGER.info("Data store is empty")
