# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from time import perf_counter

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.utils.postgresql import send_pyarrow_table_to_postgresql


@click.command()
@web_help("csm-data/store/dump-to-postgres")
@translate_help("coal-help.commands.store.dump_to_postgresql.description")
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.store_folder"),
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--table-prefix",
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.table_prefix"),
              metavar="PREFIX",
              type=str,
              default="Cosmotech_")
@click.option('--postgres-host',
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.postgres_host"),
              envvar="POSTGRES_HOST_URI",
              show_envvar=True,
              required=True)
@click.option('--postgres-port',
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.postgres_port"),
              envvar="POSTGRES_HOST_PORT",
              show_envvar=True,
              required=False,
              default=5432)
@click.option('--postgres-db',
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.postgres_db"),
              envvar="POSTGRES_DB_NAME",
              show_envvar=True,
              required=True)
@click.option('--postgres-schema',
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.postgres_schema"),
              envvar="POSTGRES_DB_SCHEMA",
              show_envvar=True,
              required=True)
@click.option('--postgres-user',
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.postgres_user"),
              envvar="POSTGRES_USER_NAME",
              show_envvar=True,
              required=True)
@click.option('--postgres-password',
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.postgres_password"),
              envvar="POSTGRES_USER_PASSWORD",
              show_envvar=True,
              required=True)
@click.option("--replace/--append",
              "replace",
              help=T("coal-help.commands.store.dump_to_postgresql.parameters.replace"),
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
    _s = Store(store_location=store_folder)

    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info(T("coal.logs.database.sending_data").format(table=f"{postgres_db}.{postgres_schema}"))
        total_rows = 0
        _process_start = perf_counter()
        for table_name in tables:
            _s_time = perf_counter()
            target_table_name = f"{table_prefix}{table_name}"
            LOGGER.info(T("coal.logs.database.table_entry").format(table=target_table_name))
            data = _s.get_table(table_name)
            if not len(data):
                LOGGER.info(T("coal.logs.database.no_rows"))
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
            LOGGER.info(T("coal.logs.database.row_count").format(count=rows))
            LOGGER.debug(T("coal.logs.progress.operation_timing").format(
                operation="Load from datastore",
                time=f"{_dl_time - _s_time:0.3}"
            ))
            LOGGER.debug(T("coal.logs.progress.operation_timing").format(
                operation="Send to postgresql",
                time=f"{_up_time - _dl_time:0.3}"
            ))
        _process_end = perf_counter()
        LOGGER.info(T("coal.logs.database.rows_fetched").format(
            table="all tables",
            count=total_rows,
            time=f"{_process_end - _process_start:0.3}"
        ))
    else:
        LOGGER.info(T("coal.logs.database.store_empty"))
