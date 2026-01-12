# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
PostgreSQL store operations module.

This module provides functions for interacting with PostgreSQL databases
for store operations.
"""

from time import perf_counter

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.postgresql.utils import PostgresUtils
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER


def dump_store_to_postgresql(
    store_folder: str,
    postgres_host: str,
    postgres_port: int,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
    table_prefix: str = "Cosmotech_",
    replace: bool = True,
    force_encode: bool = False,
    selected_tables: list[str] = [],
    fk_id: str = None,
) -> None:
    """
    Dump Store data to a PostgreSQL database.

    Args:
        store_folder: Folder containing the Store
        postgres_host: PostgreSQL host
        postgres_port: PostgreSQL port
        postgres_db: PostgreSQL database name
        postgres_schema: PostgreSQL schema
        postgres_user: PostgreSQL username
        postgres_password: PostgreSQL password
        table_prefix: Table prefix
        replace: Whether to replace existing tables
        force_encode: force password encoding to percent encoding
        selected_tables: list of tables to send
        fk_id: foreign key id to add to all table on all rows
    """
    _c = Configuration(
        {
            "coal": {"store": store_folder},
            "postgres": {
                "host": postgres_host,
                "port": postgres_port,
                "db_name": postgres_db,
                "db_schema": postgres_schema,
                "user_name": postgres_user,
                "user_password": postgres_password,
                "password_encoding": force_encode,
                "table_prefix": table_prefix,
            },
        }
    )

    dump_store_to_postgresql_from_conf(configuration=_c, replace=replace, selected_tables=selected_tables, fk_id=fk_id)


def dump_store_to_postgresql_from_conf(
    configuration: Configuration,
    replace: bool = True,
    selected_tables: list[str] = [],
    fk_id: str = None,
) -> None:
    """
    Dump Store data to a PostgreSQL database.

    Args:
        configuration: coal Configuration
        replace: Whether to replace existing tables
        selected_tables: list of tables to send
        fk_id: foreign key id to add to all table on all rows
    """
    _psql = PostgresUtils(configuration)
    _s = Store(configuration=configuration)

    tables = list(_s.list_tables())
    if selected_tables:
        tables = [t for t in tables if t in selected_tables]
    if len(tables):
        LOGGER.info(T("coal.services.database.sending_data").format(table=f"{_psql.db_name}.{_psql.db_schema}"))
        total_rows = 0
        _process_start = perf_counter()
        for table_name in tables:
            _s_time = perf_counter()
            target_table_name = f"{_psql.table_prefix}{table_name}"
            LOGGER.info(T("coal.services.database.table_entry").format(table=target_table_name))
            data = _s.get_table(table_name)
            if not len(data):
                LOGGER.info(T("coal.services.database.no_rows"))
                continue
            if fk_id:
                data = data.append_column("csm_run_id", [[fk_id] * data.num_rows])
            _dl_time = perf_counter()
            rows = _psql.send_pyarrow_table_to_postgresql(
                data,
                target_table_name,
                replace,
            )
            if fk_id and _psql.is_metadata_exists():
                metadata_table = f"{_psql.table_prefix}RunnerMetadata"
                _psql.add_fk_constraint(table_name, "csm_run_id", metadata_table, "last_csm_run_id")

            total_rows += rows
            _up_time = perf_counter()
            LOGGER.info(T("coal.services.database.row_count").format(count=rows))
            LOGGER.debug(
                T("coal.common.timing.operation_completed").format(
                    operation="Load from datastore", time=f"{_dl_time - _s_time:0.3}"
                )
            )
            LOGGER.debug(
                T("coal.common.timing.operation_completed").format(
                    operation="Send to postgresql", time=f"{_up_time - _dl_time:0.3}"
                )
            )
        _process_end = perf_counter()
        LOGGER.info(
            T("coal.services.database.rows_fetched").format(
                table="all tables",
                count=total_rows,
                time=f"{_process_end - _process_start:0.3}",
            )
        )
    else:
        LOGGER.info(T("coal.services.database.store_empty"))
