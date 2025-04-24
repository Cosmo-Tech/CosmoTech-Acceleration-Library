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
import pyarrow

from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.coal.utils.postgresql import send_pyarrow_table_to_postgresql
from cosmotech.orchestrator.utils.translate import T


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
    """
    _s = Store(store_location=store_folder)

    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info(T("coal.services.database.sending_data").format(table=f"{postgres_db}.{postgres_schema}"))
        total_rows = 0
        _process_start = perf_counter()
        for table_name in tables:
            _s_time = perf_counter()
            target_table_name = f"{table_prefix}{table_name}"
            LOGGER.info(T("coal.services.database.table_entry").format(table=target_table_name))
            data = _s.get_table(table_name)
            if not len(data):
                LOGGER.info(T("coal.services.database.no_rows"))
                continue
            _dl_time = perf_counter()
            rows = send_pyarrow_table_to_postgresql(
                data,
                target_table_name,
                postgres_host,
                postgres_port,
                postgres_db,
                postgres_schema,
                postgres_user,
                postgres_password,
                replace,
            )
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
