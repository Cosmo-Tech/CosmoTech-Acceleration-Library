# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import tempfile
import uuid
from typing import Optional, List, Dict, Tuple, Union, Any

import pyarrow
import pyarrow.csv as pc
import time
from azure.kusto.data import KustoClient
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import IngestionProperties
from azure.kusto.ingest import QueuedIngestClient
from azure.kusto.ingest import ReportLevel
from cosmotech.orchestrator.utils.translate import T
from time import perf_counter

from cosmotech.coal.azure.adx.tables import check_and_create_table, _drop_by_tag
from cosmotech.coal.azure.adx.auth import initialize_clients
from cosmotech.coal.azure.adx.ingestion import monitor_ingestion, handle_failures
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.coal.utils.postgresql import send_pyarrow_table_to_postgresql


def send_table_data(
    ingest_client: QueuedIngestClient, database: str, table_name: str, data: pyarrow.Table, operation_tag: str
) -> Tuple[str, str]:
    """
    Send a PyArrow table to ADX.

    Args:
        ingest_client: The ingest client
        database: The database name
        table_name: The table name
        data: The PyArrow table data
        operation_tag: The operation tag for tracking

    Returns:
        tuple: (source_id, table_name)
    """
    LOGGER.debug(T("coal.services.adx.sending_data").format(table_name=table_name))
    result = send_pyarrow_table_to_adx(ingest_client, database, table_name, data, operation_tag)
    return result.source_id, table_name


def process_tables(
    store: Store, kusto_client: KustoClient, ingest_client: QueuedIngestClient, database: str, operation_tag: str
) -> Tuple[List[str], Dict[str, str]]:
    """
    Process all tables in the store.

    Args:
        store: The data store
        kusto_client: The Kusto client
        ingest_client: The ingest client
        database: The database name
        operation_tag: The operation tag for tracking

    Returns:
        tuple: (source_ids, table_ingestion_id_mapping)
    """
    source_ids = []
    table_ingestion_id_mapping = dict()

    LOGGER.debug(T("coal.services.adx.listing_tables"))
    table_list = list(store.list_tables())

    for target_table_name in table_list:
        LOGGER.info(T("coal.services.adx.working_on_table").format(table_name=target_table_name))
        data = store.get_table(target_table_name)

        if data.num_rows < 1:
            LOGGER.warning(T("coal.services.adx.table_empty").format(table_name=target_table_name))
            continue

        check_and_create_table(kusto_client, database, target_table_name, data)

        source_id, _ = send_table_data(ingest_client, database, target_table_name, data, operation_tag)
        source_ids.append(source_id)
        table_ingestion_id_mapping[source_id] = target_table_name

    return source_ids, table_ingestion_id_mapping


def send_pyarrow_table_to_adx(
    client: QueuedIngestClient,
    database: str,
    table_name: str,
    table_data: pyarrow.Table,
    drop_by_tag: Optional[str] = None,
):
    drop_by_tags = [drop_by_tag] if (drop_by_tag is not None) else None

    properties = IngestionProperties(
        database=database,
        table=table_name,
        data_format=DataFormat.CSV,
        drop_by_tags=drop_by_tags,
        report_level=ReportLevel.FailuresAndSuccesses,
        flush_immediately=True,
    )

    file_name = f"adx_{database}_{table_name}_{int(time.time())}_{uuid.uuid4()}.csv"
    temp_file_path = os.path.join(os.environ.get("CSM_TEMP_ABSOLUTE_PATH", tempfile.gettempdir()), file_name)
    pc.write_csv(table_data, temp_file_path, pc.WriteOptions(include_header=False))
    try:
        return client.ingest_from_file(temp_file_path, properties)
    finally:
        os.unlink(temp_file_path)


def send_store_to_adx(
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool = False,
    tag: Optional[str] = None,
    store_location: Optional[str] = None,
) -> Union[bool, Any]:
    """
    Send data from the store to Azure Data Explorer.

    Args:
        adx_uri: The Azure Data Explorer resource URI
        adx_ingest_uri: The Azure Data Explorer resource ingest URI
        database_name: The database name
        wait: Whether to wait for ingestion to complete
        tag: The operation tag for tracking (will generate a unique one if not provided)
        store_location: Optional store location (uses default if not provided)

    Returns:
        bool: True if successful, False otherwise
    """
    # Generate a unique operation tag if none provided
    operation_tag = tag or f"op-{str(uuid.uuid4())}"
    LOGGER.debug(T("coal.services.adx.starting_ingestion").format(operation_tag=operation_tag))

    # Initialize clients
    kusto_client, ingest_client = initialize_clients(adx_uri, adx_ingest_uri)
    database = database_name

    # Load datastore
    LOGGER.debug(T("coal.services.adx.loading_datastore"))
    store = Store(store_location=store_location)

    try:
        # Process tables
        source_ids, table_ingestion_id_mapping = process_tables(
            store, kusto_client, ingest_client, database, operation_tag
        )

        LOGGER.info(T("coal.services.adx.data_sent"))

        # Monitor ingestion if wait is True
        has_failures = False
        if wait and source_ids:
            has_failures = monitor_ingestion(ingest_client, source_ids, table_ingestion_id_mapping)

        # Handle failures
        should_abort = handle_failures(kusto_client, database, operation_tag, has_failures)
        if should_abort:
            return False

        return True

    except Exception as e:
        LOGGER.exception(T("coal.services.adx.ingestion_error"))
        # Perform rollback using the tag
        LOGGER.warning(T("coal.services.adx.dropping_data").format(operation_tag=operation_tag))
        _drop_by_tag(kusto_client, database, operation_tag)
        raise e


def dump_store_to_adx(
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
    Dump Store data to an Azure Data Explorer database.

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
