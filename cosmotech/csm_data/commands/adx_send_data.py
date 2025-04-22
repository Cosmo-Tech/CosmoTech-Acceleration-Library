# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.orchestrator.utils.translate import T
import os
import time
import uuid
import tqdm

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.csm_data.utils.click import click
from cosmotech.coal.azure.adx.auth import create_ingest_client, create_kusto_client
from cosmotech.coal.azure.adx.store import send_pyarrow_table_to_adx
from cosmotech.coal.store.store import Store
from cosmotech.coal.azure.adx import check_ingestion_status
from cosmotech.coal.azure.adx import create_table
from cosmotech.coal.azure.adx import table_exists
from cosmotech.coal.azure.adx import type_mapping
from cosmotech.coal.azure.adx import IngestionStatus


def initialize_clients(adx_uri, adx_ingest_uri):
    """
    Initialize and return the Kusto and ingest clients.

    Args:
        adx_uri: The Azure Data Explorer resource URI
        adx_ingest_uri: The Azure Data Explorer resource ingest URI

    Returns:
        tuple: (kusto_client, ingest_client)
    """
    LOGGER.debug("Initializing clients")
    kusto_client = create_kusto_client(adx_uri)
    ingest_client = create_ingest_client(adx_ingest_uri)
    return kusto_client, ingest_client


def check_and_create_table(kusto_client, database, table_name, data):
    """
    Check if a table exists and create it if it doesn't.

    Args:
        kusto_client: The Kusto client
        database: The database name
        table_name: The table name
        data: The PyArrow table data

    Returns:
        bool: True if the table was created, False if it already existed
    """
    LOGGER.debug("  - Checking if table exists")
    if not table_exists(kusto_client, database, table_name):
        mapping = create_column_mapping(data)
        LOGGER.debug("  - Does not exist, creating it")
        create_table(kusto_client, database, table_name, mapping)
        return True
    return False


def create_column_mapping(data):
    """
    Create a column mapping for a PyArrow table.

    Args:
        data: The PyArrow table data

    Returns:
        dict: A mapping of column names to their ADX types
    """
    mapping = dict()
    for column_name in data.column_names:
        column = data.column(column_name)
        try:
            ex = next(v for v in column.to_pylist() if v is not None)
        except StopIteration:
            LOGGER.error(f"Column {column_name} has no content, defaulting it to string")
            mapping[column_name] = type_mapping(column_name, "string")
            continue
        else:
            mapping[column_name] = type_mapping(column_name, ex)
    return mapping


def send_table_data(ingest_client, database, table_name, data, operation_tag):
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
    LOGGER.debug(f"Sending data to the table {table_name}")
    result = send_pyarrow_table_to_adx(ingest_client, database, table_name, data, operation_tag)
    return result.source_id, table_name


def process_tables(store, kusto_client, ingest_client, database, operation_tag):
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

    LOGGER.debug("Listing tables")
    table_list = list(store.list_tables())

    for target_table_name in table_list:
        LOGGER.info(f"Working on table: {target_table_name}")
        data = store.get_table(target_table_name)

        if data.num_rows < 1:
            LOGGER.warning(f"Table {target_table_name} has no rows - skipping it")
            continue

        check_and_create_table(kusto_client, database, target_table_name, data)

        source_id, _ = send_table_data(ingest_client, database, target_table_name, data, operation_tag)
        source_ids.append(source_id)
        table_ingestion_id_mapping[source_id] = target_table_name

    return source_ids, table_ingestion_id_mapping


def monitor_ingestion(ingest_client, source_ids, table_ingestion_id_mapping):
    """
    Monitor the ingestion process with progress reporting.

    Args:
        ingest_client: The ingest client
        source_ids: List of source IDs to monitor
        table_ingestion_id_mapping: Mapping of source IDs to table names

    Returns:
        bool: True if any failures occurred, False otherwise
    """
    has_failures = False
    source_ids_copy = source_ids.copy()

    LOGGER.info("Waiting for ingestion of data to finish")

    with tqdm.tqdm(desc="Ingestion status", total=len(source_ids_copy)) as pbar:
        while any(
            list(
                map(
                    lambda _status: _status[1] in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN),
                    results := list(check_ingestion_status(ingest_client, source_ids_copy)),
                )
            )
        ):
            # Check for failures
            for ingestion_id, ingestion_status in results:
                if ingestion_status == IngestionStatus.FAILURE:
                    LOGGER.error(
                        f"Ingestion {ingestion_id} failed for table {table_ingestion_id_mapping.get(ingestion_id)}"
                    )
                    has_failures = True

            cleared_ids = list(
                result for result in results if result[1] not in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN)
            )

            for ingestion_id, ingestion_status in cleared_ids:
                pbar.update(1)
                source_ids_copy.remove(ingestion_id)

            time.sleep(1)
            if os.environ.get("CSM_USE_RICH", "False").lower() in ("true", "1", "yes", "t", "y"):
                pbar.refresh()
        else:
            for ingestion_id, ingestion_status in results:
                if ingestion_status == IngestionStatus.FAILURE:
                    LOGGER.error(
                        f"Ingestion {ingestion_id} failed for table {table_ingestion_id_mapping.get(ingestion_id)}"
                    )
                    has_failures = True
        pbar.update(len(source_ids_copy))

    LOGGER.info("All data ingestion attempts completed")
    return has_failures


def _drop_by_tag(kusto_client, database, tag):
    """
    Drop all data with the specified tag
    """
    LOGGER.info(f"Dropping data with tag: {tag}")

    try:
        # Execute the drop by tag command
        drop_command = f'.drop extents <| .show database extents where tags has "drop-by:{tag}"'
        kusto_client.execute_mgmt(database, drop_command)
        LOGGER.info("Drop by tag operation completed")
    except Exception as e:
        LOGGER.error(f"Error during drop by tag operation: {str(e)}")
        LOGGER.exception("Drop by tag details")


def handle_failures(kusto_client, database, operation_tag, has_failures):
    """
    Handle any failures and perform rollbacks if needed.

    Args:
        kusto_client: The Kusto client
        database: The database name
        operation_tag: The operation tag for tracking
        has_failures: Whether any failures occurred

    Returns:
        bool: True if the process should abort, False otherwise
    """
    if has_failures:
        LOGGER.warning(f"Failures detected during ingestion - dropping data with tag: {operation_tag}")
        _drop_by_tag(kusto_client, database, operation_tag)
        return True
    return False


@click.command()
@click.option(
    "--adx-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("csm-data.commands.storage.adx_send_data.parameters.adx_uri"),
)
@click.option(
    "--adx-ingest-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("csm-data.commands.storage.adx_send_data.parameters.adx_ingest_uri"),
)
@click.option(
    "--database-name",
    envvar="AZURE_DATA_EXPLORER_DATABASE_NAME",
    show_envvar=True,
    required=True,
    metavar="NAME",
    help=T("csm-data.commands.storage.adx_send_data.parameters.database_name"),
)
@click.option(
    "--wait/--no-wait",
    "wait",
    envvar="CSM_DATA_ADX_WAIT_INGESTION",
    show_envvar=True,
    default=False,
    show_default=True,
    help=T("csm-data.commands.storage.adx_send_data.parameters.waiting_ingestion"),
)
@click.option(
    "--tag",
    envvar="CSM_DATA_ADX_TAG",
    show_envvar=True,
    default=None,
    help=T("csm-data.commands.storage.adx_send_data.parameters.adx_tag"),
)
def adx_send_data(
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool,
    tag: str = None,
):
    """
    Send data from the store to Azure Data Explorer.
    """
    # Generate a unique operation tag if none provided
    operation_tag = tag or f"op-{str(uuid.uuid4())}"
    LOGGER.debug(f"Starting ingestion operation with tag: {operation_tag}")

    # Initialize clients
    kusto_client, ingest_client = initialize_clients(adx_uri, adx_ingest_uri)
    database = database_name

    # Load datastore
    LOGGER.debug("Loading datastore")
    store = Store()

    try:
        # Process tables
        source_ids, table_ingestion_id_mapping = process_tables(
            store, kusto_client, ingest_client, database, operation_tag
        )

        LOGGER.info("Store data was sent for ADX ingestion")

        # Monitor ingestion if wait is True
        has_failures = False
        if wait and source_ids:
            has_failures = monitor_ingestion(ingest_client, source_ids, table_ingestion_id_mapping)

        # Handle failures
        should_abort = handle_failures(kusto_client, database, operation_tag, has_failures)
        if should_abort:
            click.Abort()

    except Exception as e:
        LOGGER.exception("Error during ingestion process")
        # Perform rollback using the tag
        LOGGER.warning(f"Dropping data with tag: {operation_tag}")
        _drop_by_tag(kusto_client, database, operation_tag)
        raise e


if __name__ == "__main__":
    adx_send_data()
