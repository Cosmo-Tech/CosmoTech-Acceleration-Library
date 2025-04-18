# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.orchestrator.utils.translate import T
import os

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.csm_data.utils.click import click


@click.command()
@click.option(
    "--adx-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("csm-data.commands.storage.adx_send_runnerdata.parameters.adx_uri"),
)
@click.option(
    "--adx-ingest-uri",
    envvar="AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI",
    show_envvar=True,
    required=True,
    metavar="URI",
    help=T("csm-data.commands.storage.adx_send_runnerdata.parameters.adx_ingest_uri"),
)
@click.option(
    "--database-name",
    envvar="AZURE_DATA_EXPLORER_DATABASE_NAME",
    show_envvar=True,
    required=True,
    metavar="NAME",
    help=T("csm-data.commands.storage.adx_send_runnerdata.parameters.database_name"),
)
@click.option(
    "--wait/--no-wait",
    "wait",
    envvar="CSM_DATA_ADX_WAIT_INGESTION",
    show_envvar=True,
    default=False,
    show_default=True,
    help="Wait for ingestion to complete",
)
@click.option(
    "--tag",
    envvar="CSM_DATA_ADX_TAG",
    show_envvar=True,
    default=None,
    help="Optional tag to use for tracking and potential rollback of this ingestion operation",
)
def adx_send_data(
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool,
    tag: str = None,
):
    # Import the function at the start of the command
    from cosmotech.coal.azure.adx.auth import create_ingest_client, create_kusto_client
    from cosmotech.coal.azure.adx.store import send_pyarrow_table_to_adx
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.azure.adx import check_ingestion_status
    from cosmotech.coal.azure.adx import create_table
    from cosmotech.coal.azure.adx import table_exists
    from cosmotech.coal.azure.adx import type_mapping

    import time
    import uuid
    from cosmotech.coal.azure.adx import IngestionStatus

    # Generate operation tag if not provided
    operation_tag = tag or f"op-{str(uuid.uuid4())}"
    LOGGER.debug(f"Starting ingestion operation with tag: {operation_tag}")

    LOGGER.debug("Initializing clients")
    kusto_client = create_kusto_client(adx_uri)
    ingest_client = create_ingest_client(adx_ingest_uri)
    database = database_name

    LOGGER.debug("Loading datastore")
    s = Store()
    source_ids = []
    LOGGER.debug("Listing tables")
    table_list = list(s.list_tables())
    table_ingestion_id_mapping = dict()
    for target_table_name in table_list:
        LOGGER.info(f"Working on table: {target_table_name}")
        data = s.get_table(target_table_name)

        if data.num_rows < 1:
            LOGGER.warning(f"Table {target_table_name} has no rows - skipping it")
            continue

        LOGGER.debug("  - Checking if table exists")
        if not table_exists(kusto_client, database, target_table_name):
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
            LOGGER.debug("  - Does not exist, creating it")
            create_table(kusto_client, database, target_table_name, mapping)

        LOGGER.debug(f"Sending data to the table {target_table_name}")
        # Use the operation_tag as the drop_by_tag parameter
        result = send_pyarrow_table_to_adx(ingest_client, database, target_table_name, data, operation_tag)
        source_ids.append(result.source_id)
        table_ingestion_id_mapping[result.source_id] = target_table_name

    # Track if any failures occur
    has_failures = False

    LOGGER.info("Store data was sent for ADX ingestion")
    try:
        if wait:
            LOGGER.info("Waiting for ingestion of data to finish")
            import tqdm

            with tqdm.tqdm(desc="Ingestion status", total=len(source_ids)) as pbar:
                while any(
                    list(
                        map(
                            lambda _status: _status[1] in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN),
                            results := list(check_ingestion_status(ingest_client, source_ids)),
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
                        result
                        for result in results
                        if result[1] not in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN)
                    )

                    for ingestion_id, ingestion_status in cleared_ids:
                        pbar.update(1)
                        source_ids.remove(ingestion_id)

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
                pbar.update(len(source_ids))
            LOGGER.info("All data ingestion attempts completed")

        # If any ingestion failed, perform rollback
        if has_failures:
            LOGGER.warning(f"Failures detected during ingestion - dropping data with tag: {operation_tag}")
            _drop_by_tag(kusto_client, database, operation_tag)

    except Exception as e:
        LOGGER.exception("Error during ingestion process")
        # Perform rollback using the tag
        LOGGER.warning(f"Dropping data with tag: {operation_tag}")
        _drop_by_tag(kusto_client, database, operation_tag)
        raise e

    if has_failures:
        click.Abort()


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


if __name__ == "__main__":
    adx_send_data()
