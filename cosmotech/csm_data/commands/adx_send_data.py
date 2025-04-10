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
def adx_send_data(
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool,
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
    from cosmotech.coal.azure.adx import IngestionStatus

    LOGGER.debug("Initializing clients")
    kusto_client = create_kusto_client(adx_uri)
    ingest_client = create_ingest_client(adx_ingest_uri)
    database = database_name

    LOGGER.debug("Loading datastore")
    s = Store()
    source_ids = []
    LOGGER.debug("Listing tables")
    table_list = list(s.list_tables())[:3]
    table_ingestion_id_mapping = dict()
    for target_table_name in table_list:
        LOGGER.info(f"Working on table: {target_table_name}")
        data = s.get_table(target_table_name)

        if data.num_rows < 1:
            LOGGER.warn(f"Table {target_table_name} has no rows - skipping it")
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
        result = send_pyarrow_table_to_adx(ingest_client, database, target_table_name, data, None)
        source_ids.append(result.source_id)
        table_ingestion_id_mapping[result.source_id] = target_table_name

    LOGGER.info("Store data was sent for ADX ingestion")
    if wait:
        LOGGER.info("Waiting for ingestion of data to finish")
        import tqdm

        with tqdm.tqdm(desc="Ingestion status", total=len(source_ids)) as pbar:
            while any(
                map(
                    lambda _status: _status[1] in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN),
                    results := list(check_ingestion_status(ingest_client, source_ids)),
                )
            ):
                cleared_ids = list(
                    result for result in results if result[1] not in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN)
                )

                for ingestion_id, ingestion_status in cleared_ids:
                    pbar.update(1)
                    source_ids.remove(ingestion_id)

                if os.environ.get("CSM_USE_RICH", "False").lower() in ("true", "1", "yes", "t", "y"):
                    for _ in range(10):
                        time.sleep(1)
                        pbar.update(0)
                else:
                    time.sleep(10)
            pbar.update(len(source_ids))
        LOGGER.info("All data got ingested")


if __name__ == "__main__":
    adx_send_data()
