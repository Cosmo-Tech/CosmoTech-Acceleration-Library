# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from enum import Enum
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

import pandas as pd
import time
from azure.kusto.data import KustoClient
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import IngestionProperties
from azure.kusto.ingest import QueuedIngestClient
from azure.kusto.ingest import ReportLevel
from azure.kusto.ingest.status import FailureMessage
from azure.kusto.ingest.status import KustoIngestStatusQueues
from azure.kusto.ingest.status import SuccessMessage
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.azure.adx.tables import create_table
from cosmotech.coal.azure.adx.utils import type_mapping
from cosmotech.coal.utils.logger import LOGGER


class IngestionStatus(Enum):
    QUEUED = "QUEUED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    UNKNOWN = "UNKNOWN"
    TIMEOUT = "TIMED OUT"


# Global dictionaries to track ingestion status
_ingest_status: Dict[str, IngestionStatus] = {}
_ingest_times: Dict[str, float] = {}


def ingest_dataframe(
    client: QueuedIngestClient,
    database: str,
    table_name: str,
    dataframe: pd.DataFrame,
    drop_by_tag: Optional[str] = None,
):
    """
    Ingest a pandas DataFrame into an ADX table.

    Args:
        client: The QueuedIngestClient to use
        database: The name of the database
        table_name: The name of the table
        dataframe: The DataFrame to ingest
        drop_by_tag: Tag used for the drop by capacity of the Cosmotech API

    Returns:
        The ingestion result with source_id for status tracking
    """
    LOGGER.debug(T("coal.logs.adx.ingesting_dataframe").format(table_name=table_name, rows=len(dataframe)))

    drop_by_tags = [drop_by_tag] if (drop_by_tag is not None) else None

    properties = IngestionProperties(
        database=database,
        table=table_name,
        data_format=DataFormat.CSV,
        drop_by_tags=drop_by_tags,
        report_level=ReportLevel.FailuresAndSuccesses,
    )

    ingestion_result = client.ingest_from_dataframe(dataframe, ingestion_properties=properties)

    # Track the ingestion status
    source_id = str(ingestion_result.source_id)
    _ingest_status[source_id] = IngestionStatus.QUEUED
    _ingest_times[source_id] = time.time()

    LOGGER.debug(T("coal.logs.adx.ingestion_queued").format(source_id=source_id))

    return ingestion_result


def send_to_adx(
    query_client: KustoClient,
    ingest_client: QueuedIngestClient,
    database: str,
    dict_list: List[Dict],
    table_name: str,
    ignore_table_creation: bool = True,
    drop_by_tag: Optional[str] = None,
):
    """
    Send a list of dictionaries to an ADX table.

    Args:
        query_client: The KustoClient for querying
        ingest_client: The QueuedIngestClient for ingestion
        database: The name of the database
        dict_list: The list of dictionaries to send
        table_name: The name of the table
        ignore_table_creation: If False, will create the table if it doesn't exist
        drop_by_tag: Tag used for the drop by capacity of the Cosmotech API

    Returns:
        The ingestion result with source_id for status tracking
    """
    LOGGER.debug(T("coal.logs.adx.sending_to_adx").format(table_name=table_name, items=len(dict_list)))

    if not dict_list:
        LOGGER.warning(T("coal.logs.adx.empty_dict_list"))
        return None

    if not ignore_table_creation:
        # If the target table does not exist create it
        # First create the columns types needed for the table
        types = {k: type_mapping(k, dict_list[0][k]) for k in dict_list[0].keys()}

        # Then try to create the table
        if not create_table(query_client, database, table_name, types):
            LOGGER.error(T("coal.logs.adx.table_creation_failed").format(table_name=table_name))
            return False

    # Create a dataframe with the data to write and send them to ADX
    df = pd.DataFrame(dict_list)
    return ingest_dataframe(ingest_client, database, table_name, df, drop_by_tag)


def check_ingestion_status(
    client: QueuedIngestClient,
    source_ids: List[str],
    timeout: Optional[int] = None,
    logs: bool = False,
) -> Iterator[Tuple[str, IngestionStatus]]:
    """
    Check the status of ingestion operations.

    Args:
        client: The QueuedIngestClient to use
        source_ids: List of source IDs to check
        timeout: Timeout in seconds (default: 900)
        logs: Whether to log detailed information

    Returns:
        Iterator of (source_id, status) tuples
    """
    default_timeout = 900
    remaining_ids = []

    # First yield any already known statuses
    for source_id in source_ids:
        if source_id not in _ingest_status:
            _ingest_status[source_id] = IngestionStatus.UNKNOWN
            _ingest_times[source_id] = time.time()

        if _ingest_status[source_id] not in [
            IngestionStatus.QUEUED,
            IngestionStatus.UNKNOWN,
        ]:
            yield source_id, _ingest_status[source_id]
        else:
            remaining_ids.append(source_id)

    if not remaining_ids:
        return

    LOGGER.debug(T("coal.logs.adx.checking_status").format(count=len(remaining_ids)))

    # Get status queues
    qs = KustoIngestStatusQueues(client)

    def get_messages(queues):
        _r = []
        for q in queues:
            _r.extend(((q, m) for m in q.receive_messages(messages_per_page=32, visibility_timeout=1)))
        return _r

    successes = get_messages(qs.success._get_queues())
    failures = get_messages(qs.failure._get_queues())

    LOGGER.debug(T("coal.logs.adx.status_messages").format(success=len(successes), failure=len(failures)))

    non_sent_ids = remaining_ids[:]
    # Process success and failure messages
    for messages, cast_func, status, log_function in [
        (successes, SuccessMessage, IngestionStatus.SUCCESS, LOGGER.debug),
        (failures, FailureMessage, IngestionStatus.FAILURE, LOGGER.error),
    ]:
        for _q, _m in messages:
            dm = cast_func(_m.content)
            to_check_ids = remaining_ids[:]

            for source_id in to_check_ids:
                if dm.IngestionSourceId == str(source_id):
                    _ingest_status[source_id] = status

                    log_function(T("coal.logs.adx.status_found").format(source_id=source_id, status=status.value))

                    _q.delete_message(_m)
                    remaining_ids.remove(source_id)
                    break
            else:
                # The message did not correspond to a known ID
                continue
            break
        else:
            # No message was found on the current list of messages for the given IDs
            continue
        break

    # Check for timeouts
    actual_timeout = timeout if timeout is not None else default_timeout
    for source_id in remaining_ids:
        if time.time() - _ingest_times[source_id] > actual_timeout:
            _ingest_status[source_id] = IngestionStatus.TIMEOUT
            LOGGER.warning(T("coal.logs.adx.ingestion_timeout").format(source_id=source_id))

    # Yield results for remaining IDs
    for source_id in non_sent_ids:
        yield source_id, _ingest_status[source_id]


def clear_ingestion_status_queues(client: QueuedIngestClient, confirmation: bool = False):
    """
    Clear all data in the ingestion status queues.
    DANGEROUS: This will clear all queues for the entire ADX cluster.

    Args:
        client: The QueuedIngestClient to use
        confirmation: Must be True to proceed with clearing
    """
    if not confirmation:
        LOGGER.warning(T("coal.logs.adx.clear_queues_no_confirmation"))
        return

    LOGGER.warning(T("coal.logs.adx.clearing_queues"))
    qs = KustoIngestStatusQueues(client)

    while not qs.success.is_empty():
        qs.success.pop(32)

    while not qs.failure.is_empty():
        qs.failure.pop(32)

    LOGGER.info(T("coal.logs.adx.queues_cleared"))
