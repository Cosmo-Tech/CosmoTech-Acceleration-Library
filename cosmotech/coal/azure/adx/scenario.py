# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
ADX scenario data ingestion module.

This module provides functions for ingesting scenario data into Azure Data Explorer.
"""

import pathlib
import time
from collections import defaultdict
from typing import Dict, Any, List, Tuple, Optional

from azure.kusto.data.response import KustoResponseDataSet
from azure.kusto.ingest import ColumnMapping
from azure.kusto.ingest import FileDescriptor
from azure.kusto.ingest import IngestionMappingKind
from azure.kusto.ingest import IngestionProperties
from azure.kusto.ingest import IngestionResult
from azure.kusto.ingest import ReportLevel

from cosmotech.coal.azure.adx.wrapper import ADXQueriesWrapper
from cosmotech.coal.azure.adx.wrapper import IngestionStatus
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def prepare_csv_content(folder_path: str) -> Dict[str, Dict[str, Any]]:
    """
    Navigate through `folder_path` to generate csv information for each csv file in it.

    Args:
        folder_path: Path to the folder containing CSV files

    Returns:
        A map of filename to file_infos
        file infos:
        dict:
            filename -> filename as a string without path & extension
            headers -> map of column_name -> column_type
    """
    content = dict()
    root = pathlib.Path(folder_path)
    for _file in root.rglob("*.csv"):
        with open(_file) as _csv_content:
            header = _csv_content.readline().replace("@", "").strip()
        headers = header.split(",") if header else list()
        cols = {k.strip(): "string" for k in headers}
        csv_datas = {"filename": _file.name.removesuffix(".csv"), "headers": cols}
        content[str(_file)] = csv_datas
    LOGGER.debug(content)

    return content


def construct_create_query(files_data: Dict[str, Dict[str, Any]]) -> Dict[str, str]:
    """
    Construct ADX table creation queries for the given CSV files.

    Args:
        files_data: Map of filename to file_infos as returned by prepare_csv_content

    Returns:
        Map of table_name to creation query
    """
    queries = dict()
    for file_path, file_info in files_data.items():
        filename = file_info.get("filename")
        fields = file_info.get("headers")
        query = f".create-merge table {filename} ({','.join(':'.join((k, v)) for k, v in fields.items())})"
        queries[filename] = query
    return queries


def insert_csv_files(
    files_data: Dict[str, Dict[str, Any]],
    adx_client: ADXQueriesWrapper,
    simulation_id: str,
    database: str,
    wait: bool = False,
) -> None:
    """
    Insert CSV files into ADX tables.

    Args:
        files_data: Map of filename to file_infos as returned by prepare_csv_content
        adx_client: ADX client wrapper
        simulation_id: Simulation ID to use as a tag
        database: ADX database name
        wait: Whether to wait for ingestion to complete
    """
    ingestion_ids = dict()
    for file_path, file_info in files_data.items():
        filename = file_info.get("filename")
        fields = file_info.get("headers")
        with open(file_path) as _f:
            file_size = sum(map(len, _f.readlines()))
            LOGGER.debug(T("coal.logs.data_transfer.sending_data").format(size=file_size))
        fd = FileDescriptor(file_path, file_size)
        ord = 0
        mappings = list()
        for column, _type in fields.items():
            mapping = ColumnMapping(column_name=column, column_type=_type, ordinal=ord)
            ord += 1
            mappings.append(mapping)
        simulation_run_col = ColumnMapping(
            column_name="simulationrun",
            column_type="string",
            ordinal=ord,
            const_value=simulation_id,
        )
        mappings.append(simulation_run_col)
        ingestion_properties = IngestionProperties(
            database=database,
            table=filename,
            column_mappings=mappings,
            ingestion_mapping_kind=IngestionMappingKind.CSV,
            drop_by_tags=[
                simulation_id,
            ],
            report_level=ReportLevel.FailuresAndSuccesses,
            additional_properties={"ignoreFirstRecord": "true"},
        )
        LOGGER.info(T("coal.logs.ingestion.ingesting").format(table=filename))
        results: IngestionResult = adx_client.ingest_client.ingest_from_file(fd, ingestion_properties)
        ingestion_ids[str(results.source_id)] = filename
    if wait:
        count = 0
        limit = 5
        pause_duration = 8
        while any(
            map(
                lambda s: s[1] in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN),
                adx_client.check_ingestion_status(source_ids=list(ingestion_ids.keys())),
            )
        ):
            count += 1
            if count > limit:
                LOGGER.warning(T("coal.logs.ingestion.max_retry"))
                break
            LOGGER.info(
                T("coal.logs.ingestion.waiting_results").format(duration=pause_duration, count=count, limit=limit)
            )
            time.sleep(pause_duration)

        LOGGER.info(T("coal.logs.ingestion.status"))
        for _id, status in adx_client.check_ingestion_status(source_ids=list(ingestion_ids.keys())):
            color = (
                "red"
                if status == IngestionStatus.FAILURE
                else "green"
                if status == IngestionStatus.SUCCESS
                else "bright_black"
            )
            LOGGER.info(
                T("coal.logs.ingestion.status_report").format(table=ingestion_ids[_id], status=status.name, color=color)
            )
    else:
        LOGGER.info(T("coal.logs.ingestion.no_wait"))


def send_scenario_data(
    dataset_absolute_path: str,
    parameters_absolute_path: str,
    simulation_id: str,
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    send_parameters: bool = False,
    send_datasets: bool = False,
    wait: bool = False,
) -> None:
    """
    Send scenario data to ADX.

    Args:
        dataset_absolute_path: Path to the dataset folder
        parameters_absolute_path: Path to the parameters folder
        simulation_id: Simulation ID to use as a tag
        adx_uri: ADX cluster URI
        adx_ingest_uri: ADX ingestion URI
        database_name: ADX database name
        send_parameters: Whether to send parameters
        send_datasets: Whether to send datasets
        wait: Whether to wait for ingestion to complete
    """
    csv_data = dict()
    if send_parameters:
        csv_data.update(prepare_csv_content(parameters_absolute_path))
    if send_datasets:
        csv_data.update(prepare_csv_content(dataset_absolute_path))
    queries = construct_create_query(csv_data)
    adx_client = ADXQueriesWrapper(database=database_name, cluster_url=adx_uri, ingest_url=adx_ingest_uri)
    for k, v in queries.items():
        LOGGER.info(T("coal.logs.ingestion.creating_table").format(query=v))
        r: KustoResponseDataSet = adx_client.run_query(v)
        if r.errors_count == 0:
            LOGGER.info(T("coal.logs.ingestion.table_created").format(table=k))
        else:
            LOGGER.error(T("coal.logs.ingestion.table_creation_failed").format(table=k))
            LOGGER.error(r.get_exceptions())
            raise RuntimeError(f"Failed to create table {k}")
    insert_csv_files(
        files_data=csv_data,
        adx_client=adx_client,
        simulation_id=simulation_id,
        database=database_name,
        wait=wait,
    )
