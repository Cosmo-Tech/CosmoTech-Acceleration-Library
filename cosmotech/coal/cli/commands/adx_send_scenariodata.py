# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib
import time
from collections import defaultdict

from azure.kusto.data.response import KustoResponseDataSet
from azure.kusto.ingest import ColumnMapping
from azure.kusto.ingest import FileDescriptor
from azure.kusto.ingest import IngestionMappingKind
from azure.kusto.ingest import IngestionProperties
from azure.kusto.ingest import IngestionResult
from azure.kusto.ingest import ReportLevel

from CosmoTech_Acceleration_Library.Accelerators.adx_wrapper import ADXQueriesWrapper
from CosmoTech_Acceleration_Library.Accelerators.adx_wrapper import IngestionStatus
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--dataset-absolute-path",
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              help="A local folder to store the main dataset content",
              metavar="PATH",
              required=True)
@click.option("--parameters-absolute-path",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              metavar="PATH",
              show_envvar=True,
              help="A local folder to store the parameters content",
              required=True)
@click.option("--simulation-id",
              envvar="CSM_SIMULATION_ID",
              show_envvar=True,
              required=True,
              metavar="UUID",
              help="the Simulation Id to add to records")
@click.option("--adx-uri",
              envvar="AZURE_DATA_EXPLORER_RESOURCE_URI",
              show_envvar=True,
              required=True,
              metavar="URI",
              help="the ADX cluster path (URI info can be found into ADX cluster page)")
@click.option("--adx-ingest-uri",
              envvar="AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI",
              show_envvar=True,
              required=True,
              metavar="URI",
              help="The ADX cluster ingest path (URI info can be found into ADX cluster page)")
@click.option("--database-name",
              envvar="AZURE_DATA_EXPLORER_DATABASE_NAME",
              show_envvar=True,
              required=True,
              metavar="NAME",
              help="The targeted database name")
@click.option("--send-parameters/--no-send-parameters",
              type=bool,
              envvar="CSM_SEND_DATAWAREHOUSE_PARAMETERS",
              show_envvar=True,
              default=False,
              show_default=True,
              help="whether or not to send parameters (parameters path is mandatory then)")
@click.option("--send-datasets/--no-send-datasets",
              type=bool,
              envvar="CSM_SEND_DATAWAREHOUSE_DATASETS",
              show_envvar=True,
              default=False,
              show_default=True,
              help="whether or not to send datasets (parameters path is mandatory then)")
@click.option("--wait/--no-wait",
              envvar="WAIT_FOR_INGESTION",
              show_envvar=True,
              default=False,
              show_default=True,
              help="Toggle waiting for the ingestion results")
@web_help("csm-data/adx-send-scenario-data")
def adx_send_scenariodata(
    send_parameters: bool,
    send_datasets: bool,
    dataset_absolute_path: str,
    parameters_absolute_path: str,
    simulation_id: str,
    adx_uri: str,
    adx_ingest_uri: str,
    database_name: str,
    wait: bool
):
    """
Uses environment variables to send content of CSV files to ADX
Requires a valid Azure connection either with:
- The AZ cli command: **az login**
- A triplet of env var `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`
    """
    csv_data = dict()
    if send_parameters:
        csv_data.update(prepare_csv_content(parameters_absolute_path))
    if send_datasets:
        csv_data.update(prepare_csv_content(dataset_absolute_path))
    queries = construct_create_query(csv_data)
    adx_client = ADXQueriesWrapper(database=database_name,
                                   cluster_url=adx_uri,
                                   ingest_url=adx_ingest_uri)
    for k, v in queries.items():
        LOGGER.info(f"Create table query: [bold green]{v}[/]")
        r: KustoResponseDataSet = adx_client.run_query(v)
        if r.errors_count == 0:
            LOGGER.info(f"Table {k} created successfully")
        else:
            LOGGER.error(f"Issue creating table {k}")
            LOGGER.error(r.get_exceptions())
            raise click.Abort()
    insert_csv_files(files_data=csv_data,
                     adx_client=adx_client,
                     simulation_id=simulation_id,
                     database=database_name,
                     wait=wait)


def prepare_csv_content(folder_path):
    """
    Navigate through `folder_path` to generate csv information for each csv file in it

    return a map of filename to file_infos
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
        headers = header.split(',') if header else list()
        cols = {
            k.strip(): "string"
            for k in headers
        }
        csv_datas = {
            'filename': _file.name.removesuffix(".csv"),
            'headers': cols
        }
        content[str(_file)] = csv_datas
    LOGGER.debug(content)

    return content


def construct_create_query(files_data):
    """queries : map table_name -> query
    foreach csv_file:
        table_name = csv_file.filename
        query = ".create-merge table " + table_name + "("
        query += ",".join(
            foreach field:
                field.name + ":" + field.type
        )
        query += ")"
        queries[table_name] = query"""
    queries = dict()
    for file_path, file_info in files_data.items():
        filename = file_info.get('filename')
        fields = file_info.get('headers')
        query = f".create-merge table {filename} ({','.join(':'.join((k, v)) for k, v in fields.items())})"
        queries[filename] = query
    return queries


def insert_csv_files(files_data, adx_client: ADXQueriesWrapper, simulation_id, database, wait=False):
    """insert_data(csv_infos):
    create ingestion client
    foreach csv_file:
        open csv_file
        add column simulationRun to headers_infos
        create ingestion_mapping
        create ingestion_properties
        set ingestion_mapping and dropByTags
        ingest csv_file + ingestion_properties"""
    ingestion_ids = dict()
    for file_path, file_info in files_data.items():
        filename = file_info.get('filename')
        fields = file_info.get('headers')
        with open(file_path) as _f:
            file_size = sum(map(len, _f.readlines()))
            LOGGER.debug(f"{file_path} size: {file_size}")
        fd = FileDescriptor(file_path, file_size)
        ord = 0
        mappings = list()
        for column, _type in fields.items():
            mapping = ColumnMapping(column_name=column,
                                    column_type=_type,
                                    ordinal=ord)
            ord += 1
            mappings.append(mapping)
        simulation_run_col = ColumnMapping(column_name="simulationrun",
                                           column_type="string",
                                           ordinal=ord,
                                           const_value=simulation_id)
        mappings.append(simulation_run_col)
        ingestion_properties = IngestionProperties(database=database,
                                                   table=filename,
                                                   column_mappings=mappings,
                                                   ingestion_mapping_kind=IngestionMappingKind.CSV,
                                                   drop_by_tags=[simulation_id, ],
                                                   report_level=ReportLevel.FailuresAndSuccesses,
                                                   additional_properties={"ignoreFirstRecord": "true"})
        LOGGER.info(f"Ingesting {filename}")
        results: IngestionResult = adx_client.ingest_client.ingest_from_file(fd, ingestion_properties)
        ingestion_ids[str(results.source_id)] = filename
    if wait:
        count = 0
        limit = 5
        pause_duration = 8
        while any(map(lambda s: s[1] in (IngestionStatus.QUEUED, IngestionStatus.UNKNOWN),
                      adx_client.check_ingestion_status(source_ids=list(ingestion_ids.keys())))):
            count += 1
            if count > limit:
                LOGGER.warning("Max number of retry, stop waiting")
                break
            LOGGER.info(f"Waiting for ingestion results, retry in {pause_duration}s ({count}/{limit})")
            time.sleep(pause_duration)

        LOGGER.info("Status of ingestion:")
        status_color_mapping = defaultdict(lambda: "bright_black")
        status_color_mapping[IngestionStatus.FAILURE.value] = "red"
        status_color_mapping[IngestionStatus.SUCCESS.value] = "green"
        for _id, status in adx_client.check_ingestion_status(source_ids=list(ingestion_ids.keys())):
            LOGGER.info(f"[bold cyan]{ingestion_ids[_id]}[/] - "
                        f"[bold {status_color_mapping[status.value]}]{status.name}[/]")
    else:
        LOGGER.info("No wait for ingestion result")


if __name__ == "__main__":
    adx_send_scenariodata()
