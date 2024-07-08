# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import os
import pathlib
from csv import DictWriter

from cosmotech_api import DatasetApi
from cosmotech_api import DatasetTwinGraphQuery
from cosmotech_api import RunnerApi

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help="An organization id for the Cosmo Tech API",
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              help="A workspace id for the Cosmo Tech API",
              metavar="w-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--runner-id",
              envvar="CSM_RUNNER_ID",
              help="A runner id for the Cosmo Tech API",
              metavar="r-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--dir",
              "directory_path",
              help="Path to the directory to write the results to",
              metavar="PATH",
              default="./",
              type=str,
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              required=True)
def tdl_load_file_command(
    organization_id,
    workspace_id,
    runner_id,
    directory_path
):
    """Query a twingraph and loads all the data from it

Will create 1 csv file per node type / relationship type

The twingraph must have been populated using the "tdl-send-file" command for this to work correctly

Requires a valid connection to the API to send the data
    """

    api_client, connection_type = get_api_client()
    api_ds = DatasetApi(api_client)
    api_runner = RunnerApi(api_client)

    runner_info = api_runner.get_runner(organization_id,
                                        workspace_id,
                                        runner_id)

    if len(runner_info.dataset_list) != 1:
        LOGGER.error(f"Runner {runner_id} is not tied to a single dataset")
        LOGGER.debug(runner_info)
        raise click.Abort()

    dataset_id = runner_info.dataset_list[0]

    dataset_info = api_ds.find_dataset_by_id(organization_id,
                                             dataset_id)

    if dataset_info.ingestion_status != "SUCCESS":
        LOGGER.error(f"Dataset {dataset_id} is in state {dataset_info.ingestion_status}")
        LOGGER.debug(dataset_info)
        raise click.Abort()

    directory_path = pathlib.Path(directory_path)
    if directory_path.is_file():
        LOGGER.error(f"{directory_path} is not a directory.")
        raise click.Abort()

    directory_path.mkdir(parents=True, exist_ok=True)

    nodes_query: list[dict] = api_ds.twingraph_query(os.environ.get("CSM_ORGANIZATION_ID"),
                                                     os.environ.get("CSM_DATASET_ID"),
                                                     DatasetTwinGraphQuery(query="MATCH (n) RETURN n"))

    relationships_query: list[dict] = api_ds.twingraph_query(os.environ.get("CSM_ORGANIZATION_ID"),
                                                             os.environ.get("CSM_DATASET_ID"),
                                                             DatasetTwinGraphQuery(query="MATCH ()-[n]->() return n"))

    files_content = dict()
    files_headers = dict()

    for element_query in [nodes_query, relationships_query]:
        for element in element_query:
            data = element.get('n', dict())
            element_type = data.get('label', "")
            properties = data.get("properties", dict())
            if element_type not in files_content:
                files_content[element_type] = list()
                files_headers[element_type] = set()
            files_content[element_type].append(properties)
            files_headers[element_type].update(properties.keys())

    for file_name in files_content.keys():
        file_path = directory_path / (file_name + ".csv")
        LOGGER.info(f"Writing {len(files_content[file_name])} lines in {file_path}")
        with (file_path.open("w") as _f):
            headers = files_headers[file_name]
            headers.remove("id")
            if "src" in headers:
                headers.remove("src")
                headers.remove("dest")
                headers = ["id", "src", "dest"] + sorted(headers)
            else:
                headers = ["id"] + sorted(headers)

            dw = DictWriter(_f,
                            fieldnames=headers)
            dw.writeheader()
            dw.writerows(sorted(files_content[file_name], key=lambda r: r.get('id')))

    LOGGER.info("All CSV are written")
