# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import json
import pathlib
from csv import DictReader
from csv import DictWriter
from io import StringIO

import requests
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.twin_data_layer import CSVSourceFile
from cosmotech.coal.utils.logger import LOGGER
from cosmotech_api import DatasetApi
from cosmotech_api import DatasetTwinGraphQuery
from cosmotech_api import RunnerApi

BATCH_SIZE_LIMIT = 10000


@click.command()
@click.option("--api-url",
              envvar="CSM_API_URL",
              help="The URI to a Cosmo Tech API instance",
              metavar="URI",
              type=str,
              show_envvar=True,
              required=True)
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
              help="Path to the directory containing csvs to send",
              metavar="PATH",
              default="./",
              type=str,
              envvar="CSM_DATASET_ABSOLUTE_PATH",
              show_envvar=True,
              required=True)
@click.option("--clear/--keep",
              help="Flag to clear the target dataset first "
                   "(if set to True will clear the dataset before sending anything, irreversibly)",
              is_flag=True,
              default=True,
              show_default=True,
              type=bool)
@web_help("csm-data/api/tdl-send-files")
def tdl_send_files(
    api_url,
    organization_id,
    workspace_id,
    runner_id,
    directory_path,
    clear: bool
):
    """Reads a folder CSVs and send those to the Cosmo Tech API as a Dataset

CSVs must follow a given format :
  - Nodes files must have an `id` column
  - Relationship files must have `id`, `src` and `dest` columns

Non-existing relationship (aka dest or src does not point to existing node) won't trigger an error, 
the relationship will not be created instead.

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

    dataset_info.ingestion_status = "SUCCESS"

    api_ds.update_dataset(organization_id,
                          dataset_id,
                          dataset_info)
    entities_queries = dict()
    relation_queries = dict()

    content_path = pathlib.Path(directory_path)
    if not content_path.is_dir():
        LOGGER.error(f"'{directory_path}' is not a directory")

    for file_path in content_path.glob("*.csv"):
        _csv = CSVSourceFile(file_path)
        if _csv.is_node:
            LOGGER.info(f"Detected '{file_path}' to be a nodes containing file")
            entities_queries[file_path] = _csv.generate_query_insert()
        else:
            LOGGER.info(f"Detected '{file_path}' to be a relationships containing file")
            relation_queries[file_path] = _csv.generate_query_insert()

    header = {
        'Accept': 'application/json',
        'Content-Type': 'text/csv',
        'User-Agent': 'OpenAPI-Generator/1.0.0/python',
    }
    header.update(api_client.default_headers)

    for authtype, authinfo in api_ds.api_client.configuration.auth_settings().items():
        api_ds.api_client._apply_auth_params(header, None, None, None, None, authinfo)

    if clear:
        LOGGER.info("Clearing all dataset content")

        clear_query = "MATCH (n) DETACH DELETE n"
        api_ds.twingraph_query(organization_id,
                               dataset_id,
                               DatasetTwinGraphQuery(query=str(clear_query)))

    for query_dict in [entities_queries, relation_queries]:
        for file_path, query in query_dict.items():
            # content = []
            # with open(file_path, "r") as _f:
            #     dr = DictReader(_f)
            #     for _r in dr:
            #         content.append(_r)
            # _q = DatasetTwinGraphQuery(query=query,
            #                            parameters={"params": content})
            # print(_q)
            # api_ds.twingraph_query(organization_id,
            #                        dataset_id,
            #                        _q)

            content = StringIO()
            size = 0
            batch = 1
            errors = []
            query_craft = (api_url +
                           f"/organizations/{organization_id}"
                           f"/datasets/{dataset_id}"
                           f"/batch?query={query}")
            LOGGER.info(f"Sending content of '{file_path}")

            with open(file_path, "r") as _f:
                dr = DictReader(_f)
                dw = DictWriter(content, fieldnames=sorted(dr.fieldnames, key=len, reverse=True))
                dw.writeheader()
                for row in dr:
                    dw.writerow(row)
                    size += 1
                    if size > BATCH_SIZE_LIMIT:
                        LOGGER.info(f"Found row count of {batch * BATCH_SIZE_LIMIT}, sending now")
                        batch += 1
                        content.seek(0)
                        post = requests.post(query_craft,
                                             data=content.read(),
                                             headers=header)
                        post.raise_for_status()
                        errors.extend(json.loads(post.content)["errors"])
                        content = StringIO()
                        dw = DictWriter(content, fieldnames=sorted(dr.fieldnames, key=len, reverse=True))
                        dw.writeheader()
                        size = 0

            if size > 0:
                content.seek(0)
                post = requests.post(query_craft,
                                     data=content.read(),
                                     headers=header)
                post.raise_for_status()
                errors.extend(json.loads(post.content)["errors"])

            if len(errors):
                LOGGER.error(f"Found {len(errors)} errors while importing: ")
                for _err in errors:
                    LOGGER.error(f"{_err}")
                raise click.Abort()

    LOGGER.info("Sent all data found")

    dataset_info.ingestion_status = "SUCCESS"

    api_ds.update_dataset(organization_id,
                          dataset_id,
                          dataset_info)
