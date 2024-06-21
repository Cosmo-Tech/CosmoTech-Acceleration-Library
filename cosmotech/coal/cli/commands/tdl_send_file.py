# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib

import cosmotech_api
import requests
from cosmotech_api import DatasetApi
from cosmotech_api import DatasetTwinGraphQuery

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cosmotech_api.twin_data_layer import CSVSourceFile
from cosmotech.coal.utils.logger import LOGGER


@click.command()
@click.option("--api-url",
              envvar="CSM_API_URL",
              help="An URL to a Cosmo Tech API tenant",
              metavar="URL",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key",
              envvar="CSM_API_KEY",
              help="An API key configured in your Cosmo Tech tenant allowed to access workspace/files",
              metavar="API_KEY",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--api-key-header",
              envvar="CSM_API_KEY_HEADER",
              help="The header configured in your api to send an API key",
              metavar="HEADER",
              type=str,
              show_envvar=True,
              show_default=True,
              default="X-CSM-API-KEY")
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              help="An organization id for the Cosmo Tech API",
              metavar="o-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--dataset-id",
              envvar="CSM_DATASET_ID",
              help="A dataset id for the Cosmo Tech API",
              metavar="d-XXXXXXXX",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--dir",
              "directory-path",
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
def tdl_send_file_command(
    api_url,
    api_key,
    api_key_header,
    organization_id,
    dataset_id,
    directory_path,
    clear: bool
):
    """Reads a folder CSVs and send those to the Cosmo Tech API as a Dataset



Requires a valid connection to the API to send the data

This implementation make use of an API Key
    """

    configuration = cosmotech_api.Configuration(
        host=api_url,
    )

    with cosmotech_api.ApiClient(configuration,
                                 api_key_header,
                                 api_key) as api_client:
        api_ds = DatasetApi(api_client)
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
            'Content-Type': 'application/octet-stream',
            'User-Agent': 'OpenAPI-Generator/1.0.0/python',
            api_key_header: api_key
        }
        for authtype, authinfo in api_ds.api_client.configuration.auth_settings().items():
            api_ds.api_client._apply_auth_params(header, None, None, None, None, authinfo)

        if clear:
            LOGGER.info("Clearing all dataset content")
            api_ds.twingraph_query(organization_id,
                                   dataset_id,
                                   DatasetTwinGraphQuery(query="MATCH (n) DETACH DELETE n"))

        for query_dict in [entities_queries, relation_queries]:
            for file_path, query in query_dict.items():
                with open(file_path, "rb") as _f:
                    content = _f.read()

                query_craft = (api_url +
                               f"/organizations/{organization_id}"
                               f"/datasets/{dataset_id}"
                               f"/batch?query={query}")
                LOGGER.info(f"Sending content of '{file_path}")
                post = requests.post(query_craft,
                                     data=content,
                                     headers=header)

        LOGGER.info("Sent all data found")
