# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from io import BytesIO

from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

import pyarrow.csv as pc
import pyarrow.parquet as pq
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER

VALID_TYPES = (
    "sqlite",
    "csv",
    "parquet",
)


@click.command()
@click.option("--store-folder",
              envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
              help="The folder containing the store files",
              metavar="PATH",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--output-type",
              default="sqlite",
              help="Choose the type of file output to use",
              type=click.Choice(VALID_TYPES,
                                case_sensitive=False))
@click.option("--account-name",
              "account_name",
              envvar="AZURE_ACCOUNT_NAME",
              help="The account name on Azure to upload to",
              type=str,
              show_envvar=True,
              required=True)
@click.option("--container-name",
              "container_name",
              envvar="AZURE_CONTAINER_NAME",
              help="The container name on Azure to upload to",
              type=str,
              show_envvar=True,
              default="")
@click.option("--prefix",
              "file_prefix",
              envvar="CSM_DATA_PREFIX",
              help="A prefix by which all uploaded files should start with in the container",
              metavar="PREFIX",
              type=str,
              show_envvar=True,
              default="")
@click.option("--tenant-id",
              "tenant_id",
              help="Tenant Identity used to connect to Azure storage system",
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AZURE_TENANT_ID")
@click.option("--client-id",
              "client_id",
              help="Client Identity used to connect to Azure storage system",
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AZURE_CLIENT_ID")
@click.option("--client-secret",
              "client_secret",
              help="Client Secret tied to the ID used to connect to Azure storage system",
              type=str,
              required=True,
              show_envvar=True,
              metavar="ID",
              envvar="AZURE_CLIENT_SECRET")
@web_help("csm-data/store/dump-to-azure")
def dump_to_azure(
    store_folder,
    account_name: str,
    container_name: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_type: str,
    file_prefix: str
):
    """Dump a datastore to a Azure storage account.

Will upload everything from a given data store to a Azure storage container.

3 modes currently exists :
  - sqlite : will dump the data store underlying database as is
  - csv : will convert every table of the datastore to csv and send them as separate files
  - parquet : will convert every table of the datastore to parquet and send them as separate files

Make use of the azure.storage.blob library to access the container

More information is available on this page: 
[https://learn.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python?tabs=managed-identity%2Croles-azure-portal%2Csign-in-azure-cli&pivots=blob-storage-quickstart-scratch)
"""
    _s = Store(store_location=store_folder)

    if output_type not in VALID_TYPES:
        LOGGER.error(f"{output_type} is not a valid type of output")
        raise ValueError(f"{output_type} is not a valid type of output")

    container_client = (BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net/",
        credential=ClientSecretCredential(tenant_id=tenant_id,
                                          client_id=client_id,
                                          client_secret=client_secret))
                           .get_container_client(container_name))

    def data_upload(data_stream: BytesIO, file_name: str):
        uploaded_file_name = file_prefix + file_name
        data_stream.seek(0)
        size = len(data_stream.read())
        data_stream.seek(0)

        LOGGER.info(f"  Sending {size} bytes of data")
        container_client.upload_blob(name=uploaded_file_name, data=data_stream, length=size, overwrite=True)

    if output_type == "sqlite":
        _file_path = _s._database_path
        _file_name = "db.sqlite"
        _uploaded_file_name = file_prefix + _file_name
        LOGGER.info(f"Sending {_file_path} as {_uploaded_file_name}")
        with open(_file_path, "rb") as data:
            container_client.upload_blob(name=_uploaded_file_name, data=data, overwrite=True)
    else:
        tables = list(_s.list_tables())
        for table_name in tables:
            _data_stream = BytesIO()
            _file_name = None
            _data = _s.get_table(table_name)
            if not len(_data):
                LOGGER.info(f"Table {table_name} is empty (skipping)")
                continue
            if output_type == "csv":
                _file_name = table_name + ".csv"
                pc.write_csv(_data, _data_stream)
            elif output_type == "parquet":
                _file_name = table_name + ".parquet"
                pq.write_table(_data, _data_stream)
            LOGGER.info(f"Sending table {table_name} as {output_type}")
            data_upload(_data_stream, _file_name)
