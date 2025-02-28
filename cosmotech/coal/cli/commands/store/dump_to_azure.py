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
from cosmotech.coal.cli.utils.decorators import web_help, translate_help
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T

VALID_TYPES = (
    "sqlite",
    "csv",
    "parquet",
)


@click.command()
@click.option(
    "--store-folder",
    envvar="CSM_PARAMETERS_ABSOLUTE_PATH",
    help=T("coal-help.commands.store.dump_to_azure.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--output-type",
    default="sqlite",
    help=T("coal-help.commands.store.dump_to_azure.parameters.output_type"),
    type=click.Choice(VALID_TYPES, case_sensitive=False),
)
@click.option(
    "--account-name",
    "account_name",
    envvar="AZURE_ACCOUNT_NAME",
    help=T("coal-help.commands.store.dump_to_azure.parameters.account_name"),
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--container-name",
    "container_name",
    envvar="AZURE_CONTAINER_NAME",
    help=T("coal-help.commands.store.dump_to_azure.parameters.container_name"),
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--prefix",
    "file_prefix",
    envvar="CSM_DATA_PREFIX",
    help=T("coal-help.commands.store.dump_to_azure.parameters.prefix"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--tenant-id",
    "tenant_id",
    help=T("coal-help.commands.store.dump_to_azure.parameters.tenant_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AZURE_TENANT_ID",
)
@click.option(
    "--client-id",
    "client_id",
    help=T("coal-help.commands.store.dump_to_azure.parameters.client_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AZURE_CLIENT_ID",
)
@click.option(
    "--client-secret",
    "client_secret",
    help=T("coal-help.commands.store.dump_to_azure.parameters.client_secret"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AZURE_CLIENT_SECRET",
)
@web_help("csm-data/store/dump-to-azure")
@translate_help("coal-help.commands.store.dump_to_azure.description")
def dump_to_azure(
    store_folder,
    account_name: str,
    container_name: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
    output_type: str,
    file_prefix: str,
):
    _s = Store(store_location=store_folder)

    if output_type not in VALID_TYPES:
        LOGGER.error(T("coal.errors.data.invalid_output_type").format(output_type=output_type))
        raise ValueError(T("coal.errors.data.invalid_output_type").format(output_type=output_type))

    container_client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net/",
        credential=ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret),
    ).get_container_client(container_name)

    def data_upload(data_stream: BytesIO, file_name: str):
        uploaded_file_name = file_prefix + file_name
        data_stream.seek(0)
        size = len(data_stream.read())
        data_stream.seek(0)

        LOGGER.info(T("coal.logs.data_transfer.sending_data").format(size=size))
        container_client.upload_blob(name=uploaded_file_name, data=data_stream, length=size, overwrite=True)

    if output_type == "sqlite":
        _file_path = _s._database_path
        _file_name = "db.sqlite"
        _uploaded_file_name = file_prefix + _file_name
        LOGGER.info(
            T("coal.logs.data_transfer.file_sent").format(file_path=_file_path, uploaded_name=_uploaded_file_name)
        )
        with open(_file_path, "rb") as data:
            container_client.upload_blob(name=_uploaded_file_name, data=data, overwrite=True)
    else:
        tables = list(_s.list_tables())
        for table_name in tables:
            _data_stream = BytesIO()
            _file_name = None
            _data = _s.get_table(table_name)
            if not len(_data):
                LOGGER.info(T("coal.logs.data_transfer.table_empty").format(table_name=table_name))
                continue
            if output_type == "csv":
                _file_name = table_name + ".csv"
                pc.write_csv(_data, _data_stream)
            elif output_type == "parquet":
                _file_name = table_name + ".parquet"
                pq.write_table(_data, _data_stream)
            LOGGER.info(
                T("coal.logs.data_transfer.sending_table").format(table_name=table_name, output_type=output_type)
            )
            data_upload(_data_stream, _file_name)
