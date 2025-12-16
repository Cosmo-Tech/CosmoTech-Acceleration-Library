# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Azure Blob Storage operations module.

This module provides functions for interacting with Azure Blob Storage,
including uploading data from the Store.
"""

from io import BytesIO

import pyarrow.csv as pc
import pyarrow.parquet as pq
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER

VALID_TYPES = (
    "sqlite",
    "csv",
    "parquet",
)


def dump_store_to_azure(
    configuration: Configuration = Configuration(),
    selected_tables: list[str] = [],
) -> None:
    """
    Dump Store data to Azure Blob Storage.

    Args:
        configuration: Configuration utils class
        selected_tables: List of tables name

    Raises:
        ValueError: If the output type is invalid
    """
    _s = Store(configuration=configuration)
    output_type = configuration.safe_get("azure.output_type", default="sqlite")
    file_prefix = configuration.safe_get("azure.file_prefix", default="")

    if output_type not in VALID_TYPES:
        LOGGER.error(T("coal.common.validation.invalid_output_type").format(output_type=output_type))
        raise ValueError(T("coal.common.validation.invalid_output_type").format(output_type=output_type))

    container_client = BlobServiceClient(
        account_url=f"https://{configuration.azure.account_name}.blob.core.windows.net/",
        credential=ClientSecretCredential(
            tenant_id=configuration.azure.tenant_id,
            client_id=configuration.azure.client_id,
            client_secret=configuration.azure.client_secret,
        ),
    ).get_container_client(configuration.azure.container_name)

    def data_upload(data_stream: BytesIO, file_name: str):
        uploaded_file_name = file_prefix + file_name
        data_stream.seek(0)
        size = len(data_stream.read())
        data_stream.seek(0)

        LOGGER.info(T("coal.common.data_transfer.sending_data").format(size=size))
        container_client.upload_blob(name=uploaded_file_name, data=data_stream, length=size, overwrite=True)

    if output_type == "sqlite":
        _file_path = _s._database_path
        _file_name = "db.sqlite"
        _uploaded_file_name = file_prefix + _file_name
        LOGGER.info(
            T("coal.common.data_transfer.file_sent").format(file_path=_file_path, uploaded_name=_uploaded_file_name)
        )
        with open(_file_path, "rb") as data:
            container_client.upload_blob(name=_uploaded_file_name, data=data, overwrite=True)
    else:
        tables = list(_s.list_tables())
        if selected_tables:
            tables = [t for t in tables if t in selected_tables]
        for table_name in tables:
            _data_stream = BytesIO()
            _file_name = None
            _data = _s.get_table(table_name)
            if not len(_data):
                LOGGER.info(T("coal.common.data_transfer.table_empty").format(table_name=table_name))
                continue
            if output_type == "csv":
                _file_name = table_name + ".csv"
                pc.write_csv(_data, _data_stream)
            elif output_type == "parquet":
                _file_name = table_name + ".parquet"
                pq.write_table(_data, _data_stream)
            LOGGER.info(
                T("coal.common.data_transfer.sending_table").format(table_name=table_name, output_type=output_type)
            )
            data_upload(_data_stream, _file_name)
