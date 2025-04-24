# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Optional

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import web_help, translate_help
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
    help=T("csm-data.commands.store.dump_to_s3.parameters.store_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--output-type",
    default="sqlite",
    help=T("csm-data.commands.store.dump_to_s3.parameters.output_type"),
    type=click.Choice(VALID_TYPES, case_sensitive=False),
)
@click.option(
    "--bucket-name",
    envvar="CSM_DATA_BUCKET_NAME",
    help=T("csm-data.commands.store.dump_to_s3.parameters.bucket_name"),
    metavar="BUCKET",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--prefix",
    "file_prefix",
    envvar="CSM_DATA_BUCKET_PREFIX",
    help=T("csm-data.commands.store.dump_to_s3.parameters.prefix"),
    metavar="PREFIX",
    type=str,
    show_envvar=True,
    default="",
)
@click.option(
    "--use-ssl/--no-ssl",
    default=True,
    help=T("csm-data.commands.store.dump_to_s3.parameters.use_ssl"),
    type=bool,
    is_flag=True,
)
@click.option(
    "--s3-url",
    "endpoint_url",
    help=T("csm-data.commands.store.dump_to_s3.parameters.s3_url"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="URL",
    envvar="AWS_ENDPOINT_URL",
)
@click.option(
    "--access-id",
    "access_id",
    help=T("csm-data.commands.store.dump_to_s3.parameters.access_id"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_ACCESS_KEY_ID",
)
@click.option(
    "--secret-key",
    "secret_key",
    help=T("csm-data.commands.store.dump_to_s3.parameters.secret_key"),
    type=str,
    required=True,
    show_envvar=True,
    metavar="ID",
    envvar="AWS_SECRET_ACCESS_KEY",
)
@click.option(
    "--ssl-cert-bundle",
    help=T("csm-data.commands.store.dump_to_s3.parameters.ssl_cert_bundle"),
    type=str,
    show_envvar=True,
    metavar="PATH",
    envvar="CSM_S3_CA_BUNDLE",
)
@web_help("csm-data/store/dump-to-s3")
@translate_help("csm-data.commands.store.dump_to_s3.description")
def dump_to_s3(
    store_folder,
    bucket_name: str,
    endpoint_url: str,
    access_id: str,
    secret_key: str,
    output_type: str,
    file_prefix: str = "",
    use_ssl: bool = True,
    ssl_cert_bundle: Optional[str] = None,
):
    # Import the modules and functions at the start of the command
    from io import BytesIO
    import pyarrow.csv as pc
    import pyarrow.parquet as pq
    from cosmotech.coal.aws import create_s3_client, upload_data_stream
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.utils.logger import LOGGER

    _s = Store(store_location=store_folder)

    if output_type not in VALID_TYPES:
        LOGGER.error(T("coal.common.errors.data_invalid_output_type").format(output_type=output_type))
        raise ValueError(T("coal.common.errors.data_invalid_output_type").format(output_type=output_type))

    # Create S3 client
    s3_client = create_s3_client(
        endpoint_url=endpoint_url,
        access_id=access_id,
        secret_key=secret_key,
        use_ssl=use_ssl,
        ssl_cert_bundle=ssl_cert_bundle,
    )

    if output_type == "sqlite":
        _file_path = _s._database_path
        _file_name = "db.sqlite"
        _uploaded_file_name = file_prefix + _file_name
        LOGGER.info(
            T("coal.common.data_transfer.file_sent").format(file_path=_file_path, uploaded_name=_uploaded_file_name)
        )
        s3_client.upload_file(_file_path, bucket_name, _uploaded_file_name)
    else:
        tables = list(_s.list_tables())
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
            upload_data_stream(
                data_stream=_data_stream,
                bucket_name=bucket_name,
                s3_client=s3_client,
                file_name=_file_name,
                file_prefix=file_prefix,
            )
