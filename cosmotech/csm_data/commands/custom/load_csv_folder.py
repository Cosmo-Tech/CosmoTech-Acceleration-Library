# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.commands.custom.common import (
    connection_options,
    ingestion_errors,
)
from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.command()
@web_help("csm-data/custom/load-csv-folder")
@translate_help("csm_data.commands.custom.load_csv_folder.description")
@click.option(
    "--csv-folder",
    envvar="CSM_OUTPUT_ABSOLUTE_PATH",
    help=T("csm_data.commands.custom.load_csv_folder.parameters.csv_folder"),
    metavar="PATH",
    type=str,
    show_envvar=True,
    required=True,
)
@click.option(
    "--table-prefix",
    help=T("csm_data.commands.custom.load_csv_folder.parameters.table_prefix"),
    metavar="PREFIX",
    type=str,
    default=None,
)
@connection_options
def load_csv_folder(
    csv_folder,
    table_prefix,
    add_id: bool,
    run_id,
    postgres_host,
    postgres_port,
    postgres_db,
    postgres_schema,
    postgres_user,
    postgres_password,
    force_encode: bool,
):
    # Import the functions at the start of the command
    from cosmotech.coal.postgresql.native_csv_ingestion import (
        DEFAULT_TABLE_PREFIX,
        ingest_csv_folder,
        validate_options,
    )
    from cosmotech.coal.postgresql.psycopg_utils import (
        resolve_postgres_configuration,
    )

    with ingestion_errors():
        validate_options(add_id=add_id, run_id=run_id)
        configuration = resolve_postgres_configuration(
            host=postgres_host,
            port=postgres_port,
            db=postgres_db,
            schema=postgres_schema,
            user=postgres_user,
            password=postgres_password,
            encode_password=force_encode,
        )
        ingest_csv_folder(
            folder=csv_folder,
            configuration=configuration,
            table_prefix=DEFAULT_TABLE_PREFIX if table_prefix is None else table_prefix,
            run_id=run_id if add_id else None,
        )
