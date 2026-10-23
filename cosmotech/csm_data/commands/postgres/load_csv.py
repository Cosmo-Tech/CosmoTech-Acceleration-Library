# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.utils.click import click
from cosmotech.csm_data.utils.decorators import translate_help, web_help


@click.command()
@web_help("csm-data/postgres/load-csv")
@translate_help("csm_data.commands.postgres.load_csv.description")
@click.option(
    "--csv-file",
    help=T("csm_data.commands.postgres.load_csv.parameters.csv_file"),
    metavar="PATH",
    type=str,
    default=None,
)
@click.option(
    "--csv-folder",
    help=T("csm_data.commands.postgres.load_csv.parameters.csv_folder"),
    metavar="PATH",
    type=str,
    default=None,
)
@click.option(
    "--table",
    help=T("csm_data.commands.postgres.load_csv.parameters.table"),
    metavar="NAME",
    type=str,
    default=None,
)
@click.option(
    "--table-prefix",
    help=T("csm_data.commands.postgres.load_csv.parameters.table_prefix"),
    metavar="PREFIX",
    type=str,
    default=None,
)
@click.option(
    "--add-id/--no-add-id",
    "add_id",
    help=T("csm_data.commands.postgres.load_csv.parameters.add_id"),
    default=False,
    is_flag=True,
    show_default=True,
)
@click.option(
    "--run-id",
    help=T("csm_data.commands.postgres.load_csv.parameters.run_id"),
    envvar="CSM_RUN_ID",
    show_envvar=True,
    type=str,
    default=None,
)
@click.option(
    "--postgres-host",
    help=T("csm_data.commands.postgres.load_csv.parameters.postgres_host"),
    envvar="POSTGRES_HOST_URI",
    show_envvar=True,
    required=False,
)
@click.option(
    "--postgres-port",
    help=T("csm_data.commands.postgres.load_csv.parameters.postgres_port"),
    envvar="POSTGRES_HOST_PORT",
    show_envvar=True,
    required=False,
    default=5432,
)
@click.option(
    "--postgres-db",
    help=T("csm_data.commands.postgres.load_csv.parameters.postgres_db"),
    envvar="POSTGRES_DB_NAME",
    show_envvar=True,
    required=False,
)
@click.option(
    "--postgres-schema",
    help=T("csm_data.commands.postgres.load_csv.parameters.postgres_schema"),
    envvar="POSTGRES_DB_SCHEMA",
    show_envvar=True,
    required=False,
)
@click.option(
    "--postgres-user",
    help=T("csm_data.commands.postgres.load_csv.parameters.postgres_user"),
    envvar="POSTGRES_USER_NAME",
    show_envvar=True,
    required=False,
)
@click.option(
    "--postgres-password",
    help=T("csm_data.commands.postgres.load_csv.parameters.postgres_password"),
    envvar="POSTGRES_USER_PASSWORD",
    show_envvar=True,
    required=False,
)
@click.option(
    "--encode-password/--no-encode-password",
    "force_encode",
    help=T("csm_data.commands.postgres.load_csv.parameters.encode_password"),
    envvar="CSM_PSQL_FORCE_PASSWORD_ENCODING",
    show_envvar=True,
    default=True,
    is_flag=True,
    show_default=True,
)
def load_csv(
    csv_file,
    csv_folder,
    table,
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
        CsvIngestionError,
        CsvIngestionOptionError,
        ingest_csv_file,
        ingest_csv_folder,
        validate_options,
    )
    from cosmotech.coal.postgresql.postgres_connection import (
        PostgresConnectionError,
        PostgresConnectionOptionError,
        resolve_postgres_configuration,
    )

    try:
        validate_options(
            csv_file=csv_file,
            csv_folder=csv_folder,
            table=table,
            table_prefix=table_prefix,
            add_id=add_id,
            run_id=run_id,
        )
        configuration = resolve_postgres_configuration(
            host=postgres_host,
            port=postgres_port,
            db=postgres_db,
            schema=postgres_schema,
            user=postgres_user,
            password=postgres_password,
            encode_password=force_encode,
        )
        effective_run_id = run_id if add_id else None
        effective_prefix = DEFAULT_TABLE_PREFIX if table_prefix is None else table_prefix
        if csv_file is not None:
            ingest_csv_file(
                csv_path=csv_file,
                configuration=configuration,
                table=table,
                table_prefix=effective_prefix,
                run_id=effective_run_id,
            )
        else:
            ingest_csv_folder(
                folder=csv_folder,
                configuration=configuration,
                table_prefix=effective_prefix,
                run_id=effective_run_id,
            )
    except (CsvIngestionOptionError, PostgresConnectionOptionError) as err:
        raise click.UsageError(str(err)) from err
    except (CsvIngestionError, PostgresConnectionError) as err:
        raise click.ClickException(str(err)) from err
