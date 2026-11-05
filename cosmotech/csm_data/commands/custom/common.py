# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""Options and error handling shared by the custom PostgreSQL CSV commands."""

from contextlib import contextmanager

from cosmotech.orchestrator.utils.translate import T

from cosmotech.csm_data.utils.click import click

PARAMETERS = "csm_data.commands.custom.common.parameters"

# Listed in the order they are displayed in the help
CONNECTION_OPTIONS = (
    click.option(
        "--add-id/--no-add-id",
        "add_id",
        help=T(f"{PARAMETERS}.add_id"),
        default=False,
        is_flag=True,
        show_default=True,
    ),
    click.option(
        "--run-id",
        help=T(f"{PARAMETERS}.run_id"),
        envvar="CSM_RUN_ID",
        show_envvar=True,
        type=str,
        default=None,
    ),
    click.option(
        "--postgres-host",
        help=T(f"{PARAMETERS}.postgres_host"),
        envvar="POSTGRES_HOST_URI",
        show_envvar=True,
        required=False,
    ),
    click.option(
        "--postgres-port",
        help=T(f"{PARAMETERS}.postgres_port"),
        envvar="POSTGRES_HOST_PORT",
        show_envvar=True,
        required=False,
        default=5432,
    ),
    click.option(
        "--postgres-db",
        help=T(f"{PARAMETERS}.postgres_db"),
        envvar="POSTGRES_DB_NAME",
        show_envvar=True,
        required=False,
    ),
    click.option(
        "--postgres-schema",
        help=T(f"{PARAMETERS}.postgres_schema"),
        envvar="POSTGRES_DB_SCHEMA",
        show_envvar=True,
        required=False,
    ),
    click.option(
        "--postgres-user",
        help=T(f"{PARAMETERS}.postgres_user"),
        envvar="POSTGRES_USER_NAME",
        show_envvar=True,
        required=False,
    ),
    click.option(
        "--postgres-password",
        help=T(f"{PARAMETERS}.postgres_password"),
        envvar="POSTGRES_USER_PASSWORD",
        show_envvar=True,
        required=False,
    ),
    click.option(
        "--encode-password/--no-encode-password",
        "force_encode",
        help=T(f"{PARAMETERS}.encode_password"),
        envvar="CSM_PSQL_FORCE_PASSWORD_ENCODING",
        show_envvar=True,
        default=True,
        is_flag=True,
        show_default=True,
    ),
)


def connection_options(func):
    """Add the run id and PostgreSQL connection options to a command."""
    for option in reversed(CONNECTION_OPTIONS):
        func = option(func)
    return func


@contextmanager
def ingestion_errors():
    """Turn the ingestion and connection errors into click errors: usage error (2) or failure (1)."""
    from cosmotech.coal.postgresql.native_csv_ingestion import (
        CsvIngestionError,
        CsvIngestionOptionError,
    )
    from cosmotech.coal.postgresql.postgres_connection import (
        PostgresConnectionError,
        PostgresConnectionOptionError,
    )

    try:
        yield
    except (CsvIngestionOptionError, PostgresConnectionOptionError) as err:
        raise click.UsageError(str(err)) from err
    except (CsvIngestionError, PostgresConnectionError) as err:
        raise click.ClickException(str(err)) from err
