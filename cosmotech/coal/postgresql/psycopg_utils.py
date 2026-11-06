# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
PostgreSQL connection utilities (psycopg).

This module resolves the connection settings (from options, the configuration file or the environment)
and opens psycopg connections.

``psycopg`` is an optional dependency (``custom`` extra), so it is imported lazily, so that importing this module
never fails where it is not installed.
"""

from typing import Optional

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.postgresql.utils import PostgresUtils
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION as EC
from cosmotech.coal.utils.configuration import Configuration

DEFAULT_PORT = 5432

# Connection settings that select the source (options or configuration), with the option name used in messages.
# The password does not select the source and the port has a default value.
CONNECTION_SET = (
    ("host", "--postgres-host"),
    ("db_name", "--postgres-db"),
    ("db_schema", "--postgres-schema"),
    ("user_name", "--postgres-user"),
)
REQUIRED_CONFIGURATION_KEYS = ("host", "db_name", "db_schema", "user_name", "user_password")


class PostgresConnectionError(Exception):
    """Base class of the errors raised while resolving settings or connecting."""


class PostgresConnectionOptionError(PostgresConnectionError):
    """Partial or incomplete connection settings."""


def import_psycopg():
    """
    Import psycopg lazily.

    Returns:
        The ``psycopg`` module and its ``sql`` module

    Raises:
        PostgresConnectionError: if psycopg (or its libpq) cannot be imported
    """
    try:
        import psycopg
        from psycopg import sql
    except ImportError as err:
        raise PostgresConnectionError(T("coal.services.postgres_connection.psycopg_missing")) from err
    return psycopg, sql


def resolve_postgres_configuration(
    host: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[str] = None,
    schema: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    encode_password: bool = True,
    configuration: Configuration = EC,
) -> Configuration:
    """
    Build the connection configuration from one single source: all the options or the configuration.

    - host, db, schema and user all given: they are used, with the password (required in this case).
    - none of them given: ``configuration.postgres`` is used (configuration file or environment variables).
    - only some of them given: error, nothing is completed from the configuration.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port (default 5432)
        db: PostgreSQL database name
        schema: PostgreSQL schema
        user: PostgreSQL user name
        password: PostgreSQL password
        encode_password: force password encoding to percent encoding
        configuration: coal Configuration used when no option is given

    Returns:
        A Configuration with a postgres section

    Raises:
        PostgresConnectionOptionError: if the settings are partial or incomplete
    """
    # host, db, schema and user are the options that select the source of the settings (the password does not)
    given = {"host": host, "db_name": db, "db_schema": schema, "user_name": user}
    missing_options = [option for key, option in CONNECTION_SET if not given[key]]
    none_given = len(missing_options) == len(CONNECTION_SET)
    all_given = not missing_options

    if none_given:
        # Case 1: no option given, the settings come from the configuration only
        if configuration.safe_get("postgres") is None:
            outputs = [output for output in configuration.safe_get("outputs", []) if output.type == "postgres"]
            if len(outputs) > 1:
                raise PostgresConnectionOptionError(T("coal.services.postgres_connection.ambiguous_outputs"))
            if outputs:
                configuration = Configuration(outputs[0].conf)
        missing_keys = [key for key in REQUIRED_CONFIGURATION_KEYS if not configuration.safe_get(f"postgres.{key}")]
        if missing_keys:
            raise PostgresConnectionOptionError(
                T("coal.services.postgres_connection.incomplete_config").format(missing=", ".join(missing_keys))
            )
        if not configuration.safe_get("postgres.port"):
            configuration.postgres.port = DEFAULT_PORT
        return configuration

    if not all_given:
        # Case 2: only some options given. Refused, so that the options and the configuration are never mixed
        raise PostgresConnectionOptionError(
            T("coal.services.postgres_connection.partial_options").format(missing=", ".join(missing_options))
        )

    # Case 3: all options given (cases 1 and 2 returned or raised above), the settings come from the options only.
    # The password is not one of the options checked above, so it is checked here.
    if not password:
        raise PostgresConnectionOptionError(T("coal.services.postgres_connection.password_missing"))
    return Configuration(
        {
            "postgres": {
                "host": host,
                "port": port or DEFAULT_PORT,
                "db_name": db,
                "db_schema": schema,
                "user_name": user,
                "user_password": password,
                "password_encoding": encode_password,
            }
        }
    )


def connect(configuration: Configuration, autocommit: bool = False):
    """
    Connect with the URI built by PostgresUtils and point the search_path to the configured schema.

    Args:
        configuration: coal Configuration with a postgres section
        autocommit: psycopg autocommit mode

    Returns:
        A psycopg connection

    Raises:
        PostgresConnectionError: if the connection fails
    """
    psycopg, sql = import_psycopg()
    psql = PostgresUtils(configuration)
    conn = None
    try:
        conn = psycopg.connect(psql.full_uri, autocommit=autocommit)
        conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(psql.db_schema)))
    except psycopg.Error as err:
        if conn is not None:
            conn.close()
        raise PostgresConnectionError(
            T("coal.services.postgres_connection.connection_failed").format(error=err)
        ) from err
    return conn
