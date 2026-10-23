# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
PostgreSQL connection module (psycopg).

This module resolves the connection settings (from options, the configuration file or the environment)
and opens psycopg connections.

``psycopg`` is imported lazily, so that importing this module never fails where it is not installed.
"""

from typing import Optional
from urllib.parse import quote

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.postgresql.utils import PostgresUtils
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


def postgres_configuration(configuration: Configuration) -> Configuration:
    """
    Return a configuration exposing ``.postgres``.

    It is taken from the ``[[outputs]]`` entry of type postgres of the configuration file if there is one,
    else from the POSTGRES_* environment variables.

    Args:
        configuration: coal Configuration

    Returns:
        A Configuration with a postgres section
    """
    for output in configuration.safe_get("outputs") or []:
        postgres = (output.get("conf") or {}).get("postgres") if output.get("type") == "postgres" else None
        if postgres is not None:
            return Configuration({"postgres": postgres})
    return Configuration(Configuration.CONVERSION_DICT)


def resolve_postgres_configuration(
    host: Optional[str] = None,
    port: Optional[int] = None,
    db: Optional[str] = None,
    schema: Optional[str] = None,
    user: Optional[str] = None,
    password: Optional[str] = None,
    encode_password: bool = True,
    configuration: Optional[Configuration] = None,
) -> Configuration:
    """
    Build the connection configuration from one single source: all the options or the configuration.

    - host, db, schema and user all given: they are used, with the password (required in this case).
    - none of them given: the configuration file, then the environment variables, are used.
    - only some of them given: error, nothing is completed from the configuration.

    Args:
        host: PostgreSQL host
        port: PostgreSQL port (default 5432)
        db: PostgreSQL database name
        schema: PostgreSQL schema
        user: PostgreSQL user name
        password: PostgreSQL password
        encode_password: force password encoding to percent encoding
        configuration: base coal Configuration, loaded by Configuration() if not given

    Returns:
        A Configuration with a postgres section

    Raises:
        PostgresConnectionOptionError: if the settings are partial or incomplete
    """
    given = {"host": host, "db_name": db, "db_schema": schema, "user_name": user}
    missing_options = [option for key, option in CONNECTION_SET if not given[key]]

    if not missing_options:
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

    if len(missing_options) < len(CONNECTION_SET):
        raise PostgresConnectionOptionError(
            T("coal.services.postgres_connection.partial_options").format(missing=", ".join(missing_options))
        )

    postgres_conf = postgres_configuration(configuration if configuration is not None else Configuration())
    missing_keys = [key for key in REQUIRED_CONFIGURATION_KEYS if not postgres_conf.safe_get(f"postgres.{key}")]
    if missing_keys:
        raise PostgresConnectionOptionError(
            T("coal.services.postgres_connection.incomplete_config").format(missing=", ".join(missing_keys))
        )
    if not postgres_conf.safe_get("postgres.port"):
        postgres_conf.postgres.port = DEFAULT_PORT
    return postgres_conf


def _redact(error: Exception, psql: PostgresUtils) -> str:
    """Return the error message without the password."""
    message = str(error)
    for secret in {psql.user_password, quote(psql.user_password or "", safe="")}:
        if secret:
            message = message.replace(secret, "***")
    return message


def connect(configuration: Configuration, autocommit: bool = False):
    """
    Connect with the URI built by PostgresUtils and point the search_path to the configured schema.

    Args:
        configuration: coal Configuration with a postgres section
        autocommit: psycopg autocommit mode

    Returns:
        A psycopg connection

    Raises:
        PostgresConnectionError: if the connection fails (the message never contains the password)
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
            T("coal.services.postgres_connection.connection_failed").format(error=_redact(err, psql))
        ) from None
    return conn
