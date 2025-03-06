# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Optional
from urllib.parse import quote

import adbc_driver_manager
import pyarrow as pa
from adbc_driver_postgresql import dbapi
from pyarrow import Table

from cosmotech.coal.utils.logger import LOGGER


def generate_postgresql_full_uri(
    postgres_host: str,
    postgres_port: str,
    postgres_db: str,
    postgres_user: str,
    postgres_password: str, 
    force_encode: bool = False) -> str:
    # Check if password needs percent encoding (contains special characters)
    # We don't log anything about the password for security
    encoded_password = postgres_password
    if force_encode:
        encoded_password = quote(postgres_password, safe='')
    
    return ('postgresql://' +
            f'{postgres_user}'
            f':{encoded_password}'
            f'@{postgres_host}'
            f':{postgres_port}'
            f'/{postgres_db}')


def get_postgresql_table_schema(
    target_table_name: str,
    postgres_host: str,
    postgres_port: str,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
) -> Optional[pa.Schema]:
    """
    Get the schema of an existing PostgreSQL table using SQL queries.
    
    Args:
        target_table_name: Name of the table
        postgres_host: PostgreSQL host
        postgres_port: PostgreSQL port
        postgres_db: PostgreSQL database name
        postgres_schema: PostgreSQL schema name
        postgres_user: PostgreSQL username
        postgres_password: PostgreSQL password
        
    Returns:
        PyArrow Schema if table exists, None otherwise
    """
    LOGGER.debug(f"Getting schema for table {postgres_schema}.{target_table_name}")

    postgresql_full_uri = generate_postgresql_full_uri(postgres_host,
                                                       postgres_port,
                                                       postgres_db,
                                                       postgres_user,
                                                       postgres_password)

    with (dbapi.connect(postgresql_full_uri) as conn):
        try:
            return conn.adbc_get_table_schema(
                target_table_name,
                db_schema_filter=postgres_schema,
            )
        except adbc_driver_manager.ProgrammingError:
            LOGGER.warning(f"Table {postgres_schema}.{target_table_name} not found")
        return None


def adapt_table_to_schema(
    data: pa.Table,
    target_schema: pa.Schema
) -> pa.Table:
    """
    Adapt a PyArrow table to match a target schema with detailed logging.
    """
    LOGGER.debug(f"Starting schema adaptation for table with {len(data)} rows")
    LOGGER.debug(f"Original schema: {data.schema}")
    LOGGER.debug(f"Target schema: {target_schema}")

    target_fields = {field.name: field.type for field in target_schema}
    new_columns = []

    # Track adaptations for summary
    added_columns = []
    dropped_columns = []
    type_conversions = []
    failed_conversions = []

    # Process each field in target schema
    for field_name, target_type in target_fields.items():
        if field_name in data.column_names:
            # Column exists - try to cast to target type
            col = data[field_name]
            original_type = col.type

            if original_type != target_type:
                LOGGER.debug(
                    f"Attempting to cast column '{field_name}' "
                    f"from {original_type} to {target_type}"
                )
                try:
                    new_col = pa.compute.cast(col, target_type)
                    new_columns.append(new_col)
                    type_conversions.append(
                        f"{field_name}: {original_type} -> {target_type}"
                    )
                except pa.ArrowInvalid as e:
                    LOGGER.warning(
                        f"Failed to cast column '{field_name}' "
                        f"from {original_type} to {target_type}. "
                        f"Filling with nulls. Error: {str(e)}"
                    )
                    new_columns.append(pa.nulls(len(data), type=target_type))
                    failed_conversions.append(
                        f"{field_name}: {original_type} -> {target_type}"
                    )
            else:
                new_columns.append(col)
        else:
            # Column doesn't exist - add nulls
            LOGGER.debug(f"Adding missing column '{field_name}' with null values")
            new_columns.append(pa.nulls(len(data), type=target_type))
            added_columns.append(field_name)

    # Log columns that will be dropped
    dropped_columns = [
        name for name in data.column_names
        if name not in target_fields
    ]
    if dropped_columns:
        LOGGER.debug(
            f"Dropping extra columns not in target schema: {dropped_columns}"
        )

    # Create new table
    adapted_table = pa.Table.from_arrays(
        new_columns,
        schema=target_schema
    )

    # Log summary of adaptations
    LOGGER.debug("Schema adaptation summary:")
    if added_columns:
        LOGGER.debug(f"- Added columns (filled with nulls): {added_columns}")
    if dropped_columns:
        LOGGER.debug(f"- Dropped columns: {dropped_columns}")
    if type_conversions:
        LOGGER.debug(f"- Successful type conversions: {type_conversions}")
    if failed_conversions:
        LOGGER.debug(
            f"- Failed conversions (filled with nulls): {failed_conversions}"
        )

    LOGGER.debug(f"Final adapted table schema: {adapted_table.schema}")
    return adapted_table


def send_pyarrow_table_to_postgresql(
    data: Table,
    target_table_name: str,
    postgres_host: str,
    postgres_port: str,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
    replace: bool
) -> int:
    LOGGER.debug(
        f"Preparing to send data to PostgreSQL table '{postgres_schema}.{target_table_name}'"
    )
    LOGGER.debug(f"Input table has {len(data)} rows")

    # Get existing schema if table exists
    existing_schema = get_postgresql_table_schema(
        target_table_name,
        postgres_host,
        postgres_port,
        postgres_db,
        postgres_schema,
        postgres_user,
        postgres_password
    )

    if existing_schema is not None:
        LOGGER.debug(f"Found existing table with schema: {existing_schema}")
        if not replace:
            LOGGER.debug("Adapting incoming data to match existing schema")
            data = adapt_table_to_schema(data, existing_schema)
        else:
            LOGGER.debug("Replace mode enabled - skipping schema adaptation")
    else:
        LOGGER.debug("No existing table found - will create new table")

    # Proceed with ingestion
    total = 0
    postgresql_full_uri = generate_postgresql_full_uri(
        postgres_host,
        postgres_port,
        postgres_db,
        postgres_user,
        postgres_password
    )

    LOGGER.debug("Connecting to PostgreSQL database")
    with dbapi.connect(postgresql_full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            LOGGER.debug(
                f"Ingesting data with mode: {'replace' if replace else 'create_append'}"
            )
            total += curs.adbc_ingest(
                target_table_name,
                data,
                "replace" if replace else "create_append",
                db_schema_name=postgres_schema
            )

    LOGGER.debug(f"Successfully ingested {total} rows")
    return total
