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
from cosmotech.orchestrator.utils.translate import T
from pyarrow import Table

from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER


class PostgresUtils:

    def __init__(self, configuration: Configuration):
        self._configuration = configuration.postgres

    @property
    def table_prefix(self):
        if "table_prefix" in self._configuration:
            return self._configuration.table_prefix
        return "Cosmotech_"

    @property
    def db_name(self):
        return self._configuration.db_name

    @property
    def db_schema(self):
        return self._configuration.db_schema

    @property
    def host_uri(self):
        return self._configuration.host

    @property
    def host_port(self):
        return self._configuration.port

    @property
    def user_name(self):
        return self._configuration.user_name

    @property
    def user_password(self):
        return self._configuration.user_password

    @property
    def password_encoding(self):
        if "password_encoding" in self._configuration:
            return self._configuration.password_encoding
        return False

    @property
    def full_uri(self) -> str:
        # Check if password needs percent encoding (contains special characters)
        # We don't log anything about the password for security
        encoded_password = self.user_password
        if self.password_encoding:
            encoded_password = quote(self.user_password, safe="")

        return (
            "postgresql://" + f"{self.user_name}"
            f":{encoded_password}"
            f"@{self.host_uri}"
            f":{self.host_port}"
            f"/{self.db_name}"
        )

    def metadata_table_name(self):
        return f"{self.table_prefix}RunnerMetadata"

    def get_postgresql_table_schema(self, target_table_name: str) -> Optional[pa.Schema]:
        """
        Get the schema of an existing PostgreSQL table using SQL queries.

        Args:
            target_table_name: Name of the table

        Returns:
            PyArrow Schema if table exists, None otherwise
        """
        LOGGER.debug(
            T("coal.services.postgresql.getting_schema").format(
                postgres_schema=self.db_schema, target_table_name=target_table_name
            )
        )

        with dbapi.connect(self.full_uri) as conn:
            try:
                return conn.adbc_get_table_schema(
                    target_table_name,
                    db_schema_filter=self.db_schema,
                )
            except adbc_driver_manager.ProgrammingError:
                LOGGER.warning(
                    T("coal.services.postgresql.table_not_found").format(
                        postgres_schema=self.db_schema, target_table_name=target_table_name
                    )
                )
            return None

    def send_pyarrow_table_to_postgresql(
        self,
        data: Table,
        target_table_name: str,
        replace: bool,
    ) -> int:
        LOGGER.debug(
            T("coal.services.postgresql.preparing_send").format(
                postgres_schema=self.db_schema, target_table_name=target_table_name
            )
        )
        LOGGER.debug(T("coal.services.postgresql.input_rows").format(rows=len(data)))

        # Get existing schema if table exists
        existing_schema = self.get_postgresql_table_schema(target_table_name)

        if existing_schema is not None:
            LOGGER.debug(T("coal.services.postgresql.found_existing_table").format(schema=existing_schema))
            if not replace:
                LOGGER.debug(T("coal.services.postgresql.adapting_data"))
                data = adapt_table_to_schema(data, existing_schema)
            else:
                LOGGER.debug(T("coal.services.postgresql.replace_mode"))
        else:
            LOGGER.debug(T("coal.services.postgresql.no_existing_table"))

        # Proceed with ingestion
        total = 0

        LOGGER.debug(T("coal.services.postgresql.connecting"))
        with dbapi.connect(self.full_uri, autocommit=True) as conn:
            with conn.cursor() as curs:
                mode = "replace" if replace else "create_append"
                LOGGER.debug(T("coal.services.postgresql.ingesting_data").format(mode=mode))
                total += curs.adbc_ingest(target_table_name, data, mode, db_schema_name=self.db_schema)

        LOGGER.debug(T("coal.services.postgresql.ingestion_success").format(rows=total))
        return total

    def add_fk_constraint(
        self,
        from_table: str,
        from_col: str,
        to_table: str,
        to_col: str,
    ) -> None:
        # Connect to PostgreSQL and remove runner metadata row
        with dbapi.connect(self.full_uri, autocommit=True) as conn:
            with conn.cursor() as curs:
                sql_add_fk = f"""
                    ALTER TABLE {self.db_schema}.{from_table}
                    CONSTRAINT metadata FOREIGN KEY ({from_col}) REFERENCES {to_table}({to_col})
                """
                curs.execute(sql_add_fk)
                conn.commit()

    def is_metadata_exists(self) -> None:
        with dbapi.connect(self.full_uri, autocommit=True) as conn:
            try:
                conn.adbc_get_table_schema(
                    self.metadata_table_name,
                    db_schema_filter=self.db_schema,
                )
                return True
            except adbc_driver_manager.ProgrammingError:
                return False


def adapt_table_to_schema(data: pa.Table, target_schema: pa.Schema) -> pa.Table:
    """
    Adapt a PyArrow table to match a target schema with detailed logging.
    """
    LOGGER.debug(T("coal.services.postgresql.schema_adaptation_start").format(rows=len(data)))
    LOGGER.debug(T("coal.services.postgresql.original_schema").format(schema=data.schema))
    LOGGER.debug(T("coal.services.postgresql.target_schema").format(schema=target_schema))

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
                    T("coal.services.postgresql.casting_column").format(
                        field_name=field_name,
                        original_type=original_type,
                        target_type=target_type,
                    )
                )
                try:
                    new_col = pa.compute.cast(col, target_type)
                    new_columns.append(new_col)
                    type_conversions.append(f"{field_name}: {original_type} -> {target_type}")
                except pa.ArrowInvalid as e:
                    LOGGER.warning(
                        T("coal.services.postgresql.cast_failed").format(
                            field_name=field_name,
                            original_type=original_type,
                            target_type=target_type,
                            error=str(e),
                        )
                    )
                    new_columns.append(pa.nulls(len(data), type=target_type))
                    failed_conversions.append(f"{field_name}: {original_type} -> {target_type}")
            else:
                new_columns.append(col)
        else:
            # Column doesn't exist - add nulls
            LOGGER.debug(T("coal.services.postgresql.adding_missing_column").format(field_name=field_name))
            new_columns.append(pa.nulls(len(data), type=target_type))
            added_columns.append(field_name)

    # Log columns that will be dropped
    dropped_columns = [name for name in data.column_names if name not in target_fields]
    if dropped_columns:
        LOGGER.debug(T("coal.services.postgresql.dropping_columns").format(columns=dropped_columns))

    # Create new table
    adapted_table = pa.Table.from_arrays(new_columns, schema=target_schema)

    # Log summary of adaptations
    LOGGER.debug(T("coal.services.postgresql.adaptation_summary"))
    if added_columns:
        LOGGER.debug(T("coal.services.postgresql.added_columns").format(columns=added_columns))
    if dropped_columns:
        LOGGER.debug(T("coal.services.postgresql.dropped_columns").format(columns=dropped_columns))
    if type_conversions:
        LOGGER.debug(T("coal.services.postgresql.successful_conversions").format(conversions=type_conversions))
    if failed_conversions:
        LOGGER.debug(T("coal.services.postgresql.failed_conversions").format(conversions=failed_conversions))

    LOGGER.debug(T("coal.services.postgresql.final_schema").format(schema=adapted_table.schema))
    return adapted_table
