# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
PostgreSQL native CSV ingestion module.

This module streams CSV files straight into existing PostgreSQL tables with the native ``COPY ... FROM STDIN``
command. Python only reads bytes and forwards them: PostgreSQL parses the CSV and converts the values into the
column types declared on the table. Each file is loaded in a single transaction.

The connection is handled by ``cosmotech.coal.postgresql.postgres_connection``.
"""

import csv
import time
from pathlib import Path
from typing import Optional

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.postgresql.postgres_connection import connect, import_psycopg
from cosmotech.coal.postgresql.utils import PostgresUtils
from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER

DEFAULT_TABLE_PREFIX = "Cosmotech_"
CHUNK_SIZE = 8 * 1024 * 1024
LOG_EVERY_BYTES = 128 * 1024 * 1024
RUN_ID_SETTING = "csm.run_id"
RUN_ID_COLUMN = "csm_run_id"


class CsvIngestionError(Exception):
    """Base class of the errors raised by the CSV ingestion (failed checks, rejected loads)."""


class CsvIngestionOptionError(CsvIngestionError):
    """Invalid combination of options."""


def validate_options(
    csv_file=None,
    csv_folder=None,
    table: Optional[str] = None,
    table_prefix: Optional[str] = None,
    add_id: bool = False,
    run_id: Optional[str] = None,
) -> None:
    """
    Validate the combination of options of the load command.

    Args:
        csv_file: path given with --csv-file
        csv_folder: path given with --csv-folder
        table: table given with --table
        table_prefix: prefix explicitly given with --table-prefix (None if not given)
        add_id: whether --add-id is set
        run_id: run id given with --run-id

    Raises:
        CsvIngestionOptionError: if the combination is invalid
    """
    if (csv_file is None) == (csv_folder is None):
        raise CsvIngestionOptionError(T("coal.services.native_csv_ingestion.option_input_required"))
    if table is not None and csv_folder is not None:
        raise CsvIngestionOptionError(T("coal.services.native_csv_ingestion.option_table_with_folder"))
    if table is not None and table_prefix is not None:
        raise CsvIngestionOptionError(T("coal.services.native_csv_ingestion.option_table_with_prefix"))
    if add_id and not run_id:
        raise CsvIngestionOptionError(T("coal.services.native_csv_ingestion.option_run_id_required"))


def read_header(csv_path: Path) -> list[str]:
    """
    Read the header line of a CSV file, stripping a BOM.

    Args:
        csv_path: CSV file

    Returns:
        The column names, an empty list for an empty file
    """
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        return next(csv.reader(f), [])


def resolve_table_name(csv_path: Path, table: Optional[str] = None, table_prefix: Optional[str] = None) -> str:
    """
    Get the table to load a file into.

    Args:
        csv_path: CSV file
        table: table name, used verbatim if given
        table_prefix: prefix (lowercased) added to the file stem when no table is given

    Returns:
        The table name
    """
    if table is not None:
        return table
    return f"{(table_prefix or '').lower()}{Path(csv_path).stem}"


def check_csv_file(csv_path: Path) -> None:
    """
    Check that a CSV file exists and is not empty.

    Raises:
        CsvIngestionError: if it is not a file or is empty
    """
    if not Path(csv_path).is_file():
        raise CsvIngestionError(T("coal.services.native_csv_ingestion.file_not_found").format(path=csv_path))
    if Path(csv_path).stat().st_size == 0:
        raise CsvIngestionError(T("coal.services.native_csv_ingestion.file_empty").format(path=csv_path))


def list_csv_files(folder: Path) -> list[Path]:
    """
    List the ``*.csv`` files directly in a folder (not recursive), sorted.

    Raises:
        CsvIngestionError: if the folder does not exist or contains no CSV file
    """
    folder = Path(folder)
    if not folder.is_dir():
        raise CsvIngestionError(T("coal.services.native_csv_ingestion.folder_not_found").format(path=folder))
    files = sorted(p for p in folder.glob("*.csv") if p.is_file())
    if not files:
        raise CsvIngestionError(T("coal.services.native_csv_ingestion.folder_no_csv").format(path=folder))
    return files


def build_copy_statement(table: str, columns: list[str], schema: str):
    """
    Build the COPY statement, every name being quoted as an identifier (case-sensitive).

    Args:
        table: target table
        columns: column names, in the order of the CSV
        schema: schema of the table

    Returns:
        A psycopg ``sql.Composed`` statement
    """
    _, sql = import_psycopg()
    return sql.SQL("COPY {table} ({columns}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(
        table=sql.Identifier(schema, table),
        columns=sql.SQL(", ").join(sql.Identifier(c) for c in columns),
    )


def _check_header(csv_path: Path, columns: list[str]) -> None:
    if not columns or columns == [""]:
        raise CsvIngestionError(T("coal.services.native_csv_ingestion.header_empty").format(path=csv_path))
    if "" in columns:
        raise CsvIngestionError(T("coal.services.native_csv_ingestion.header_empty_column").format(path=csv_path))
    duplicates = sorted({c for c in columns if columns.count(c) > 1})
    if duplicates:
        raise CsvIngestionError(
            T("coal.services.native_csv_ingestion.header_duplicate_columns").format(
                path=csv_path, columns=", ".join(duplicates)
            )
        )


def preflight(conn, csv_path: Path, table: str, add_id: bool, schema: str) -> list[str]:
    """
    Minimal checks, done on an open connection before any byte of the file is sent.

    The table is looked up in the catalog with plain strings (not ``to_regclass``), so that a mixed-case
    table name is matched exactly, as the COPY target is.

    Args:
        conn: open psycopg connection
        csv_path: CSV file
        table: target table
        add_id: whether csm_run_id is filled from the run id
        schema: schema of the table

    Returns:
        The header columns of the file

    Raises:
        CsvIngestionError: if a check fails
    """
    columns = read_header(csv_path)
    _check_header(csv_path, columns)

    row = conn.execute(
        "SELECT c.relkind FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relname = %s",
        (schema, table),
    ).fetchone()
    if row is None:
        raise CsvIngestionError(
            T("coal.services.native_csv_ingestion.table_not_found").format(schema=schema, table=table)
        )
    if row[0] not in ("r", "p"):
        raise CsvIngestionError(
            T("coal.services.native_csv_ingestion.table_wrong_kind").format(schema=schema, table=table)
        )

    table_columns = dict(
        conn.execute(
            "SELECT a.attname, pg_catalog.pg_get_expr(d.adbin, d.adrelid) "
            "FROM pg_catalog.pg_attribute a "
            "JOIN pg_catalog.pg_class c ON c.oid = a.attrelid "
            "JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
            "WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped",
            (schema, table),
        ).fetchall()
    )

    unknown = [c for c in columns if c not in table_columns]
    if unknown:
        raise CsvIngestionError(
            T("coal.services.native_csv_ingestion.unknown_columns").format(
                path=csv_path, schema=schema, table=table, columns=", ".join(unknown)
            )
        )

    if add_id:
        if RUN_ID_COLUMN in columns:
            raise CsvIngestionError(T("coal.services.native_csv_ingestion.run_id_in_header").format(path=csv_path))
        if RUN_ID_COLUMN not in table_columns:
            raise CsvIngestionError(
                T("coal.services.native_csv_ingestion.run_id_column_missing").format(schema=schema, table=table)
            )
        default = table_columns[RUN_ID_COLUMN] or ""
        if "current_setting" not in default or RUN_ID_SETTING not in default:
            raise CsvIngestionError(
                T("coal.services.native_csv_ingestion.run_id_default_missing").format(schema=schema, table=table)
            )

    LOGGER.debug(
        T("coal.services.native_csv_ingestion.preflight_ok").format(
            path=csv_path, schema=schema, table=table, columns=len(columns)
        )
    )
    return columns


def _copy_file(cur, csv_path: Path, copy_stmt, total_bytes: int) -> None:
    """
    Stream a file to PostgreSQL by chunks, logging the progress.

    Args:
        cur: psycopg cursor
        csv_path: CSV file
        copy_stmt: COPY ... FROM STDIN statement
        total_bytes: size of the file
    """
    sent = 0
    next_log = LOG_EVERY_BYTES
    with cur.copy(copy_stmt) as copy, open(csv_path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            copy.write(chunk)
            sent += len(chunk)
            if sent >= next_log:
                LOGGER.info(
                    T("coal.services.native_csv_ingestion.progress").format(
                        sent_mb=sent / 1e6, total_mb=total_bytes / 1e6
                    )
                )
                next_log += LOG_EVERY_BYTES


def _copy_in_transaction(
    conn,
    csv_path: Path,
    schema: str,
    table: str,
    columns: list[str],
    run_id: Optional[str],
) -> int:
    """
    Load a file in the current transaction of the connection, then commit.

    Returns:
        The number of rows copied, as reported by COPY

    Raises:
        CsvIngestionError: if PostgreSQL fails, the transaction is not committed
    """
    psycopg, _ = import_psycopg()
    copy_stmt = build_copy_statement(table, columns, schema)
    total_bytes = csv_path.stat().st_size
    LOGGER.info(
        T("coal.services.native_csv_ingestion.loading").format(
            path=csv_path, size_mb=total_bytes / 1e6, schema=schema, table=table
        )
    )

    try:
        with conn.cursor() as cur:
            if run_id is not None:
                cur.execute("SELECT set_config(%s, %s, true)", (RUN_ID_SETTING, run_id))
            start = time.perf_counter()
            _copy_file(cur, csv_path, copy_stmt, total_bytes)
            seconds = max(time.perf_counter() - start, 1e-9)
            rows = cur.rowcount
        LOGGER.info(
            T("coal.services.native_csv_ingestion.copy_finished").format(
                rows=rows, size_mb=total_bytes / 1e6, seconds=seconds, rate=total_bytes / 1e6 / seconds
            )
        )
        conn.commit()
        LOGGER.info(T("coal.services.native_csv_ingestion.committed").format(rows=rows, schema=schema, table=table))
    except psycopg.Error as err:
        raise CsvIngestionError(
            T("coal.services.native_csv_ingestion.load_failed").format(path=csv_path.name, error=err)
        ) from err

    if rows == 0:
        LOGGER.warning(
            T("coal.services.native_csv_ingestion.no_rows").format(path=csv_path, schema=schema, table=table)
        )
    return rows


def ingest_csv_file(
    csv_path: Path,
    configuration: Configuration,
    table: Optional[str] = None,
    table_prefix: str = DEFAULT_TABLE_PREFIX,
    run_id: Optional[str] = None,
) -> int:
    """
    Load one CSV file into an existing table, in one transaction.

    Args:
        csv_path: CSV file, with a header line
        configuration: coal Configuration with a postgres section
        table: target table, used verbatim; default is the lowercased table_prefix + file stem
        table_prefix: prefix used when no table is given
        run_id: when set (--add-id), the csm_run_id column is filled with it through the transaction-local setting

    Returns:
        The number of rows copied
    """
    csv_path = Path(csv_path)
    check_csv_file(csv_path)
    table = resolve_table_name(csv_path, table, table_prefix)
    schema = PostgresUtils(configuration).db_schema

    with connect(configuration) as conn:
        columns = preflight(conn, csv_path, table, run_id is not None, schema)
        return _copy_in_transaction(conn, csv_path, schema, table, columns, run_id)


def _log_folder_failure(tables: list[str], loaded: list[str], failed: str) -> None:
    if loaded:
        LOGGER.error(T("coal.services.native_csv_ingestion.folder_loaded").format(tables=", ".join(loaded)))
    LOGGER.error(T("coal.services.native_csv_ingestion.folder_failed").format(table=failed))
    not_started = tables[tables.index(failed) + 1 :]
    if not_started:
        LOGGER.error(T("coal.services.native_csv_ingestion.folder_not_started").format(tables=", ".join(not_started)))


def ingest_csv_folder(
    folder: Path,
    configuration: Configuration,
    table_prefix: str = DEFAULT_TABLE_PREFIX,
    run_id: Optional[str] = None,
) -> dict[str, int]:
    """
    Load every ``*.csv`` file of a folder into its own table (lowercased table_prefix + file stem).

    All files are checked before the first load. Then files are loaded one after the other, one connection and
    one transaction each: if a load fails, the tables already loaded stay committed.

    Args:
        folder: folder containing the CSV files (not recursive)
        configuration: coal Configuration with a postgres section
        table_prefix: prefix of the table names
        run_id: when set (--add-id), the csm_run_id column is filled with it

    Returns:
        The number of rows copied per table
    """
    files = list_csv_files(folder)
    for csv_path in files:
        check_csv_file(csv_path)
    targets = [(csv_path, resolve_table_name(csv_path, None, table_prefix)) for csv_path in files]
    schema = PostgresUtils(configuration).db_schema

    with connect(configuration) as conn:
        plan = [
            (csv_path, table, preflight(conn, csv_path, table, run_id is not None, schema))
            for csv_path, table in targets
        ]

    tables = [table for _, table, _ in plan]
    results: dict[str, int] = {}
    for csv_path, table, columns in plan:
        try:
            with connect(configuration) as conn:
                results[table] = _copy_in_transaction(conn, csv_path, schema, table, columns, run_id)
        except Exception:
            _log_folder_failure(tables, list(results), table)
            raise
    return results
