# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
SingleStore store operations module.

This module provides functions for interacting with SingleStore databases
for store operations.
"""

import pathlib
import time
import csv
import singlestoredb as s2

from cosmotech.coal.store.csv import store_csv_file
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def _get_data(table_name: str, output_directory: str, cursor) -> None:
    """
    Run a SQL query to fetch all data from a table and write them in csv files.

    Args:
        table_name: Table name
        output_directory: Output directory
        cursor: SingleStore cursor
    """
    start_time = time.perf_counter()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    end_time = time.perf_counter()
    LOGGER.info(
        T("coal.services.database.rows_fetched").format(
            table=table_name, count=len(rows), time=round(end_time - start_time, 2)
        )
    )
    with open(f"{output_directory}/{table_name}.csv", "w", newline="") as csv_stock:
        w = csv.DictWriter(csv_stock, rows[0].keys())
        w.writeheader()
        w.writerows(rows)


def load_from_singlestore(
    single_store_host: str,
    single_store_port: int,
    single_store_db: str,
    single_store_user: str,
    single_store_password: str,
    store_folder: str,
    single_store_tables: str = "",
) -> None:
    """
    Load data from SingleStore and store it in the Store.

    Args:
        single_store_host: SingleStore host
        single_store_port: SingleStore port
        single_store_db: SingleStore database name
        single_store_user: SingleStore username
        single_store_password: SingleStore password
        store_folder: Store folder
        single_store_tables: Comma-separated list of tables to load
    """
    single_store_working_dir = store_folder + "/singlestore"
    if not pathlib.Path.exists(single_store_working_dir):
        pathlib.Path.mkdir(single_store_working_dir)

    start_full = time.perf_counter()

    conn = s2.connect(
        host=single_store_host,
        port=single_store_port,
        database=single_store_db,
        user=single_store_user,
        password=single_store_password,
        results_type="dicts",
    )
    with conn:
        with conn.cursor() as cur:
            if single_store_tables == "":
                cur.execute("SHOW TABLES")
                table_names = cur.fetchall()
            else:
                table_names = single_store_tables.split(",")
            LOGGER.info(T("coal.services.database.tables_to_fetch").format(tables=table_names))
            for name in table_names:
                _get_data(name, single_store_working_dir, cur)
    end_full = time.perf_counter()
    LOGGER.info(T("coal.services.database.full_dataset").format(time=round(end_full - start_full, 2)))

    for csv_path in pathlib.Path(single_store_working_dir).glob("*.csv"):
        LOGGER.info(T("coal.services.azure_storage.found_file").format(file=csv_path.name))
        store_csv_file(csv_path.name[:-4], csv_path, store=Store(False, store_folder))
