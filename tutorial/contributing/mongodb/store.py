# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
MongoDB store operations module.

This module provides functions for interacting with MongoDB databases
for store operations.
"""

from time import perf_counter

import pyarrow
import pymongo
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.logger import LOGGER


def send_pyarrow_table_to_mongodb(
    data: pyarrow.Table,
    collection_name: str,
    mongodb_uri: str,
    mongodb_db: str,
    replace: bool = True,
) -> int:
    """
    Send a PyArrow table to MongoDB.

    Args:
        data: PyArrow table to send
        collection_name: MongoDB collection name
        mongodb_uri: MongoDB connection URI
        mongodb_db: MongoDB database name
        replace: Whether to replace existing collection

    Returns:
        Number of documents inserted
    """
    # Convert PyArrow table to list of dictionaries
    records = data.to_pylist()

    # Connect to MongoDB
    client = pymongo.MongoClient(mongodb_uri)
    db = client[mongodb_db]

    # Drop collection if replace is True and collection exists
    if replace and collection_name in db.list_collection_names():
        db[collection_name].drop()

    # Insert records
    if records:
        result = db[collection_name].insert_many(records)
        return len(result.inserted_ids)

    return 0


def dump_store_to_mongodb(
    store_folder: str,
    mongodb_uri: str,
    mongodb_db: str,
    collection_prefix: str = "Cosmotech_",
    replace: bool = True,
) -> None:
    """
    Dump Store data to a MongoDB database.

    Args:
        store_folder: Folder containing the Store
        mongodb_uri: MongoDB connection URI
        mongodb_db: MongoDB database name
        collection_prefix: Collection prefix
        replace: Whether to replace existing collections
    """
    _s = Store(store_location=store_folder)

    tables = list(_s.list_tables())
    if len(tables):
        LOGGER.info(T("coal.logs.database.sending_data").format(table=mongodb_db))
        total_rows = 0
        _process_start = perf_counter()
        for table_name in tables:
            _s_time = perf_counter()
            target_collection_name = f"{collection_prefix}{table_name}"
            LOGGER.info(T("coal.logs.database.table_entry").format(table=target_collection_name))
            data = _s.get_table(table_name)
            if not len(data):
                LOGGER.info(T("coal.logs.database.no_rows"))
                continue
            _dl_time = perf_counter()
            rows = send_pyarrow_table_to_mongodb(
                data,
                target_collection_name,
                mongodb_uri,
                mongodb_db,
                replace,
            )
            total_rows += rows
            _up_time = perf_counter()
            LOGGER.info(T("coal.logs.database.row_count").format(count=rows))
            LOGGER.debug(
                T("coal.logs.progress.operation_timing").format(
                    operation="Load from datastore", time=f"{_dl_time - _s_time:0.3}"
                )
            )
            LOGGER.debug(
                T("coal.logs.progress.operation_timing").format(
                    operation="Send to MongoDB", time=f"{_up_time - _dl_time:0.3}"
                )
            )
        _process_end = perf_counter()
        LOGGER.info(
            T("coal.logs.database.rows_fetched").format(
                table="all tables",
                count=total_rows,
                time=f"{_process_end - _process_start:0.3}",
            )
        )
    else:
        LOGGER.info(T("coal.logs.database.store_empty"))
