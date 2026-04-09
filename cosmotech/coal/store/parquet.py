# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pathlib

import pyarrow.parquet as pq

from cosmotech.coal.store.store import Store


def store_parquet_file(
    table_name: str,
    parquet_path: pathlib.Path,
    replace_existsing_file: bool = False,
    store=Store(),
):
    if not parquet_path.exists():
        raise FileNotFoundError(f"File {parquet_path} does not exists")

    data = pq.read_table(parquet_path)
    _c = data.column_names
    data = data.rename_columns([Store.sanitize_column(_column) for _column in _c])

    store.add_table(table_name=table_name, data=data, replace=replace_existsing_file)
