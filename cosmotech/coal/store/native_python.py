# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pyarrow as pa

from cosmotech.coal.store.store import Store


def store_pylist(
    table_name: str,
    data: list[dict],
    replace_existsing_file: bool = False,
    store=Store(),
):
    data = pa.Table.from_pylist(data)

    store.add_table(table_name=table_name, data=data, replace=replace_existsing_file)


def convert_table_as_pylist(table_name: str, store=Store()):
    return store.get_table(table_name).to_pylist()
