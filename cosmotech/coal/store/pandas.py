# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pyarrow

from cosmotech.coal.store.store import Store
import pandas as pd


def store_dataframe(
    table_name: str,
    dataframe: pd.DataFrame,
    replace_existsing_file: bool = False,
    store=Store(),
):
    data = pyarrow.Table.from_pandas(dataframe)

    store.add_table(table_name=table_name, data=data, replace=replace_existsing_file)


def convert_store_table_to_dataframe(table_name: str, store=Store()) -> pd.DataFrame:
    return store.get_table(table_name).to_pandas()
