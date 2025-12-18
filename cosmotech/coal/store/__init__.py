# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Store module.

This module provides functions for working with the Store,
including loading and converting data.
"""

# Re-export functions from the csv module
from cosmotech.coal.store.csv import (
    convert_store_table_to_csv,
    store_csv_file,
)

# Re-export functions from the native_python module
from cosmotech.coal.store.native_python import (
    convert_table_as_pylist,
    store_pylist,
)
from cosmotech.coal.store.pandas import (
    convert_store_table_to_dataframe as convert_store_table_to_pandas_dataframe,
)
from cosmotech.coal.store.pandas import (
    store_dataframe,
)
from cosmotech.coal.store.pyarrow import (
    convert_store_table_to_dataframe as convert_store_table_to_pyarrow_table,
)
from cosmotech.coal.store.pyarrow import (
    store_table,
)

# Re-export the Store class
from cosmotech.coal.store.store import Store

# Re-export functions from the pandas module (if available)


# Re-export functions from the pyarrow module (if available)
