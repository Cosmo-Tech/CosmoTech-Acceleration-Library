# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Dataset handling module.

This module is deprecated and will be removed in a future version.
Please use the cosmotech.coal.cosmotech_api.dataset module instead.
"""

import warnings

warnings.warn(
    "The cosmotech.coal.dataset module is deprecated and will be removed in a future version. "
    "Please use the cosmotech.coal.cosmotech_api.dataset module instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export all functions from the new module
from cosmotech.coal.cosmotech_api.dataset import (
    download_adt_dataset,
    download_twingraph_dataset,
    download_legacy_twingraph_dataset,
    download_file_dataset,
    download_dataset_by_id,
    convert_dataset_to_files,
    convert_graph_dataset_to_files,
    convert_file_dataset_to_files,
    get_content_from_twin_graph_data,
    sheet_to_header,
)
