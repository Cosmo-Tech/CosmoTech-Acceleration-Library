# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Cosmotech API integration module.

This module provides functions for interacting with the Cosmotech API.
"""

# Re-export functions from the parameters module
from cosmotech.coal.cosmotech_api.parameters import (
    write_parameters,
)

# Re-export functions from the twin_data_layer module
from cosmotech.coal.cosmotech_api.twin_data_layer import (
    get_dataset_id_from_runner,
    send_files_to_tdl,
    load_files_from_tdl,
)

# Re-export functions from the run_data module
from cosmotech.coal.cosmotech_api.run_data import (
    send_csv_to_run_data,
    send_store_to_run_data,
    load_csv_from_run_data,
)

# Re-export functions from the run_template module
from cosmotech.coal.cosmotech_api.run_template import (
    load_run_template_handlers,
)
