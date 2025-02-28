# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.

"""
Cosmotech API integration module.

This module provides functions for interacting with the Cosmotech API.
"""

# Re-export functions from the orchestrator module
from cosmotech.coal.cosmotech_api.orchestrator import (
    generate_from_solution,
    generate_from_template,
)

# Re-export functions from the parameters module
from cosmotech.coal.cosmotech_api.parameters import (
    write_parameters,
    generate_parameters,
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
