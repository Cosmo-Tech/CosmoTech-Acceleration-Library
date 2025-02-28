# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Scenario and Runner data handling module.

This module is deprecated and will be removed in a future version.
Please use the cosmotech.coal.cosmotech_api.runner module instead.
"""

import warnings

warnings.warn(
    "The cosmotech.coal.scenario module is deprecated and will be removed in a future version. "
    "Please use the cosmotech.coal.cosmotech_api.runner module instead.",
    DeprecationWarning,
    stacklevel=2,
)

# Re-export functions from the runner module
from cosmotech.coal.cosmotech_api.runner.data import get_runner_data
from cosmotech.coal.cosmotech_api.runner.parameters import (
    get_runner_parameters,
    format_parameters_list,
    write_parameters,
    write_parameters_to_json,
    write_parameters_to_csv,
)
from cosmotech.coal.cosmotech_api.runner.datasets import (
    get_dataset_ids_from_runner,
    download_dataset,
    download_datasets,
    dataset_to_file,
)
from cosmotech.coal.cosmotech_api.runner.download import (
    download_run_data,
    download_runner_data,
)
