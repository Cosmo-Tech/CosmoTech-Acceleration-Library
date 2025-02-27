# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Parameter handling functions.

This module is deprecated and will be removed in a future version.
Please use the cosmotech.coal.cosmotech_api.runner.parameters module instead.
"""

import warnings
from typing import Dict, List, Any

from cosmotech.coal.cosmotech_api.runner.parameters import (
    get_runner_parameters,
    format_parameters_list,
    write_parameters,
    write_parameters_to_json,
    write_parameters_to_csv
)


def get_scenario_parameters(scenario_data) -> Dict[str, Any]:
    """
    Extract parameters from scenario data.
    
    This function is deprecated and will be removed in a future version.
    Please use get_runner_parameters from cosmotech.coal.cosmotech_api.runner.parameters instead.
    
    Args:
        scenario_data: Scenario data object
        
    Returns:
        Dictionary mapping parameter IDs to values
    """
    warnings.warn(
        "get_scenario_parameters is deprecated and will be removed in a future version. "
        "Please use get_runner_parameters from cosmotech.coal.cosmotech_api.runner.parameters instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return get_runner_parameters(scenario_data)
