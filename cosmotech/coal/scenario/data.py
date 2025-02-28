# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Core scenario data retrieval functions.

This module is deprecated and will be removed in a future version.
Please use the cosmotech.coal.cosmotech_api.runner.data module instead.
"""

import warnings
from cosmotech.coal.cosmotech_api.runner.data import get_runner_data


def get_scenario_data(organization_id: str, workspace_id: str, scenario_id: str):
    """
    Get scenario data from the API.

    This function is deprecated and will be removed in a future version.
    Please use get_runner_data from cosmotech.coal.cosmotech_api.runner.data instead.

    Args:
        organization_id: The ID of the organization
        workspace_id: The ID of the workspace
        scenario_id: The ID of the scenario

    Returns:
        Scenario data object
    """
    warnings.warn(
        "get_scenario_data is deprecated and will be removed in a future version. "
        "Please use get_runner_data from cosmotech.coal.cosmotech_api.runner.data instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    return get_runner_data(organization_id, workspace_id, scenario_id)
