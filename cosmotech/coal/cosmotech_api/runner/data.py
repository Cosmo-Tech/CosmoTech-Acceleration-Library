# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Core runner data retrieval functions.
"""

from cosmotech_api.api.runner_api import RunnerApi
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def get_runner_data(organization_id: str, workspace_id: str, runner_id: str):
    """
    Get runner data from the API.

    Args:
        organization_id: The ID of the organization
        workspace_id: The ID of the workspace
        runner_id: The ID of the runner

    Returns:
        Runner data object
    """
    LOGGER.info(T("coal.cosmotech_api.runner.loading_data"))
    with get_api_client()[0] as api_client:
        api_instance = RunnerApi(api_client)
        runner_data = api_instance.get_runner(
            organization_id=organization_id,
            workspace_id=workspace_id,
            runner_id=runner_id,
        )
    return runner_data
