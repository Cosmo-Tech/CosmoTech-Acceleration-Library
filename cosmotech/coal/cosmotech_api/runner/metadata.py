# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Runner metadata retrieval functions.
"""

from typing import Any, Optional

import cosmotech_api


def get_runner_metadata(
    api_client: cosmotech_api.api_client.ApiClient,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> dict[str, Any]:
    """
    Get runner metadata from the API.

    Args:
        api_client: The API client to use
        organization_id: The ID of the organization
        workspace_id: The ID of the workspace
        runner_id: The ID of the runner
        include: Optional list of fields to include
        exclude: Optional list of fields to exclude

    Returns:
        Dictionary with runner metadata
    """
    runner_api = cosmotech_api.RunnerApi(api_client)
    runner: cosmotech_api.Runner = runner_api.get_runner(organization_id, workspace_id, runner_id)

    return runner.model_dump(by_alias=True, exclude_none=True, include=include, exclude=exclude, mode="json")
