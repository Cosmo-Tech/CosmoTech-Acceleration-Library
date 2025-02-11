# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from typing import Any
from typing import Optional

import cosmotech_api


def get_runner_metadata(
    api_client: cosmotech_api.api_client.ApiClient,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> dict[str, Any]:
    runner_api = cosmotech_api.RunnerApi(api_client)
    runner: cosmotech_api.Runner = runner_api.get_runner(organization_id,
                                                         workspace_id,
                                                         runner_id)

    return runner.model_dump(by_alias=True, exclude_none=True, include=include, exclude=exclude, mode='json')
