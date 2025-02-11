# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from typing import Any
from typing import Optional

import cosmotech_api


def get_run_metadata(
    api_client: cosmotech_api.api_client.ApiClient,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    run_id: str,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> dict[str, Any]:
    run_api = cosmotech_api.RunApi(api_client)

    run: cosmotech_api.Run = run_api.get_run(organization_id,
                                             workspace_id,
                                             runner_id,
                                             run_id)
    return run.model_dump(by_alias=True, exclude_none=True, include=include, exclude=exclude, mode='json')
