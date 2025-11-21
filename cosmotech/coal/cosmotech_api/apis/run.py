# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from typing import Any, Optional

from cosmotech.orchestrator.utils.translate import T
from cosmotech_api import RunApi as BaseRunApi

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION, Configuration
from cosmotech.coal.utils.logger import LOGGER


class RunApi(BaseRunApi, Connection):

    def __init__(
        self,
        configuration: Configuration = ENVIRONMENT_CONFIGURATION,
    ):
        Connection.__init__(self, configuration)
        BaseRunApi.__init__(self, self.api_client)

        LOGGER.debug(T("coal.cosmotech_api.initialization.run_api_initialized"))

    def get_run_metadata(
        self,
        organization_id: str,
        workspace_id: str,
        runner_id: str,
        run_id: str,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        run = self.get_run(organization_id, workspace_id, runner_id, run_id)
        return run.model_dump(by_alias=True, exclude_none=True, include=include, exclude=exclude, mode="json")
