# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.orchestrator.utils.translate import T
from cosmotech_api import SolutionApi as BaseSolutionApi

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION, Configuration
from cosmotech.coal.utils.logger import LOGGER


class SolutionApi(BaseSolutionApi, Connection):
    def __init__(
        self,
        configuration: Configuration = ENVIRONMENT_CONFIGURATION,
    ):
        Connection.__init__(self, configuration)
        BaseSolutionApi.__init__(self, self.api_client)

        LOGGER.debug(T("coal.cosmotech_api.initialization.solution_api_initialized"))
