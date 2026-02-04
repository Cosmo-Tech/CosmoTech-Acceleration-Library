# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from typing import Any, Optional

from cosmotech.orchestrator.utils.translate import T
from cosmotech_api import RunnerApi as BaseRunnerApi

from cosmotech.coal.cosmotech_api.apis.dataset import DatasetApi
from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.cosmotech_api.objects.parameters import Parameters
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION, Configuration
from cosmotech.coal.utils.logger import LOGGER


class RunnerApi(BaseRunnerApi, Connection):

    def __init__(
        self,
        configuration: Configuration = ENVIRONMENT_CONFIGURATION,
    ):
        Connection.__init__(self, configuration)
        BaseRunnerApi.__init__(self, self.api_client)

        LOGGER.debug(T("coal.cosmotech_api.initialization.runner_api_initialized"))

    def get_runner_metadata(
        self,
        runner_id=None,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        runner = self.get_runner(
            self.configuration.cosmotech.organization_id,
            self.configuration.cosmotech.workspace_id,
            runner_id or self.configuration.cosmotech.runner_id,
        )

        return runner.model_dump(by_alias=True, exclude_none=True, include=include, exclude=exclude, mode="json")

    def download_runner_data(
        self,
        download_datasets: Optional[str] = None,
    ):
        LOGGER.info(T("coal.cosmotech_api.runner.starting_download"))

        # Get runner data
        runner = self.get_runner(
            self.configuration.cosmotech.organization_id,
            self.configuration.cosmotech.workspace_id,
            self.configuration.cosmotech.runner_id,
        )

        # Skip if no parameters found
        if not runner.parameters_values:
            LOGGER.warning(T("coal.cosmotech_api.runner.no_parameters"))
        else:
            LOGGER.info(T("coal.cosmotech_api.runner.loaded_data"))
            parameters = Parameters(runner)
            parameters.write_parameters_to_json(self.configuration.cosmotech.parameters_absolute_path)

        if runner.datasets.parameter:
            ds_api = DatasetApi(self.configuration)
            ds_api.download_parameter(runner.datasets.parameter)

        # Download datasets if requested
        if download_datasets:
            LOGGER.info(T("coal.cosmotech_api.runner.downloading_datasets").format(count=len(runner.datasets.bases)))
            if runner.datasets.bases:
                ds_api = DatasetApi(self.configuration)
                for dataset_id in runner.datasets.bases:
                    ds_api.download_dataset(dataset_id)
