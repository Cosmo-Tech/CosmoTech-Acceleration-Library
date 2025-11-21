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
        organization_id: str,
        workspace_id: str,
        runner_id: str,
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> dict[str, Any]:
        runner = self.get_runner(organization_id, workspace_id, runner_id)

        return runner.model_dump(by_alias=True, exclude_none=True, include=include, exclude=exclude, mode="json")

    def download_runner_data(
        self,
        organization_id: str,
        workspace_id: str,
        runner_id: str,
        parameter_folder: str,
        dataset_folder: Optional[str] = None,
    ):
        LOGGER.info(T("coal.cosmotech_api.runner.starting_download"))

        # Get runner data
        runner_data = self.get_runner(organization_id, workspace_id, runner_id)

        # Skip if no parameters found
        if not runner_data.parameters_values:
            LOGGER.warning(T("coal.cosmotech_api.runner.no_parameters"))
        else:
            LOGGER.info(T("coal.cosmotech_api.runner.loaded_data"))
            parameters = Parameters(runner_data)
            parameters.write_parameters_to_json(parameter_folder)

        # Download datasets if requested
        if dataset_folder:
            datasets_ids = runner_data.datasets.bases

            if datasets_ids:
                LOGGER.info(T("coal.cosmotech_api.runner.downloading_datasets").format(count=len(datasets_ids)))
                ds_api = DatasetApi(self.configuration)
                for dataset_id in datasets_ids:
                    ds_api.download_dataset(dataset_id)
