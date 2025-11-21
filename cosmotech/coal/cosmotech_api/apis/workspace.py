# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from pathlib import Path

from cosmotech.orchestrator.utils.translate import T
from cosmotech_api import ApiException
from cosmotech_api import WorkspaceApi as BaseWorkspaceApi

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION, Configuration
from cosmotech.coal.utils.logger import LOGGER


class WorkspaceApi(BaseWorkspaceApi, Connection):

    def __init__(
        self,
        configuration: Configuration = ENVIRONMENT_CONFIGURATION,
    ):
        Connection.__init__(self, configuration)
        BaseWorkspaceApi.__init__(self, self.api_client)

        LOGGER.debug(T("coal.cosmotech_api.initialization.workspace_api_initialized"))

    def list_filtered_workspace_files(
        self,
        organization_id: str,
        workspace_id: str,
        file_prefix: str,
    ) -> list[str]:
        target_list = []
        LOGGER.info(T("coal.cosmotech_api.workspace.target_is_folder"))
        wsf = self.list_workspace_files(organization_id, workspace_id)
        for workspace_file in wsf:
            if workspace_file.file_name.startswith(file_prefix):
                target_list.append(workspace_file.file_name)

        if not target_list:
            LOGGER.error(
                T("coal.common.errors.data_no_workspace_files").format(
                    file_prefix=file_prefix, workspace_id=workspace_id
                )
            )
            raise ValueError(
                T("coal.common.errors.data_no_workspace_files").format(
                    file_prefix=file_prefix, workspace_id=workspace_id
                )
            )

        return target_list

    def download_workspace_file(
        self,
        organization_id: str,
        workspace_id: str,
        file_name: str,
        target_dir: Path,
    ) -> Path:
        if target_dir.is_file():
            raise ValueError(T("coal.common.file_operations.not_directory").format(target_dir=target_dir))

        LOGGER.info(T("coal.cosmotech_api.workspace.loading_file").format(file_name=file_name))

        _file_content = self.get_workspace_file(organization_id, workspace_id, file_name)

        local_target_file = target_dir / file_name
        local_target_file.parent.mkdir(parents=True, exist_ok=True)

        with open(local_target_file, "wb") as _file:
            _file.write(_file_content)

        LOGGER.info(T("coal.cosmotech_api.workspace.file_loaded").format(file=local_target_file))

        return local_target_file

    def upload_workspace_file(
        self,
        organization_id: str,
        workspace_id: str,
        file_path: str,
        workspace_path: str,
        overwrite: bool = True,
    ) -> str:
        target_file = Path(file_path)
        if not target_file.exists():
            LOGGER.error(T("coal.common.file_operations.not_exists").format(file_path=file_path))
            raise ValueError(T("coal.common.file_operations.not_exists").format(file_path=file_path))
        if not target_file.is_file():
            LOGGER.error(T("coal.common.file_operations.not_single_file").format(file_path=file_path))
            raise ValueError(T("coal.common.file_operations.not_single_file").format(file_path=file_path))

        destination = workspace_path + target_file.name if workspace_path.endswith("/") else workspace_path

        LOGGER.info(T("coal.cosmotech_api.workspace.sending_to_api").format(destination=destination))
        try:
            _file = self.create_workspace_file(
                organization_id, workspace_id, file_path, overwrite, destination=destination
            )
        except ApiException as e:
            LOGGER.error(T("coal.common.file_operations.already_exists").format(csv_path=destination))
            raise e

        LOGGER.info(T("coal.cosmotech_api.workspace.file_sent").format(file=_file.file_name))
        return _file.file_name
