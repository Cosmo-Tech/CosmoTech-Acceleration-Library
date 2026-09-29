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
        """List workspace files whose name starts with the given prefix.

        Args:
            organization_id: The ID of the organization
            workspace_id: The ID of the workspace
            file_prefix: The prefix to filter workspace file names by

        Returns:
            List of matching workspace file names

        Raises:
            ValueError: If no workspace file matches the given prefix
        """
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
        """Download a single workspace file to a local directory.

        Args:
            organization_id: The ID of the organization
            workspace_id: The ID of the workspace
            file_name: The name of the workspace file to download
            target_dir: The local directory to download the file into

        Returns:
            The local path of the downloaded file

        Raises:
            ValueError: If target_dir is not a directory
        """
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
        """Upload a local file to a workspace.

        Args:
            organization_id: The ID of the organization
            workspace_id: The ID of the workspace
            file_path: Local path of the file to upload
            workspace_path: Destination path (or directory ending with '/') in the workspace
            overwrite: If True, overwrite an existing file at the destination

        Returns:
            The name of the uploaded workspace file

        Raises:
            ValueError: If file_path does not exist or is not a single file
            ApiException: If the API call fails, e.g. because the file already exists
        """
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
