# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import pathlib

import cosmotech_api

from cosmotech.coal.utils.logger import LOGGER


def list_workspace_files(
    api_client: cosmotech_api.api_client.ApiClient,
    organization_id: str,
    workspace_id: str,
    file_prefix: str
) -> list[str]:
    """
    Helper function to list all workspace files using a pre-given file prefix
    :param api_client: An api client used to connect to the Cosmo Tech API
    :param organization_id: An ID of an Organization in the Cosmo Tech API
    :param workspace_id: An ID of a Workspace in the Cosmo Tech API
    :param file_prefix: The prefix of the files to find in the Workspace
    :return: A list of existing files inside the workspace
    """
    target_list = []
    api_ws = cosmotech_api.api.workspace_api.WorkspaceApi(api_client)
    LOGGER.info(f"Target path is a folder, listing content")
    wsf = api_ws.find_all_workspace_files(organization_id,
                                          workspace_id)
    for workspace_file in wsf:
        if workspace_file.file_name.startswith(file_prefix):
            target_list.append(workspace_file.file_name)

    if not target_list:
        LOGGER.error(f"No workspace file were found with filter [bold green]{file_prefix}[/]")
        raise ValueError(f"No workspace file were found with filter {file_prefix} in workspace {workspace_id}")

    return target_list


def download_workspace_file(
    api_client: cosmotech_api.api_client.ApiClient,
    organization_id: str,
    workspace_id: str,
    file_name: str,
    target_dir: pathlib.Path
) -> pathlib.Path:
    """
    Downloads a given file from a workspace to a given directory
    If the file is inside a directory in the workspace, sub-directories will be created.
    :param api_client: An api client used to connect to the Cosmo Tech API
    :param organization_id: An ID of an Organization in the Cosmo Tech API
    :param workspace_id: An ID of a Workspace in the Cosmo Tech API
    :param file_name: The file to download to the workspace
    :param target_dir: The directory in which to write the file
    :return: The path to the created file
    """
    if target_dir.is_file():
        raise ValueError(f"{target_dir} is a file and not a directory")
    api_ws = cosmotech_api.api.workspace_api.WorkspaceApi(api_client)

    LOGGER.info(f"Loading [bold green]{file_name}[/] from the API")

    _file_content = api_ws.download_workspace_file(organization_id,
                                                   workspace_id,
                                                   file_name)

    local_target_file = target_dir / file_name
    local_target_file.parent.mkdir(parents=True, exist_ok=True)

    with open(local_target_file, "wb") as _file:
        _file.write(_file_content)

    LOGGER.info(f"[bold green]{local_target_file}[/] successfuly loaded from the API")

    return local_target_file


def upload_workspace_file(
    api_client: cosmotech_api.api_client.ApiClient,
    organization_id: str,
    workspace_id: str,
    file_path: str,
    workspace_path: str,
    overwrite: bool = True
) -> str:
    """
    Upload a local file to a given workspace
    
    If workspace_path ends with a "/" it will be considered as a folder inside the workspace 
    and the file will keep its current name
    
    :param api_client: An api client used to connect to the Cosmo Tech API
    :param organization_id: An ID of an Organization in the Cosmo Tech API
    :param workspace_id: An ID of a Workspace in the Cosmo Tech API
    :param file_path: Path to the file to upload in the workspace
    :param workspace_path: The path inside the workspace to upload the file to
    :param overwrite: Overwrite existing file in the workspace
    :return: The final name of the file uploaded to the workspace
    """
    target_file = pathlib.Path(file_path)
    if not target_file.exists():
        LOGGER.error(f'"{file_path}" does not exists')
        raise ValueError(f'"{file_path}" does not exists')
    if not target_file.is_file():
        LOGGER.error(f'"{file_path}" is not a single file')
        raise ValueError(f'"{file_path}" is not a single file')

    api_ws = cosmotech_api.api.workspace_api.WorkspaceApi(api_client)
    destination = workspace_path + target_file.name if workspace_path.endswith("/") else workspace_path

    LOGGER.info(f"Sending [bold green]{target_file.name}[/] as [bold green]{destination}[/] to the API")
    try:
        _file = api_ws.upload_workspace_file(organization_id,
                                             workspace_id,
                                             target_file,
                                             overwrite,
                                             destination=destination)
    except cosmotech_api.exceptions.ApiException as e:
        LOGGER.error(f"[bold green]{destination}[/] already exists, use the [cyan]overwrite flag[/] to replace it")
        raise e

    LOGGER.info(f"[bold green]{_file.file_name}[/] successfuly sent to the API")
    return _file.file_name
