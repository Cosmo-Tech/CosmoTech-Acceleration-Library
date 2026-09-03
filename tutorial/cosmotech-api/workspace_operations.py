# Example: Working with workspaces in the CosmoTech API
import pathlib

from cosmotech.orchestrator.utils.logger import get_logger

from cosmotech.coal.cosmotech_api.apis import WorkspaceApi
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION as EC

logger = get_logger("my_project.worspace_work")

# Use Coal configuration to setup connection and WorkspaceApi object
ws_api = WorkspaceApi()

organization_id = EC.cosmotech.organization_id
workspace_id = EC.cosmotech.workspace_id

# Example 1: List workspace files with a given prefix
file_prefix = "data/"
try:
    files = ws_api.list_filtered_workspace_files(organization_id, workspace_id, file_prefix)
    logger.info(f"Files in workspace with prefix '{file_prefix}':")
    for file in files:
        logger.info(f"  - {file}")
except ValueError as e:
    logger.error(f"No files found: {e}")

# Example 2: Download a file from the workspace
file_to_download = "data/sample.csv"  # Replace with an actual file in your workspace
target_directory = pathlib.Path("./downloaded_files")
target_directory.mkdir(exist_ok=True, parents=True)

try:
    local_path = ws_api.download_workspace_file(organization_id, workspace_id, file_to_download, target_directory)
    logger.info(f"Downloaded file to: {local_path}")
except Exception as e:
    logger.error(f"Error downloading file: {e}")

# Example 3: Upload a file to the workspace
file_path = "./local_data/upload_sample.csv"  # Replace with a local file path
workspace_path = "data/uploaded/"  # Trailing slash → original filename is kept

try:
    uploaded_name = ws_api.upload_workspace_file(
        organization_id,
        workspace_id,
        file_path,
        workspace_path,
        overwrite=True,
    )
    logger.info(f"Uploaded file as: {uploaded_name}")
except Exception as e:
    logger.error(f"Error uploading file: {e}")
