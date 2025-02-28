# Example: Working with workspaces in the CosmoTech API
import os
import pathlib
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.workspace import (
    list_workspace_files,
    download_workspace_file,
    upload_workspace_file,
)
from cosmotech.coal.utils.logger import LOGGER

# Set up environment variables for authentication
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

# Organization and workspace IDs
organization_id = "your-organization-id"  # Replace with your organization ID
workspace_id = "your-workspace-id"  # Replace with your workspace ID

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

try:
    # Example 1: List files in a workspace with a specific prefix
    file_prefix = "data/"  # List files in the "data" directory
    try:
        files = list_workspace_files(api_client, organization_id, workspace_id, file_prefix)
        print(f"Files in workspace with prefix '{file_prefix}':")
        for file in files:
            print(f"  - {file}")
    except ValueError as e:
        print(f"Error listing files: {e}")

    # Example 2: Download a file from the workspace
    file_to_download = "data/sample.csv"  # Replace with an actual file in your workspace
    target_directory = pathlib.Path("./downloaded_files")
    target_directory.mkdir(exist_ok=True, parents=True)

    try:
        downloaded_file = download_workspace_file(
            api_client, organization_id, workspace_id, file_to_download, target_directory
        )
        print(f"Downloaded file to: {downloaded_file}")
    except Exception as e:
        print(f"Error downloading file: {e}")

    # Example 3: Upload a file to the workspace
    file_to_upload = "./local_data/upload_sample.csv"  # Replace with a local file path
    workspace_destination = "data/uploaded/"  # Destination in the workspace (ending with / to keep filename)

    try:
        uploaded_file = upload_workspace_file(
            api_client,
            organization_id,
            workspace_id,
            file_to_upload,
            workspace_destination,
            overwrite=True,  # Set to False to prevent overwriting existing files
        )
        print(f"Uploaded file as: {uploaded_file}")
    except Exception as e:
        print(f"Error uploading file: {e}")

finally:
    # Always close the API client when done
    api_client.close()
