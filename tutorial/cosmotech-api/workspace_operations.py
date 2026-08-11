# Example: Working with workspaces in the CosmoTech API
import os
import pathlib

from cosmotech.coal.cosmotech_api.apis import WorkspaceApi

os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

organization_id = "your-organization-id"  # Replace with your organization ID
workspace_id = "your-workspace-id"  # Replace with your workspace ID

ws_api = WorkspaceApi()

# Example 1: List workspace files with a given prefix
file_prefix = "data/"
try:
    files = ws_api.list_filtered_workspace_files(organization_id, workspace_id, file_prefix)
    print(f"Files in workspace with prefix '{file_prefix}':")
    for file in files:
        print(f"  - {file}")
except ValueError as e:
    print(f"No files found: {e}")

# Example 2: Download a file from the workspace
file_to_download = "data/sample.csv"  # Replace with an actual file in your workspace
target_directory = pathlib.Path("./downloaded_files")
target_directory.mkdir(exist_ok=True, parents=True)

try:
    local_path = ws_api.download_workspace_file(organization_id, workspace_id, file_to_download, target_directory)
    print(f"Downloaded file to: {local_path}")
except Exception as e:
    print(f"Error downloading file: {e}")

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
    print(f"Uploaded file as: {uploaded_name}")
except Exception as e:
    print(f"Error uploading file: {e}")
