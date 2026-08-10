# Example: Working with runners and runs in the CosmoTech API
import os
import pathlib

from cosmotech.coal.cosmotech_api.apis import RunnerApi
from cosmotech.coal.utils.configuration import Configuration

os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

organization_id = "your-organization-id"  # Replace with your organization ID
workspace_id = "your-workspace-id"  # Replace with your workspace ID
runner_id = "your-runner-id"  # Replace with your runner ID

# Directories for downloaded data
param_dir = pathlib.Path("./runner_parameters")
dataset_dir = pathlib.Path("./runner_datasets")
param_dir.mkdir(exist_ok=True, parents=True)
dataset_dir.mkdir(exist_ok=True, parents=True)

# Build a Configuration scoped to this runner
config = Configuration()
config.cosmotech.organization_id = organization_id
config.cosmotech.workspace_id = workspace_id
config.cosmotech.runner_id = runner_id
config.cosmotech.parameters_absolute_path = str(param_dir)
config.cosmotech.dataset_absolute_path = str(dataset_dir)

runner_api = RunnerApi(config)

# Example 1: Get runner metadata
metadata = runner_api.get_runner_metadata(runner_id=runner_id)
print(f"Runner name:  {metadata.get('name')}")
print(f"Runner state: {metadata.get('state')}")

# Optionally scope the returned fields:
# metadata = runner_api.get_runner_metadata(
#     runner_id=runner_id, include=["parametersValues", "datasetList"]
# )

# Example 2: Download runner parameters and datasets
runner_api.download_runner_data(download_datasets=True)
print(f"Parameters saved to: {param_dir}")
print(f"Datasets    saved to: {dataset_dir}")
