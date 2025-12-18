# Example: Working with runners and runs in the CosmoTech API
import os
import pathlib

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.runner import (
    download_datasets,
    download_runner_data,
    get_runner_data,
    get_runner_parameters,
)
from cosmotech.coal.utils.logger import LOGGER

# Set up environment variables for authentication
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

# Organization, workspace, and runner IDs
organization_id = "your-organization-id"  # Replace with your organization ID
workspace_id = "your-workspace-id"  # Replace with your workspace ID
runner_id = "your-runner-id"  # Replace with your runner ID

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

try:
    # Example 1: Get runner data
    runner_data = get_runner_data(organization_id, workspace_id, runner_id)
    print(f"Runner name: {runner_data.name}")
    print(f"Runner ID: {runner_data.id}")
    print(f"Runner state: {runner_data.state}")

    # Example 2: Get runner parameters
    parameters = get_runner_parameters(runner_data)
    print("\nRunner parameters:")
    for param in parameters:
        print(f"  - {param['parameterId']}: {param['value']} (type: {param['varType']})")

    # Example 3: Download runner data (parameters and datasets)
    # Create directories for parameters and datasets
    param_dir = pathlib.Path("./runner_parameters")
    dataset_dir = pathlib.Path("./runner_datasets")
    param_dir.mkdir(exist_ok=True, parents=True)
    dataset_dir.mkdir(exist_ok=True, parents=True)

    # Download runner data
    result = download_runner_data(
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=runner_id,
        parameter_folder=str(param_dir),
        dataset_folder=str(dataset_dir),
        read_files=True,  # Read file contents
        parallel=True,  # Download datasets in parallel
        write_json=True,  # Write parameters as JSON
        write_csv=True,  # Write parameters as CSV
        fetch_dataset=True,  # Fetch datasets
    )

    print("\nDownloaded runner data:")
    print(f"  - Parameters saved to: {param_dir}")
    print(f"  - Datasets saved to: {dataset_dir}")

    # Example 4: Working with specific datasets
    if result["datasets"]:
        print("\nDatasets associated with the runner:")
        for dataset_id, dataset_info in result["datasets"].items():
            print(f"  - Dataset ID: {dataset_id}")
            print(f"    Name: {dataset_info.get('name', 'N/A')}")

            # List files in the dataset
            if "files" in dataset_info:
                print(f"    Files:")
                for file_info in dataset_info["files"]:
                    print(f"      - {file_info.get('name', 'N/A')}")
    else:
        print("\nNo datasets associated with this runner.")

    # Example 5: Download specific datasets
    """
    from cosmotech.coal.cosmotech_api.runner import get_dataset_ids_from_runner

    # Get dataset IDs from the runner
    dataset_ids = get_dataset_ids_from_runner(runner_data)

    if dataset_ids:
        # Create a directory for the datasets
        specific_dataset_dir = pathlib.Path("./specific_datasets")
        specific_dataset_dir.mkdir(exist_ok=True, parents=True)

        # Download the datasets
        datasets = download_datasets(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
            read_files=True,
            parallel=True,
        )

        print("\nDownloaded specific datasets:")
        for dataset_id, dataset_info in datasets.items():
            print(f"  - Dataset ID: {dataset_id}")
            print(f"    Name: {dataset_info.get('name', 'N/A')}")
    """

finally:
    # Always close the API client when done
    api_client.close()
