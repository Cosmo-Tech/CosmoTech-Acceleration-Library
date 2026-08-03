---
description: "Comprehensive guide to working with the CosmoTech API in CoAL: authentication, workspaces, runners, and datasets"
---

# Working with the CosmoTech API

!!! abstract "Objective"
    + Understand how to authenticate and connect to the CosmoTech API
    + Learn to work with workspaces for file management
    + Implement runner and run data management
    + Upload and download datasets
    + Build complete workflows integrating multiple API features

## Introduction to the CosmoTech API Integration

The CosmoTech Acceleration Library (CoAL) provides a comprehensive set of tools for interacting with the CosmoTech API. This integration allows you to:

- Authenticate with different identity providers
- Manage workspaces and files
- Handle runners and runs
- Upload and download datasets
- Process and transform data
- Build end-to-end workflows

The API integration is organized into two sub-packages under `cosmotech.coal.cosmotech_api`:

- **`objects/`**: Core building blocks
    - `connection` — `Connection` class: authentication and `ApiClient` management
    - `parameters` — `Parameters` class: typed access to runner parameters
- **`apis/`**: High-level wrappers for each CosmoTech API resource
    - `DatasetApi` — dataset upload, download, and parts management
    - `RunnerApi` — runner metadata and data download
    - `WorkspaceApi` — workspace file listing, download, and upload
    - `RunApi`, `OrganizationApi`, `SolutionApi`, `MetaApi` — additional resource wrappers

!!! info "API vs CLI"
    While the `csm-data` CLI provides command-line tools for many common operations, the direct API integration offers more flexibility and programmatic control. Use the API integration when you need to:

    - Build custom workflows
    - Integrate with other Python code
    - Perform complex operations not covered by the CLI
    - Implement real-time interactions with the platform

## Authentication and Connection

The first step in working with the CosmoTech API is establishing a connection. CoAL supports multiple authentication methods:

- API Key authentication
- Azure Entra (formerly Azure AD) authentication
- Keycloak authentication

The `Connection` class automatically detects which authentication method to use based on the environment variables present.

```python title="Basic connection setup" linenums="1"
from cosmotech.coal.cosmotech_api.objects.connection import Connection

# Connection auto-detects authentication from environment variables
connection = Connection()
api_client = connection.api_client  # cosmotech_api.ApiClient
```

All API wrapper classes (`WorkspaceApi`, `RunnerApi`, `DatasetApi`, …) extend `Connection` and set themselves up automatically — you do not need to create the `Connection` separately unless you want direct access to the raw `ApiClient`.

```python
from cosmotech.coal.cosmotech_api.apis import WorkspaceApi, RunnerApi, DatasetApi

ws_api = WorkspaceApi()   # auth resolved automatically
runner_api = RunnerApi()
dataset_api = DatasetApi()
```

!!! tip "Environment Variables"
    You can set environment variables in your code for testing, but in production environments, it's better to set them at the system or container level for security.

### API Key Authentication

API Key authentication is the simplest method and requires two environment variables:

- `CSM_API_URL`: The URL of the CosmoTech API
- `CSM_API_KEY`: Your API key

### Azure Entra Authentication

Azure Entra authentication uses service principal credentials and requires these environment variables:

- `CSM_API_URL`: The URL of the CosmoTech API
- `CSM_API_SCOPE`: The API scope (usually in the format `api://app-id/.default`)
- `AZURE_CLIENT_ID`: Your client ID
- `AZURE_CLIENT_SECRET`: Your client secret
- `AZURE_TENANT_ID`: Your tenant ID

### Keycloak Authentication

Keycloak authentication requires these environment variables:

- `CSM_API_URL`: The URL of the CosmoTech API
- `IDP_BASE_URL`: The base URL of your Keycloak server
- `IDP_TENANT_ID`: Your realm name
- `IDP_CLIENT_ID`: Your client ID
- `IDP_CLIENT_SECRET`: Your client secret

!!! warning "API Client Lifecycle"
    Always close the API client when you're done using it to release resources. The best practice is to use a `try`/`finally` block to ensure the client is closed even if an error occurs.

## Working with Workspaces

Workspaces in the CosmoTech platform provide a way to organize and share files. `WorkspaceApi` offers methods for listing, downloading, and uploading files.

```python title="Workspace operations" linenums="1"
from pathlib import Path
from cosmotech.coal.cosmotech_api.apis import WorkspaceApi

ws_api = WorkspaceApi()

# List files whose names start with a given prefix
files = ws_api.list_filtered_workspace_files(
    organization_id, workspace_id, file_prefix="inputs/"
)

# Download a file to a local directory
local_path = ws_api.download_workspace_file(
    organization_id, workspace_id,
    file_name="inputs/data.csv",
    target_dir=Path("/tmp/downloads"),
)

# Upload a local file to the workspace
uploaded_name = ws_api.upload_workspace_file(
    organization_id, workspace_id,
    file_path="/tmp/results/output.csv",
    workspace_path="outputs/",  # trailing slash → preserves original filename
    overwrite=True,
)
```

### Listing Files

`list_filtered_workspace_files` returns all workspace files whose `file_name` starts with the given prefix. It raises `ValueError` when no matching files are found.

### Downloading Files

`download_workspace_file` writes the file content to `target_dir / file_name`, creating any necessary intermediate directories.

### Uploading Files

`upload_workspace_file` uploads a single local file. The `workspace_path` parameter can be:

- A specific file path in the workspace
- A directory path ending with `/`, in which case the original filename is preserved

!!! tip "Workspace Paths"
    When working with workspace paths:

    - Use forward slashes (`/`) regardless of your operating system
    - End directory paths with a trailing slash (`/`)
    - Use relative paths from the workspace root

## Dataset Management

`DatasetApi` provides helpers for uploading datasets and managing their parts (files that compose the dataset).

```python title="Dataset upload" linenums="1"
from cosmotech.coal.cosmotech_api.apis import DatasetApi

dataset_api = DatasetApi()

# Upload a single file as a dataset
dataset_api.upload_dataset(
    organization_id=organization_id,
    dataset_id=dataset_id,
    file_path="/tmp/data/customers.csv",
)

# Upload multiple parts from a folder (one part per file)
dataset_api.upload_dataset_parts(
    organization_id=organization_id,
    dataset_id=dataset_id,
    folder_path="/tmp/data/parts/",
)

# Download a dataset to a local directory
dataset_api.download_dataset(
    dataset_id=dataset_id,
)
```

!!! info "Dataset Parts"
    When uploading parts, the part name is derived from the filename without its extension.

## Runner and Run Management

Runners and runs are central concepts in the CosmoTech platform. `RunnerApi` provides methods for retrieving runner metadata and downloading all associated data (parameters and datasets).

```python title="Runner operations" linenums="1"
from cosmotech.coal.cosmotech_api.apis import RunnerApi

runner_api = RunnerApi()

# Retrieve runner metadata as a dict
metadata = runner_api.get_runner_metadata(
    runner_id=runner_id,
    # optionally scope returned fields:
    # include=["parametersValues", "datasetList"]
)

# Download runner parameters and datasets
runner_api.download_runner_data(
    download_datasets="all",  # or None to skip dataset download
)
```

## Complete Workflow Example

Putting it all together, here's a typical end-to-end workflow for a CosmoTech data processing pipeline:

```python title="Complete workflow" linenums="1"
from cosmotech.coal.cosmotech_api.apis import RunnerApi, WorkspaceApi, DatasetApi
from pathlib import Path

# 1. Download runner parameters and datasets
runner_api = RunnerApi()
runner_api.download_runner_data(download_datasets="all")

# 2. Process the data (application-specific logic)
# ...

# 3. Upload results back to the workspace
ws_api = WorkspaceApi()
ws_api.upload_workspace_file(
    organization_id, workspace_id,
    file_path="/tmp/results/report.csv",
    workspace_path="outputs/",
    overwrite=True,
)

# 4. Update a dataset with processed parts
dataset_api = DatasetApi()
dataset_api.upload_dataset_parts(
    organization_id=organization_id,
    dataset_id=output_dataset_id,
    folder_path="/tmp/results/parts/",
)
```

This workflow:

1. Downloads runner parameters and associated datasets
2. Processes the data (application-specific logic)
3. Uploads processed results to the workspace
4. Updates a dataset with the processed output parts

!!! tip "Real-world Workflows"
    In real-world scenarios, you might:

    - Use more complex data transformations
    - Integrate with other Python code or services
    - Implement error handling and retries
    - Add logging and monitoring
    - Parallelize operations for better performance

## Best Practices and Tips

### Authentication

- Use environment variables for credentials
- Implement proper secret management in production
- Always close API clients when done

### Error Handling

```python
import cosmotech_api

try:
    # API operations
except cosmotech_api.exceptions.ApiException as e:
    # Handle API errors
    print(f"API error: {e.status} - {e.reason}")
except Exception as e:
    # Handle other errors
    print(f"Error: {e}")
```

### Performance Considerations

- Download datasets in parallel when possible (`parallel=True`)
- Batch operations when sending multiple items to the API
- Use appropriate error handling and retries for network operations

### Security

- Never hardcode credentials in your code
- Use the principle of least privilege for API keys and service principals
- Validate and sanitize inputs before sending them to the API

## Conclusion

The CosmoTech API integration in CoAL provides a powerful way to interact with the CosmoTech platform programmatically. By leveraging these capabilities, you can:

- Automate workflows
- Integrate with other systems
- Build custom applications
- Process and analyze data
- Create end-to-end solutions

Whether you're building data pipelines, creating custom interfaces, or integrating with existing systems, the CoAL library's API integration offers the tools you need to work effectively with the CosmoTech platform.
