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
All API wrapper classes (`WorkspaceApi`, `RunnerApi`, `DatasetApi`, …) extend `Connection` and set themselves up automatically — you do not need to create the `Connection` separately unless you want direct access to the raw `ApiClient`.

```python
from cosmotech.coal.cosmotech_api.apis import WorkspaceApi, RunnerApi, DatasetApi

ws_api = WorkspaceApi()   # auth resolved automatically
runner_api = RunnerApi()
dataset_api = DatasetApi()
```

!!! tip "Environment Variables"
    You can set environment variables in your code for testing, but in production environments, it's better to set them at the container level using Coal configuration. Coal configuration uses a combination of Kubernetes ConfigMaps and Secrets to setup the environnement.

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

## Configuration

The CoAL configuration system is based on a centralized data dictionary used to manage platform settings and behaviors dynamically. It allows scripts to run without requiring users to manually define connection or output specifics every single time. Data is primarily sourced from a TOML file loaded into a Kubernetes ConfigMap.

### Core mechanics

- The Configuration singleton: CoAL provides a `ENVIRONMENT_CONFIGURATION` singleton that users can import this into their scripts (`from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION as EC`) to access properties using dot-notation, such as `EC.cosmotech.runner_id`.

- Kubernetes (K8s) ConfigMap integration: To supply configuration inside a pod launched via a workflow, CoAL mounts a K8s ConfigMap containing the configuration file directly inside the container.

- Automatic path loading: CoAL automatically attempts to load the TOML file at the specific path `/mnt/coal/coal-config.toml`, making K8s ConfigMap auto-mounts seamless.

### Syntax

The configuration uses the TOML format to support specific features:

- **secrets**: Environment variables (e.g. credentials, `TWIN_CACHE_HOST`, or `IDP_BASE_URL`) that are loaded at startup. At import, they are initialized and then removed from the final configuration dictionary, so variables like `run_template_id` are accessed directly under `EC.cosmotech` rather than a "secrets" sub-dictionary. CosmoTech environment variables provided by the API are always loaded.

- **env.**: Fetches environment variables dynamically at runtime (e.g. `env.POSTGRES_USER_PASSWORD`), unlike "secrets" which are resolved statically at import.

- **Internal References ($)**: Allows configuration keys to reference other values in the same TOML file (e.g. `$postgres.host`).

- **[[outputs]]**: Uses TOML double-bracket list syntax to define a series of output destinations (such as PostgreSQL, S3, or Azure Blob Storage) utilized by the ChannelSplitter to direct simulation results.

- **Error handling**: CoAL handles internal configuration references (like `$config.path`) with proper error reporting such as the `ReferenceKeyError` exception for missing configuration references.

### Configuration dictionary

```toml title="Configuration TOML file" linenums="1"
--8<-- 'tutorial/cosmotech-api/coal-config.toml'
```

## Working with Workspaces

Workspaces in the CosmoTech platform provide a way to organize and share files. `WorkspaceApi` offers methods for listing, downloading, and uploading files.

```python title="Workspace operations" linenums="1"
--8<-- 'tutorial/cosmotech-api/workspace_operations.py'
```

### Listing Files

`list_filtered_workspace_files` returns all workspace files whose `file_name` starts with the given prefix. It raises `ValueError` when no matching files are found:

```python
files = ws_api.list_filtered_workspace_files(
    organization_id,
    workspace_id,
    file_prefix
)
```

This is useful for finding files in a specific directory or with a specific naming pattern.

### Downloading Files

`download_workspace_file` writes the file content to `target_dir / file_name`, creating any necessary intermediate directories:


```python
local_path = ws_api.download_workspace_file(
    organization_id,
    workspace_id,
    file_to_download,
    target_directory
)
```

### Uploading Files

`upload_workspace_file` uploads a single local file:

```python
uploaded_name = ws_api.upload_workspace_file(
    organization_id,
    workspace_id,
    file_path,
    workspace_path,
    overwrite=True,
)
```

The `workspace_path` parameter can be:

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
--8<-- 'tutorial/cosmotech-api/dataset_operations.py'
```

!!! info "Dataset Parts"
    When uploading parts, the part name is derived from the filename without its extension.

## Runner Management

Runners are central concepts in the CosmoTech platform. `RunnerApi` provides methods for retrieving runner metadata and downloading all associated data (parameters and datasets).

```python title="Runner operations" linenums="1"
--8<-- 'tutorial/cosmotech-api/runner_operations.py'
```

## Best Practices and Tips

### Authentication

- Implement proper secret management in production
- Use Coal configuration secrets loading for credentials

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
