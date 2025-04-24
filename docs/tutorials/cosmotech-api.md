---
description: "Comprehensive guide to working with the CosmoTech API in CoAL: authentication, workspaces, Twin Data Layer, and more"
---

# Working with the CosmoTech API

!!! abstract "Objective"
    + Understand how to authenticate and connect to the CosmoTech API
    + Learn to work with workspaces for file management
    + Master the Twin Data Layer for graph data operations
    + Implement runner and run data management
    + Build complete workflows integrating multiple API features

## Introduction to the CosmoTech API Integration

The CosmoTech Acceleration Library (CoAL) provides a comprehensive set of tools for interacting with the CosmoTech API. This integration allows you to:

- Authenticate with different identity providers
- Manage workspaces and files
- Work with the Twin Data Layer for graph data
- Handle runners and runs
- Process and transform data
- Build end-to-end workflows

The API integration is organized into several modules, each focused on specific functionality:

- **connection**: Authentication and API client management
- **workspace**: Workspace file operations
- **twin_data_layer**: Graph data management
- **runner**: Runner and run data operations

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

The `get_api_client()` function automatically detects which authentication method to use based on the environment variables you've set.

```python title="Basic connection setup" linenums="1"
--8<-- 'tutorial/cosmotech-api/connection_setup.py'
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

Workspaces in the CosmoTech platform provide a way to organize and share files. The CoAL library offers functions for listing, downloading, and uploading files in workspaces.

```python title="Workspace operations" linenums="1"
--8<-- 'tutorial/cosmotech-api/workspace_operations.py'
```

### Listing Files

The `list_workspace_files` function allows you to list files in a workspace with a specific prefix:

```python
files = list_workspace_files(api_client, organization_id, workspace_id, file_prefix)
```

This is useful for finding files in a specific directory or with a specific naming pattern.

### Downloading Files

The `download_workspace_file` function downloads a file from the workspace to a local directory:

```python
downloaded_file = download_workspace_file(
    api_client, 
    organization_id, 
    workspace_id, 
    file_to_download, 
    target_directory
)
```

If the file is in a subdirectory in the workspace, the function will create the necessary local subdirectories.

### Uploading Files

The `upload_workspace_file` function uploads a local file to the workspace:

```python
uploaded_file = upload_workspace_file(
    api_client,
    organization_id,
    workspace_id,
    file_to_upload,
    workspace_destination,
    overwrite=True
)
```

The `workspace_destination` parameter can be:
- A specific file path in the workspace
- A directory path ending with `/`, in which case the original filename is preserved

!!! tip "Workspace Paths"
    When working with workspace paths:
    
    - Use forward slashes (`/`) regardless of your operating system
    - End directory paths with a trailing slash (`/`)
    - Use relative paths from the workspace root

## Twin Data Layer Operations

The Twin Data Layer (TDL) is a graph database that stores nodes and relationships. CoAL provides tools for working with the TDL, particularly for preparing and sending CSV data.

```python title="Twin Data Layer operations" linenums="1"
--8<-- 'tutorial/cosmotech-api/twin_data_layer.py'
```

### CSV File Format

The TDL expects CSV files in a specific format:

- **Node files**: Must have an `id` column and can have additional property columns
- **Relationship files**: Must have `src` and `dest` columns and can have additional property columns

The filename (without the `.csv` extension) becomes the node label or relationship type in the graph.

### Parsing CSV Files

The `CSVSourceFile` class helps parse CSV files and determine if they represent nodes or relationships:

```python
csv_file = CSVSourceFile(file_path)
print(f"Is node: {csv_file.is_node}")
print(f"Fields: {csv_file.fields}")
```

### Generating Cypher Queries

The `generate_query_insert` method creates Cypher queries for inserting data into the TDL:

```python
query = csv_file.generate_query_insert()
```

These queries can then be executed using the TwinGraphApi:

```python
twin_graph_api.run_twin_graph_cypher_query(
    organization_id=organization_id,
    workspace_id=workspace_id,
    twin_graph_id=twin_graph_id,
    twin_graph_cypher_query={
        "query": query,
        "parameters": params
    }
)
```

!!! warning "Node References"
    When creating relationships, make sure the nodes referenced by the `src` and `dest` columns already exist in the graph. Otherwise, the relationship creation will fail.

## Runner and Run Management

Runners and runs are central concepts in the CosmoTech platform. CoAL provides functions for working with runner data, parameters, and associated datasets.

```python title="Runner operations" linenums="1"
--8<-- 'tutorial/cosmotech-api/runner_operations.py'
```

### Getting Runner Data

The `get_runner_data` function retrieves information about a runner:

```python
runner_data = get_runner_data(organization_id, workspace_id, runner_id)
```

### Working with Parameters

The `get_runner_parameters` function extracts parameters from runner data:

```python
parameters = get_runner_parameters(runner_data)
```

### Downloading Runner Data

The `download_runner_data` function downloads all data associated with a runner, including parameters and datasets:

```python
result = download_runner_data(
    organization_id=organization_id,
    workspace_id=workspace_id,
    runner_id=runner_id,
    parameter_folder=str(param_dir),
    dataset_folder=str(dataset_dir),
    write_json=True,
    write_csv=True,
    fetch_dataset=True,
)
```

This function:
- Downloads parameters and writes them as JSON and/or CSV files
- Downloads associated datasets
- Organizes everything in the specified directories

!!! tip "Dataset References"
    Runners can reference datasets in two ways:
    
    - Through parameters with the `%DATASETID%` variable type
    - Through the `dataset_list` property
    
    The `download_runner_data` function handles both types of references.

## Complete Workflow Example

Putting it all together, here's a complete workflow that demonstrates how to use the CosmoTech API for a data processing pipeline:

```python title="Complete workflow" linenums="1"
--8<-- 'tutorial/cosmotech-api/complete_workflow.py'
```

This workflow:

1. Downloads runner data (parameters and datasets)
2. Processes the data (calculates loyalty scores for customers)
3. Uploads the processed data to the workspace
4. Prepares the data for the Twin Data Layer
5. Generates a report with statistics and insights

!!! tip "Real-world Workflows"
    In real-world scenarios, you might:
    
    - Use more complex data transformations
    - Integrate with external systems
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
try:
    # API operations
except cosmotech_api.exceptions.ApiException as e:
    # Handle API errors
    print(f"API error: {e.status} - {e.reason}")
except Exception as e:
    # Handle other errors
    print(f"Error: {e}")
finally:
    # Always close the client
    api_client.close()
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
