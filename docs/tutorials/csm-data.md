---
description: "Comprehensive guide to the csm-data CLI: a powerful data management tool for CosmoTech platforms"
---

# CSM-DATA

!!! abstract "Objective"
    + Understand what the csm-data CLI is and its capabilities
    + Learn how to use the various command groups for different data management tasks
    + Explore common use cases and workflows
    + Master integration with CosmoTech platform services

## What is csm-data?

`csm-data` is a powerful Command Line Interface (CLI) bundled inside the CosmoTech Acceleration Library (CoAL). It provides a comprehensive set of commands designed to streamline interactions with various services used within a CosmoTech platform.

The CLI is organized into several command groups, each focused on specific types of data operations:

- **api**: Commands for interacting with the CosmoTech API
- **store**: Commands for working with the CoAL datastore
- **s3-bucket-***: Commands for S3 bucket operations (download, upload, delete)
- **adx-send-scenariodata**: Command for sending scenario data to Azure Data Explorer
- **az-storage-upload**: Command for uploading to Azure Storage

!!! info "Getting Help"
    You can get detailed help for any command using the `--help` flag:
    ```bash
    csm-data --help
    csm-data api --help
    csm-data api scenariorun-load-data --help
    ```

## Why use csm-data?

### Standardized Interactions

The `csm-data` CLI provides tested, standardized interactions with multiple services used in CosmoTech simulations. This eliminates the need to:

- Write custom code for common data operations
- Handle authentication and connection details for each service
- Manage error handling and retries
- Deal with format conversions between services

### Environment Variable Support

Most commands support environment variables, making them ideal for:

- Integration with orchestration tools like `csm-orc`
- Use in Docker containers and cloud environments
- Secure handling of credentials and connection strings
- Consistent configuration across development and production

### Workflow Automation

The commands are designed to work together in data processing pipelines, enabling you to:

- Download data from various sources
- Transform and process the data
- Store results in different storage systems
- Send data to visualization and analysis services

## Command Groups and Use Cases

### API Commands

The `api` command group facilitates interaction with the CosmoTech API, allowing you to work with scenarios, datasets, and other API resources.

#### Scenario Data Management

```bash title="Download scenario data" linenums="1" hl_lines="7 8 9"
csm-data api scenariorun-load-data \
  --organization-id "o-organization" \
  --workspace-id "w-workspace" \
  --scenario-id "s-scenario" \
  --dataset-absolute-path "/path/to/dataset/folder" \
  --parameters-absolute-path "/path/to/parameters/folder" \
  --write-json \
  --write-csv \
  --fetch-dataset
```

This command:
- Downloads scenario parameters and datasets from the CosmoTech API
- Writes parameters as JSON and/or CSV files
- Fetches associated datasets

!!! tip "Common Use Case"
    This command is particularly useful in container environments where you need to initialize your simulation with data from the platform. The environment variables are typically set by the platform when launching the container.

#### Twin Data Layer Operations

```bash title="Load files to Twin Data Layer" linenums="1"
csm-data api tdl-load-files \
  --organization-id "o-organization" \
  --workspace-id "w-workspace" \
  --dataset-id "d-dataset" \
  --source-folder "/path/to/source/files"
```

```bash title="Send files to Twin Data Layer" linenums="1"
csm-data api tdl-send-files \
  --organization-id "o-organization" \
  --workspace-id "w-workspace" \
  --dataset-id "d-dataset" \
  --source-folder "/path/to/source/files"
```

These commands facilitate working with the Twin Data Layer, allowing you to:
- Load data from the Twin Data Layer to local files
- Send local files to the Twin Data Layer

### Storage Commands

The `s3-bucket-*` commands provide a simple interface for working with S3-compatible storage:

=== "Download"
    ```bash title="Download from S3 bucket" linenums="1"
    csm-data s3-bucket-download \
      --target-folder "/path/to/download/to" \
      --bucket-name "my-bucket" \
      --prefix-filter "folder/prefix/" \
      --s3-url "https://s3.example.com" \
      --access-id "access-key-id" \
      --secret-key "secret-access-key"
    ```

=== "Upload"
    ```bash title="Upload to S3 bucket" linenums="1"
    csm-data s3-bucket-upload \
      --source-folder "/path/to/upload/from" \
      --bucket-name "my-bucket" \
      --target-prefix "folder/prefix/" \
      --s3-url "https://s3.example.com" \
      --access-id "access-key-id" \
      --secret-key "secret-access-key"
    ```

=== "Delete"
    ```bash title="Delete from S3 bucket" linenums="1"
    csm-data s3-bucket-delete \
      --bucket-name "my-bucket" \
      --prefix-filter "folder/prefix/" \
      --s3-url "https://s3.example.com" \
      --access-id "access-key-id" \
      --secret-key "secret-access-key"
    ```

!!! tip "Environment Variables"
    All these commands support environment variables for credentials and connection details, making them secure and easy to use in automated workflows:
    ```bash
    export AWS_ENDPOINT_URL="https://s3.example.com"
    export AWS_ACCESS_KEY_ID="access-key-id"
    export AWS_SECRET_ACCESS_KEY="secret-access-key"
    export CSM_DATA_BUCKET_NAME="my-bucket"
    ```

### Azure Data Explorer Integration

The `adx-send-scenariodata` command enables sending scenario data to Azure Data Explorer for analysis and visualization:

```bash title="Send scenario data to ADX" linenums="1"
csm-data adx-send-scenariodata \
  --dataset-absolute-path "/path/to/dataset/folder" \
  --parameters-absolute-path "/path/to/parameters/folder" \
  --simulation-id "simulation-id" \
  --adx-uri "https://adx.example.com" \
  --adx-ingest-uri "https://ingest-adx.example.com" \
  --database-name "my-database" \
  --send-datasets \
  --wait
```

This command:
- Creates tables in ADX based on CSV files in the dataset and/or parameters folders
- Ingests the data into those tables
- Adds a `simulationrun` column with the simulation ID for tracking
- Optionally waits for ingestion to complete

!!! warning "Table Creation"
    This command will create tables in ADX based on the CSV file names and headers. Ensure your CSV files have appropriate headers and follow naming conventions suitable for ADX tables.

### Datastore Commands

The `store` command group provides tools for working with the CoAL datastore:

```bash title="Load CSV folder into datastore" linenums="1"
csm-data store load-csv-folder \
  --folder-path "/path/to/csv/folder" \
  --reset
```

```bash title="Dump datastore to S3" linenums="1"
csm-data store dump-to-s3 \
  --bucket-name "my-bucket" \
  --target-prefix "store-dump/" \
  --s3-url "https://s3.example.com" \
  --access-id "access-key-id" \
  --secret-key "secret-access-key"
```

These commands allow you to:
- Load data from CSV files into the datastore
- Dump datastore contents to various destinations (S3, Azure, PostgreSQL)
- List tables in the datastore
- Reset the datastore

## Common Workflows and Integration Patterns

### Scenario Data Processing Pipeline

A common workflow combines multiple commands to create a complete data processing pipeline:

```bash title="Complete data processing pipeline" linenums="1"
# 1. Download scenario data from the API
csm-data api scenariorun-load-data \
  --organization-id "$CSM_ORGANIZATION_ID" \
  --workspace-id "$CSM_WORKSPACE_ID" \
  --scenario-id "$CSM_SCENARIO_ID" \
  --dataset-absolute-path "$CSM_DATASET_ABSOLUTE_PATH" \
  --parameters-absolute-path "$CSM_PARAMETERS_ABSOLUTE_PATH" \
  --write-json \
  --fetch-dataset

# 2. Load data into the datastore for processing
csm-data store load-csv-folder \
  --folder-path "$CSM_DATASET_ABSOLUTE_PATH" \
  --reset

# 3. Run your simulation (using your own code)
# ...

# 4. Send results to Azure Data Explorer for analysis
csm-data adx-send-scenariodata \
  --dataset-absolute-path "$CSM_DATASET_ABSOLUTE_PATH" \
  --parameters-absolute-path "$CSM_PARAMETERS_ABSOLUTE_PATH" \
  --simulation-id "$CSM_SIMULATION_ID" \
  --adx-uri "$AZURE_DATA_EXPLORER_RESOURCE_URI" \
  --adx-ingest-uri "$AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI" \
  --database-name "$AZURE_DATA_EXPLORER_DATABASE_NAME" \
  --send-datasets \
  --wait
```

### Integration with csm-orc

The `csm-data` commands integrate seamlessly with `csm-orc` for orchestration:

```json title="run.json for csm-orc" linenums="1"
{
  "steps": [
    {
      "id": "download-scenario-data",
      "command": "csm-data",
      "arguments": [
        "api", "scenariorun-load-data",
        "--write-json",
        "--fetch-dataset"
      ],
      "useSystemEnvironment": true
    },
    {
      "id": "run-simulation",
      "command": "python",
      "arguments": ["run_simulation.py"],
      "precedents": ["download-scenario-data"]
    },
    {
      "id": "send-results-to-adx",
      "command": "csm-data",
      "arguments": [
        "adx-send-scenariodata",
        "--send-datasets",
        "--wait"
      ],
      "useSystemEnvironment": true,
      "precedents": ["run-simulation"]
    }
  ]
}
```

## Best Practices and Tips

!!! tip "Environment Variables"
    Use environment variables for sensitive information and configuration that might change between environments:
    ```bash
    # API connection
    export CSM_ORGANIZATION_ID="o-organization"
    export CSM_WORKSPACE_ID="w-workspace"
    export CSM_SCENARIO_ID="s-scenario"
    
    # Paths
    export CSM_DATASET_ABSOLUTE_PATH="/path/to/dataset"
    export CSM_PARAMETERS_ABSOLUTE_PATH="/path/to/parameters"
    
    # ADX connection
    export AZURE_DATA_EXPLORER_RESOURCE_URI="https://adx.example.com"
    export AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI="https://ingest-adx.example.com"
    export AZURE_DATA_EXPLORER_DATABASE_NAME="my-database"
    ```

!!! tip "Error Handling"
    Most commands will exit with a non-zero status code on failure, making them suitable for use in scripts and orchestration tools that check exit codes.

!!! tip "Logging"
    Control the verbosity of logging with the `--log-level` option:
    ```bash
    csm-data --log-level debug api scenariorun-load-data ...
    ```

## Extending csm-data

If the existing commands don't exactly match your needs, you have several options:

1. **Use as a basis**: Examine the code of similar commands and use it as a starting point for your own scripts
2. **Combine commands**: Use shell scripting to combine multiple commands into a custom workflow
3. **Environment variables**: Customize behavior through environment variables without modifying the code
4. **Contribute**: Consider contributing enhancements back to the CoAL project

## Conclusion

The `csm-data` CLI provides a powerful set of tools for managing data in CosmoTech platform environments. By leveraging these commands, you can:

- Streamline interactions with platform services
- Automate data processing workflows
- Integrate with orchestration tools
- Focus on your simulation logic rather than data handling

Whether you're developing locally or deploying to production, `csm-data` offers a consistent interface for your data management needs.
