---
description: "Comprehensive guide to the csm-data CLI: a powerful data management tool for CosmoTech platforms"
---

# csm-data

!!! abstract "Objective"
    + Understand what the csm-data CLI is and its capabilities
    + Learn how to use the various command groups for different data management tasks
    + Explore common use cases and workflows
    + Master integration with CosmoTech platform services

## What is csm-data?

`csm-data` is a powerful Command Line Interface (CLI) bundled inside the CosmoTech Acceleration Library (CoAL). It provides a comprehensive set of commands designed to streamline interactions with various services used within a CosmoTech platform.

The CLI is organized into several command groups, each focused on specific types of data operations:

- **api**: Commands for interacting with the CosmoTech API
- **store**: Commands for working with the CoAL datastore (load CSV/Parquet, dump, output channels, delete, reset)
- **custom**: Optional customized commands for special use cases (load CSV files straight into PostgreSQL tables), see [Custom Commands](#custom-commands)
- **s3-bucket-***: Commands for S3 bucket operations (download, upload, delete)
- **adx-send-runnerdata**: Command for sending runner data to Azure Data Explorer
- **az-storage-upload**: Command for uploading to Azure Storage

!!! info "Getting Help"
    You can get detailed help for any command using the `--help` flag:
    ```bash
    --8<-- 'tutorial/csm-data/getting_help.bash'
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

#### Runner Data Management

```bash title="Download run data" linenums="1" hl_lines="7 8 9"
--8<-- 'tutorial/csm-data/run_load_data.bash'
```

This command:
- Downloads scenario parameters and datasets from the CosmoTech API
- Writes parameters as JSON and/or CSV files
- Fetches associated datasets

!!! tip "Common Use Case"
    This command is particularly useful in container environments where you need to initialize your simulation with data from the platform. The environment variables are typically set by the platform when launching the container.
### Storage Commands

The `s3-bucket-*` commands provide a simple interface for working with S3-compatible storage:

=== "Download"
    ```bash title="Download from S3 bucket" linenums="1"
    --8<-- 'tutorial/csm-data/s3_bucket_download.bash'
    ```

=== "Upload"
    ```bash title="Upload to S3 bucket" linenums="1"
    --8<-- 'tutorial/csm-data/s3_bucket_upload.bash'
    ```

=== "Delete"
    ```bash title="Delete from S3 bucket" linenums="1"
    --8<-- 'tutorial/csm-data/s3_bucket_delete.bash'
    ```

!!! tip "Environment Variables"
    All these commands support environment variables for credentials and connection details, making them secure and easy to use in automated workflows:
    ```bash
    --8<-- 'tutorial/csm-data/s3_env_variables.bash'
    ```

### Azure Data Explorer Integration

The `adx-send-runnerdata` command enables sending runner data to Azure Data Explorer for analysis and visualization:

```bash title="Send runner data to ADX" linenums="1"
--8<-- 'tutorial/csm-data/adx_send_runnerdata.bash'
```

This command:

- Creates tables in ADX based on CSV files in the dataset and/or parameters folders
- Ingests the data into those tables
- Adds a `run` column with the runner ID for tracking
- Optionally waits for ingestion to complete

!!! warning "Table Creation"
    This command will create tables in ADX based on the CSV file names and headers. Ensure your CSV files have appropriate headers and follow naming conventions suitable for ADX tables.

### Datastore Commands

The `store` command group provides tools for working with the CoAL datastore:

```bash title="Load CSV folder into datastore" linenums="1"
--8<-- 'tutorial/csm-data/store_load_csv_folder.bash'
```

```bash title="Dump datastore to S3" linenums="1"
--8<-- 'tutorial/csm-data/store_dump_to_s3.bash'
```

These commands allow you to:

- Load data from CSV files into the datastore
- Load data from Parquet files into the datastore
- Dump datastore contents to various destinations (S3, Azure, PostgreSQL)
- Send the full datastore output through one or more configured output channels
- List tables in the datastore
- Delete specific tables from the datastore
- Reset the datastore

### Custom Commands

The `custom` command group contains optional, customized commands for special use cases. They do not follow the
standard datastore design and need an extra dependency:

```bash
pip install "cosmotech-acceleration-library[custom]"
```

#### Loading CSV files directly into PostgreSQL

`load-csv-file` and `load-csv-folder` send CSV files straight into **existing** PostgreSQL tables, without going
through the datastore. Use them when you want to keep the types declared on your tables, or when the files are
too large for the datastore route (`store load-csv-folder` then `store dump-to-postgresql`).

```bash title="Load one CSV file into a table" linenums="1"
--8<-- 'tutorial/csm-data/custom_load_csv_file.bash'
```

```bash title="Load every CSV file of a folder, one table per file" linenums="1"
--8<-- 'tutorial/csm-data/custom_load_csv_folder.bash'
```

How they behave:

- The tables must already exist, for example created with the following statement. Their column types are used to
  convert the CSV values, and each CSV header must only contain column names of the table (case-sensitive).

    ```sql title="Table prepared before the load" linenums="1"
    --8<-- 'tutorial/csm-data/custom_create_table.sql'
    ```

- The table of a file is `--table` for `load-csv-file`, otherwise the table prefix (default `Cosmotech_`) followed by
  the file name without extension, in lowercase.
- Each file is loaded in a single transaction: if anything fails, nothing is written for that file.
  `load-csv-folder` checks all the files and tables before the first load, then loads the files one after the
  other: if a load fails, the tables already loaded stay committed.
- With `--add-id`, the `csm_run_id` column of the table is filled with `--run-id` (or `CSM_RUN_ID`) for every row. Without it, the CSV is loaded as is and no run id is added.
- The connection comes from `--postgres-*` options (all of host, db, schema and user, and the password), or from the
  CoAL configuration file and `POSTGRES_*` environment variables when none of them is given.
  Configuration uses the top-level `[postgres]` section, or, if absent, the single `[[outputs]]` entry of type
  `postgres` with settings under `[outputs.conf.postgres]`. Multiple PostgreSQL outputs require an explicit choice
  through `[postgres]` or the connection options.

## Common Workflows and Integration Patterns

### Runner Data Processing Pipeline

A common workflow combines multiple commands to create a complete data processing pipeline:

```bash title="Complete data processing pipeline" linenums="1"
--8<-- 'tutorial/csm-data/complete_pipeline.bash'
```

### Integration with csm-orc

The `csm-data` commands integrate seamlessly with `csm-orc` for orchestration:

```json title="run.json for csm-orc" linenums="1"
--8<-- 'tutorial/csm-data/csm_orc_integration.json'
```

```json title="delete.json for csm-orc" linenums="1"
--8<-- 'tutorial/csm-data/csm_orc_integration_delete.json'
```

## Best Practices and Tips

!!! tip "Environment Variables"
    Use environment variables for sensitive information and configuration that might change between environments:
    ```bash
    --8<-- 'tutorial/csm-data/api_env_variables.bash'
    ```

!!! tip "Error Handling"
    Most commands will exit with a non-zero status code on failure, making them suitable for use in scripts and orchestration tools that check exit codes.

!!! tip "Logging"
    Control the verbosity of logging with the `--log-level` option:
    ```bash
    --8<-- 'tutorial/csm-data/logging.bash'
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
