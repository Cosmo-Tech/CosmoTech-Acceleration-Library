---
description: "Helpers"
---
# **cosmotech.coal.azure**

::: cosmotech.coal.azure.adx.auth
    options:
      members:
        - create_kusto_client
        - create_ingest_client
        - initialize_clients
        - get_cluster_urls

<br/>

::: cosmotech.coal.azure.adx.ingestion
    options:
      members:
        - ingest_dataframe
        - send_to_adx
        - check_ingestion_status
        - monitor_ingestion
        - handle_failures
        - clear_ingestion_status_queues

<br/>

::: cosmotech.coal.azure.adx.query
    options:
      members:
        - run_query
        - run_command_query

<br/>

::: cosmotech.coal.azure.adx.runner
    options:
      members:
        - prepare_csv_content
        - construct_create_query
        - insert_csv_files
        - send_runner_data

<br/>

::: cosmotech.coal.azure.adx.store
    options:
      members:
        - send_table_data
        - process_tables
        - send_pyarrow_table_to_adx
        - send_store_to_adx

<br/>

::: cosmotech.coal.azure.adx.tables
    options:
      members:
        - table_exists
        - check_and_create_table
        - create_table

<br/>

::: cosmotech.coal.azure.adx.utils
    options:
      members:
        - create_column_mapping
        - type_mapping

<br/>

::: cosmotech.coal.azure.blob
    options:
      members:
        - dump_store_to_azure
        - delete_azure_blobs

<br/>

::: cosmotech.coal.azure.storage
    options:
      members:
        - upload_file
        - upload_folder
