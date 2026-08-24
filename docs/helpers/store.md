---
description: "Helpers"
---
# **cosmotech.coal.store**

::: cosmotech.coal.store.csv
    options:
      members:
        - store_csv_file
        - convert_store_table_to_csv

<br/>

::: cosmotech.coal.store.native_python
    options:
      members:
        - store_pylist
        - convert_table_as_pylist

<br/>

::: cosmotech.coal.store.output.aws_channel.AwsChannel
    options:
      members:
        - send
        - delete

<br/>

::: cosmotech.coal.store.output.az_storage_channel.AzureStorageChannel
    options:
      members:
        - send
        - delete

<br/>

::: cosmotech.coal.store.output.channel_interface.ChannelInterface
    options:
      members:
        - send
        - delete
        - is_available

<br/>

::: cosmotech.coal.store.output.channel_spliter.ChannelSpliter
    options:
      members:
        - send
        - delete

<br/>

::: cosmotech.coal.store.output.postgres_channel.PostgresChannel
    options:
      members:
        - send
        - delete

<br/>

::: cosmotech.coal.store.pandas
    options:
      members:
        - store_dataframe
        - convert_store_table_to_dataframe

<br/>

::: cosmotech.coal.store.parquet
    options:
      members:
        - store_parquet_file
        - convert_store_table_to_parquet

<br/>

::: cosmotech.coal.store.pyarrow
    options:
      members:
        - store_table
        - convert_store_table_to_dataframe

<br/>

::: cosmotech.coal.store.store
    options:
      members:
        - table_name_to_lower

<br/>

::: cosmotech.coal.store.store.Store
    options:
      members:
        - sanitize_column
        - reset
        - get_table
        - table_exists
        - get_table_schema
        - add_table
        - execute_query
        - list_tables
