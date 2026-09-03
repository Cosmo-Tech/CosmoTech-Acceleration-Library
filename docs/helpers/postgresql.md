---
description: "Helpers"
---
# **cosmotech.coal.postgresql**

::: cosmotech.coal.postgresql.runner
    options:
      members:
        - send_runner_metadata_to_postgresql
        - remove_runner_metadata_from_postgresql

<br/>

::: cosmotech.coal.postgresql.store
    options:
      members:
        - dump_store_to_postgresql
        - dump_store_to_postgresql_from_conf

<br/>

::: cosmotech.coal.postgresql.utils.PostgresUtils
    options:
      members:
        - table_prefix
        - db_name
        - db_schema
        - host_uri
        - host_port
        - user_name
        - user_password
        - password_encoding
        - full_uri
        - metadata_table_name
        - get_postgresql_table_schema
        - send_pyarrow_table_to_postgresql
        - add_fk_constraint
        - is_metadata_exists

<br/>

::: cosmotech.coal.postgresql.utils
    options:
      members:
        - adapt_table_to_schema
