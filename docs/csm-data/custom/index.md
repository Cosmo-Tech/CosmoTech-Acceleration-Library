---
hide:
  - toc
description: "Command help: `csm-data custom`"
---
# custom

Optional, customized commands for special use cases. They do not follow the standard datastore design and need
the `custom` extra: `pip install "cosmotech-acceleration-library[custom]"`.

| Command | Use it to |
|---|---|
| [load-csv-file](load-csv-file.md) | Load one CSV file into an existing PostgreSQL table |
| [load-csv-folder](load-csv-folder.md) | Load every CSV file of a folder, one PostgreSQL table per file |

See [Custom Commands](../../tutorials/csm-data.md#custom-commands) for when to use them and an example.

!!! info "Help command"
    ```text
    --8<-- "generated/commands_help/csm-data/custom.txt"
    ```
