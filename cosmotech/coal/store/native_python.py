import pyarrow as pa

from cosmotech.coal.store.store import Store


def store_pylist(
    table_name: str,
    data: list[dict],
    replace_existsing_file:
    bool = False,
    store=Store()
):
    data = pa.Table.from_pylist(data)

    store.add_table(table_name=table_name,
                    data=data,
                    replace=replace_existsing_file)


def convert_table_as_pylist(
    table_name: str,
    store=Store()
):
    return store.get_table(table_name).to_pylist()
