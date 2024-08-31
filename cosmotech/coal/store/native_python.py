import pyarrow as pa

from cosmotech.coal.store.store import Store


def store_pylist(table_name: str, data: list[dict], replace_existsing_file: bool = False):
    data = pa.Table.from_pylist(data)

    _s = Store()

    _s.add_table(table_name=table_name,
                 data=data,
                 replace=replace_existsing_file)


def convert_table_as_pylist(table_name: str):
    _s = Store()
    return _s.get_table(table_name).to_pylist()
