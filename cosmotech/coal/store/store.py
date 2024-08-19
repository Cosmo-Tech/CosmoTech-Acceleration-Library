import os
import pathlib

import pyarrow
import pyarrow.parquet as pq


class Store:
    _instance = None

    def __new__(class_, *args, **kwargs):
        if not isinstance(class_._instance, class_):
            class_._instance = object.__new__(class_, *args, **kwargs)
        return class_._instance

    def __init__(self):
        self.store_location = pathlib.Path(os.environ.get("CSM_PARAMETERS_ABSOLUTE_PATH", ".coal")) / "store"
        self.store_location.mkdir(parents=True, exist_ok=True)
        self._tables = dict()
        for existing_path in self.store_location.glob("*.parquet"):
            table_name = existing_path.name.split('.')[0]
            self._tables[table_name] = existing_path

    def get_table(self, table_name: str) -> pyarrow.Table:
        if table_name not in self._tables:
            raise FileNotFoundError(f"No table with name {table_name} exists")
        return pq.read_table(self._tables[table_name]).to_pylist()

    def add_table(self, table_name: str, data=pyarrow.Table, replace: bool = False) -> pathlib.Path:
        if table_name in self._tables and not replace:
            raise FileExistsError(f"Table {table_name} already exists, consider using replace to replace is")
        table_path = self.store_location / f"{table_name}.parquet"
        pq.write_table(data, table_path)
        return table_path

    def list_tables(self) -> list[str]:
        for table_name in self._tables.keys():
            yield table_name
