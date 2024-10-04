import os
import pathlib

import pyarrow
from adbc_driver_sqlite import dbapi

from cosmotech.coal.utils.logger import LOGGER


class Store:

    @staticmethod
    def sanitize_column(column_name: str) -> str:
        return column_name.replace(" ", "_").lower()

    def __init__(
        self,
        reset=False,
        store_location: pathlib.Path = pathlib.Path(os.environ.get("CSM_PARAMETERS_ABSOLUTE_PATH",
                                                                   "."))
    ):
        self.store_location = pathlib.Path(store_location) / ".coal/store"
        self.store_location.mkdir(parents=True, exist_ok=True)
        self._tables = dict()
        self._database_path = self.store_location / "db.sqlite"
        if reset:
            self.reset()
        self._database = str(self._database_path)

    def reset(self):
        if self._database_path.exists():
            self._database_path.unlink()

    def get_table(self, table_name: str) -> pyarrow.Table:
        if not self.table_exists(table_name):
            raise ValueError(f"No table with name {table_name} exists")
        return self.execute_query(f"select * from {table_name}")

    def table_exists(self, table_name) -> bool:
        return table_name in self.list_tables()

    def get_table_schema(self, table_name: str) -> pyarrow.Schema:
        if not self.table_exists(table_name):
            raise ValueError(f"No table with name {table_name} exists")
        with dbapi.connect(self._database) as conn:
            return conn.adbc_get_table_schema(table_name)

    def add_table(self, table_name: str, data=pyarrow.Table, replace: bool = False):
        with dbapi.connect(self._database, autocommit=True) as conn:
            with conn.cursor() as curs:
                rows = curs.adbc_ingest(table_name, data, "replace" if replace else "create_append")
                LOGGER.debug(f"Inserted {rows} rows in table {table_name}")

    def execute_query(self, sql_query: str) -> pyarrow.Table:
        with dbapi.connect(self._database, autocommit=True) as conn:
            with conn.cursor() as curs:
                curs.execute(sql_query)
                return curs.fetch_arrow_table()

    def list_tables(self) -> list[str]:
        with dbapi.connect(self._database) as conn:
            objects = conn.adbc_get_objects(depth="all").read_all()
            tables = objects["catalog_db_schemas"][0][0]["db_schema_tables"]
        for table in tables:
            table_name: pyarrow.StringScalar = table["table_name"]
            yield table_name.as_py()
