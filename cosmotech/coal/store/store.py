# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import pathlib

import pyarrow
from adbc_driver_sqlite import dbapi

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


class Store:
    @staticmethod
    def sanitize_column(column_name: str) -> str:
        return column_name.replace(" ", "_")

    def __init__(
        self,
        reset=False,
        store_location: pathlib.Path = pathlib.Path(os.environ.get("CSM_PARAMETERS_ABSOLUTE_PATH", ".")),
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
            raise ValueError(T("coal.errors.data.no_table").format(table_name=table_name))
        return self.execute_query(f"select * from {table_name}")

    def table_exists(self, table_name) -> bool:
        return table_name in self.list_tables()

    def get_table_schema(self, table_name: str) -> pyarrow.Schema:
        if not self.table_exists(table_name):
            raise ValueError(T("coal.errors.data.no_table").format(table_name=table_name))
        with dbapi.connect(self._database) as conn:
            return conn.adbc_get_table_schema(table_name)

    def add_table(self, table_name: str, data=pyarrow.Table, replace: bool = False):
        with dbapi.connect(self._database, autocommit=True) as conn:
            with conn.cursor() as curs:
                rows = curs.adbc_ingest(table_name, data, "replace" if replace else "create_append")
                LOGGER.debug(T("coal.common.data_transfer.rows_inserted").format(rows=rows, table_name=table_name))

    def execute_query(self, sql_query: str) -> pyarrow.Table:
        batch_size = 1024
        batch_size_increment = 1024
        while True:
            try:
                with dbapi.connect(self._database, autocommit=True) as conn:
                    with conn.cursor() as curs:
                        curs.adbc_statement.set_options(**{"adbc.sqlite.query.batch_rows": str(batch_size)})
                        curs.execute(sql_query)
                        return curs.fetch_arrow_table()
            except OSError:
                batch_size += batch_size_increment

    def list_tables(self) -> list[str]:
        with dbapi.connect(self._database) as conn:
            objects = conn.adbc_get_objects(depth="all").read_all()
            tables = objects["catalog_db_schemas"][0][0]["db_schema_tables"]
        for table in tables:
            table_name: pyarrow.StringScalar = table["table_name"]
            yield table_name.as_py()
