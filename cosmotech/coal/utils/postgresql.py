# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from adbc_driver_postgresql import dbapi
from pyarrow import Table


def generate_postgresql_full_uri(
    postgres_host: str,
    postgres_port: str,
    postgres_db: str,
    postgres_user: str,
    postgres_password: str, ) -> str:
    return ('postgresql://' +
            f'{postgres_user}'
            f':{postgres_password}'
            f'@{postgres_host}'
            f':{postgres_port}'
            f'/{postgres_db}')


def send_pyarrow_table_to_postgresql(
    data: Table,
    target_table_name: str,
    postgres_host: str,
    postgres_port: str,
    postgres_db: str,
    postgres_schema: str,
    postgres_user: str,
    postgres_password: str,
    replace: bool
) -> int:
    total = 0

    postgresql_full_uri = generate_postgresql_full_uri(postgres_host,
                                                       postgres_port,
                                                       postgres_db,
                                                       postgres_user,
                                                       postgres_password)
    with dbapi.connect(postgresql_full_uri, autocommit=True) as conn:
        with conn.cursor() as curs:
            total += curs.adbc_ingest(
                target_table_name,
                data,
                "replace" if replace else "create_append",
                db_schema_name=postgres_schema)

    return total
