import pathlib

import pyarrow.csv as pc

from cosmotech.coal.store.store import Store


def store_csv_file(table_name: str, csv_path: pathlib.Path, replace_existsing_file: bool = False):
    if not csv_path.exists():
        raise FileNotFoundError(f"File {csv_path} does not exists")

    data = pc.read_csv(csv_path)

    _s = Store()

    _s.add_table(table_name=table_name,
                 data=data,
                 replace=replace_existsing_file)


def convert_store_table_to_csv(table_name: str, csv_path: pathlib.Path, replace_existsing_file: bool = False):
    if csv_path.name.endswith(".csv") and csv_path.exists() and not replace_existsing_file:
        raise FileExistsError(f"File {csv_path} already exists")
    if not csv_path.name.endswith(".csv"):
        csv_path = csv_path / f"{table_name}.csv"
    folder = csv_path.parent
    folder.mkdir(parents=True, exist_ok=True)

    _s = Store()
    pc.write_csv(_s.get_table(table_name), csv_path)
