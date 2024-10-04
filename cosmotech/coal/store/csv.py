import pathlib

import pyarrow.csv as pc

from cosmotech.coal.store.store import Store


def store_csv_file(
    table_name: str,
    csv_path: pathlib.Path,
    replace_existsing_file: bool = False,
    store=Store()
):
    if not csv_path.exists():
        raise FileNotFoundError(f"File {csv_path} does not exists")

    data = pc.read_csv(csv_path)
    _c = data.column_names
    data = data.rename_columns([Store.sanitize_column(_column) for _column in _c])

    store.add_table(table_name=table_name,
                    data=data,
                    replace=replace_existsing_file)


def convert_store_table_to_csv(
    table_name: str,
    csv_path: pathlib.Path,
    replace_existsing_file: bool = False,
    store=Store()
):
    if csv_path.name.endswith(".csv") and csv_path.exists() and not replace_existsing_file:
        raise FileExistsError(f"File {csv_path} already exists")
    if not csv_path.name.endswith(".csv"):
        csv_path = csv_path / f"{table_name}.csv"
    folder = csv_path.parent
    folder.mkdir(parents=True, exist_ok=True)

    pc.write_csv(store.get_table(table_name), csv_path)
