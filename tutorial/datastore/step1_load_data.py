from cosmotech.coal.store.store import Store
from cosmotech.coal.store.csv import store_csv_file
import pathlib

# Initialize the store
store = Store(reset=True)

# Load raw data from CSV
raw_data_path = pathlib.Path("path/to/raw_data.csv")
store_csv_file("raw_data", raw_data_path, store=store)
