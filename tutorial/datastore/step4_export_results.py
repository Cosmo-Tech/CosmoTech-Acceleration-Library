from cosmotech.coal.store.native_python import convert_table_as_pylist
from cosmotech.coal.store.csv import convert_store_table_to_csv
import pathlib

# Export to Python list
summary_data = convert_table_as_pylist("summary_data", store=store)
print(summary_data)

# Save to CSV for reporting
output_path = pathlib.Path("path/to/output/summary.csv")
convert_store_table_to_csv("summary_data", output_path, store=store)
