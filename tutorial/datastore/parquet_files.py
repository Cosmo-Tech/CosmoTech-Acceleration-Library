import pathlib

import pyarrow.parquet as pq

from cosmotech.coal.store.parquet import convert_store_table_to_parquet, store_parquet_file
from cosmotech.coal.store.store import Store

# Initialize the store
store = Store(reset=True)

# --- Loading a Parquet file into the store ---

# Create a sample parquet file for demonstration
sample_dir = pathlib.Path("./parquet_example")
sample_dir.mkdir(exist_ok=True, parents=True)
sample_file = sample_dir / "sales.parquet"

import pyarrow as pa

table = pa.table(
    {
        "region": ["North", "South", "East", "West"],
        "product": ["Widget", "Gadget", "Widget", "Gadget"],
        "units": [120, 85, 200, 60],
        "revenue": [2400.0, 1275.0, 4000.0, 900.0],
    }
)
pq.write_table(table, sample_file)

# Load the parquet file into the store under the table name "sales"
store_parquet_file("sales", sample_file, store=store)

# Query the loaded data
result = store.execute_query(
    """
    SELECT product, SUM(units) AS total_units, SUM(revenue) AS total_revenue
    FROM sales
    GROUP BY product
    ORDER BY total_revenue DESC
"""
)
print(result)

# --- Exporting a store table back to Parquet ---

output_dir = pathlib.Path("./parquet_output")
output_dir.mkdir(exist_ok=True, parents=True)

# Write the "sales" table from the store to a parquet file
convert_store_table_to_parquet("sales", output_dir / "sales.parquet", store=store)
print(f"Exported store table 'sales' to {output_dir / 'sales.parquet'}")
