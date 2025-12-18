import pathlib

from cosmotech.coal.store.csv import convert_store_table_to_csv, store_csv_file
from cosmotech.coal.store.store import Store

# Initialize the store
store = Store(reset=True)

# Load data from a CSV file
csv_path = pathlib.Path("path/to/your/data.csv")
store_csv_file("customers", csv_path)

# Query the data
high_value_customers = store.execute_query(
    """
    SELECT * FROM customers
    WHERE annual_spend > 10000
    ORDER BY annual_spend DESC
"""
)

# Export results to a new CSV file
output_path = pathlib.Path("path/to/output/high_value_customers.csv")
convert_store_table_to_csv("high_value_customers", output_path)
