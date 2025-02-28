from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import store_pylist, convert_table_as_pylist
import pathlib
from cosmotech.coal.store.csv import store_csv_file, convert_store_table_to_csv

# Initialize the store
store = Store(reset=True)

# 1. Load raw data from CSV
raw_data_path = pathlib.Path("path/to/raw_data.csv")
store_csv_file("raw_data", raw_data_path, store=store)

# 2. Clean and transform the data
store.execute_query(
    """
    CREATE TABLE cleaned_data AS
    SELECT 
        id,
        TRIM(name) as name,
        UPPER(category) as category,
        CASE WHEN value < 0 THEN 0 ELSE value END as value
    FROM raw_data
    WHERE id IS NOT NULL
"""
)

# 3. Aggregate the data
store.execute_query(
    """
    CREATE TABLE summary_data AS
    SELECT
        category,
        COUNT(*) as count,
        AVG(value) as avg_value,
        SUM(value) as total_value
    FROM cleaned_data
    GROUP BY category
"""
)

# 4. Export the results
summary_data = convert_table_as_pylist("summary_data", store=store)
print(summary_data)

# 5. Save to CSV for reporting
output_path = pathlib.Path("path/to/output/summary.csv")
convert_store_table_to_csv("summary_data", output_path, store=store)
