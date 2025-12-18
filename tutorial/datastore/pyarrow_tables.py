import pyarrow as pa

from cosmotech.coal.store.pyarrow import store_table
from cosmotech.coal.store.store import Store

# Initialize the store
store = Store(reset=True)

# Create a PyArrow Table
data = {
    "date": pa.array(["2023-01-01", "2023-01-02", "2023-01-03"]),
    "value": pa.array([100, 150, 200]),
    "category": pa.array(["A", "B", "A"]),
}
table = pa.Table.from_pydict(data)

# Store the table
store_table("time_series", table)

# Query and retrieve data
result = store.execute_query(
    """
    SELECT date, SUM(value) as total_value
    FROM time_series
    GROUP BY date
"""
)

print(result)
