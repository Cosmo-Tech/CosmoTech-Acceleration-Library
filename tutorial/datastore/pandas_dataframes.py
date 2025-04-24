import pandas as pd
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.pandas import store_dataframe, convert_store_table_to_dataframe

# Initialize the store
store = Store(reset=True)

# Create a pandas DataFrame
df = pd.DataFrame(
    {
        "product_id": [1, 2, 3, 4, 5],
        "product_name": ["Widget A", "Widget B", "Gadget X", "Tool Y", "Device Z"],
        "price": [19.99, 29.99, 99.99, 49.99, 199.99],
        "category": ["Widgets", "Widgets", "Gadgets", "Tools", "Devices"],
    }
)

# Store the DataFrame
store_dataframe("products", df)

# Query the data
expensive_products = store.execute_query(
    """
    SELECT * FROM products
    WHERE price > 50
    ORDER BY price DESC
"""
)

# Convert results back to a pandas DataFrame for further analysis
expensive_df = convert_store_table_to_dataframe("expensive_products", store)

# Use pandas methods on the result
print(expensive_df.describe())
