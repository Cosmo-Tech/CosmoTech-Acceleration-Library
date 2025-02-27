---
description: "Comprehensive guide to the CoAL data store: a powerful data management solution"
---

# Datastore

!!! abstract "Objective"
    + Understand what the CoAL datastore is and its capabilities
    + Learn how to store and retrieve data in various formats
    + Master SQL querying capabilities for data analysis
    + Build efficient data processing pipelines

## What is the datastore?

The datastore is a powerful data management abstraction that provides a unified interface to a SQLite database. It allows you to store, retrieve, transform, and query tabular data in various formats through a consistent API.

The core idea behind the datastore is to provide a robust, flexible system for data management that simplifies working with different data formats while offering persistence and advanced query capabilities.

!!! info "Key Features"
    - Format flexibility (Python dictionaries, CSV files, Pandas DataFrames, PyArrow Tables)
    - Persistent storage in SQLite
    - SQL query capabilities
    - Simplified data pipeline management

## Why use the datastore?

### Format Flexibility

The datastore works seamlessly with multiple data formats:

- Python dictionaries and lists
- CSV files
- Pandas DataFrames
- PyArrow Tables

This flexibility eliminates the need for manual format conversions and allows you to work with data in your preferred format.

### Persistence and Performance

Instead of keeping all your data in memory or writing/reading from files repeatedly, the datastore:

- Persists data in a SQLite database
- Provides efficient storage and retrieval
- Handles large datasets that might not fit in memory
- Maintains data between application runs

### SQL Query Capabilities

The datastore leverages the power of SQL:

- Filter, aggregate, join, and transform data using familiar SQL syntax
- Execute complex queries without writing custom data manipulation code
- Perform operations that would be cumbersome with file-based approaches

### Simplified Data Pipeline

The datastore serves as a central hub in your data processing pipeline:

- Import data from various sources
- Transform and clean data
- Query and analyze data
- Export results in different formats

## Basic example

```python title="Basic use of the datastore" linenums="1"
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import store_pylist

# We initialize and reset the data store
my_datastore = Store(reset=True)

# We create a simple list of dict data
my_data = [{
    "foo": "bar"
},{
    "foo": "barbar"
},{
    "foo": "world"
},{
    "foo": "bar"
}]

# We use a bundled method to send the py_list to the store
store_pylist("my_data", my_data)

# We can make a sql query over our data
# Store.execute_query returns a pyarrow.Table object so we can make use of Table.to_pylist to get an equivalent format
results = my_datastore.execute_query("SELECT foo, count(*) as line_count FROM my_data GROUP BY foo").to_pylist()

# We can print our results now
print(results)
# > [{'foo': 'bar', 'line_count': 2}, {'foo': 'barbar', 'line_count': 1}, {'foo': 'world', 'line_count': 1}]
```

## Working with different data formats

The datastore provides specialized adapters for working with various data formats:

### CSV Files

```python title="Working with CSV files" linenums="1"
import pathlib
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.csv import store_csv_file, convert_store_table_to_csv

# Initialize the store
store = Store(reset=True)

# Load data from a CSV file
csv_path = pathlib.Path("path/to/your/data.csv")
store_csv_file("customers", csv_path)

# Query the data
high_value_customers = store.execute_query("""
    SELECT * FROM customers 
    WHERE annual_spend > 10000
    ORDER BY annual_spend DESC
""")

# Export results to a new CSV file
output_path = pathlib.Path("path/to/output/high_value_customers.csv")
convert_store_table_to_csv("high_value_customers", output_path)
```

### Pandas DataFrames

```python title="Working with pandas DataFrames" linenums="1"
import pandas as pd
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.pandas import store_dataframe, convert_store_table_to_dataframe

# Initialize the store
store = Store(reset=True)

# Create a pandas DataFrame
df = pd.DataFrame({
    'product_id': [1, 2, 3, 4, 5],
    'product_name': ['Widget A', 'Widget B', 'Gadget X', 'Tool Y', 'Device Z'],
    'price': [19.99, 29.99, 99.99, 49.99, 199.99],
    'category': ['Widgets', 'Widgets', 'Gadgets', 'Tools', 'Devices']
})

# Store the DataFrame
store_dataframe("products", df)

# Query the data
expensive_products = store.execute_query("""
    SELECT * FROM products
    WHERE price > 50
    ORDER BY price DESC
""")

# Convert results back to a pandas DataFrame for further analysis
expensive_df = convert_store_table_to_dataframe("expensive_products", store)

# Use pandas methods on the result
print(expensive_df.describe())
```

### PyArrow Tables

```python title="Working with PyArrow Tables directly" linenums="1"
import pyarrow as pa
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.pyarrow import store_table

# Initialize the store
store = Store(reset=True)

# Create a PyArrow Table
data = {
    'date': pa.array(['2023-01-01', '2023-01-02', '2023-01-03']),
    'value': pa.array([100, 150, 200]),
    'category': pa.array(['A', 'B', 'A'])
}
table = pa.Table.from_pydict(data)

# Store the table
store_table("time_series", table)

# Query and retrieve data
result = store.execute_query("""
    SELECT date, SUM(value) as total_value
    FROM time_series
    GROUP BY date
""")

print(result)
```

## Advanced use cases

### Joining multiple tables

```python title="Joining tables in the datastore" linenums="1" 
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import store_pylist

store = Store(reset=True)

# Store customer data
customers = [
    {"customer_id": 1, "name": "Acme Corp", "segment": "Enterprise"},
    {"customer_id": 2, "name": "Small Shop", "segment": "SMB"},
    {"customer_id": 3, "name": "Tech Giant", "segment": "Enterprise"}
]
store_pylist("customers", customers, store=store)

# Store order data
orders = [
    {"order_id": 101, "customer_id": 1, "amount": 5000},
    {"order_id": 102, "customer_id": 2, "amount": 500},
    {"order_id": 103, "customer_id": 1, "amount": 7500},
    {"order_id": 104, "customer_id": 3, "amount": 10000}
]
store_pylist("orders", orders, store=store)

# Join tables to analyze orders by customer segment
results = store.execute_query("""
    SELECT c.segment, COUNT(o.order_id) as order_count, SUM(o.amount) as total_revenue
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.segment
""").to_pylist()

print(results)
# > [{'segment': 'Enterprise', 'order_count': 3, 'total_revenue': 22500}, {'segment': 'SMB', 'order_count': 1, 'total_revenue': 500}]
```

### Data transformation pipeline

=== "Complete pipeline"
    ```python title="Building a data transformation pipeline" linenums="1"
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
    store.execute_query("""
        CREATE TABLE cleaned_data AS
        SELECT 
            id,
            TRIM(name) as name,
            UPPER(category) as category,
            CASE WHEN value < 0 THEN 0 ELSE value END as value
        FROM raw_data
        WHERE id IS NOT NULL
    """)

    # 3. Aggregate the data
    store.execute_query("""
        CREATE TABLE summary_data AS
        SELECT
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            SUM(value) as total_value
        FROM cleaned_data
        GROUP BY category
    """)

    # 4. Export the results
    summary_data = convert_table_as_pylist("summary_data", store=store)
    print(summary_data)

    # 5. Save to CSV for reporting
    output_path = pathlib.Path("path/to/output/summary.csv")
    convert_store_table_to_csv("summary_data", output_path, store=store)
    ```

=== "Step-by-step"
    ```python title="Step 1: Load data" linenums="1"
    from cosmotech.coal.store.store import Store
    from cosmotech.coal.store.csv import store_csv_file
    import pathlib

    # Initialize the store
    store = Store(reset=True)

    # Load raw data from CSV
    raw_data_path = pathlib.Path("path/to/raw_data.csv")
    store_csv_file("raw_data", raw_data_path, store=store)
    ```

    ```python title="Step 2: Clean data" linenums="1"
    # Clean and transform the data
    store.execute_query("""
        CREATE TABLE cleaned_data AS
        SELECT 
            id,
            TRIM(name) as name,
            UPPER(category) as category,
            CASE WHEN value < 0 THEN 0 ELSE value END as value
        FROM raw_data
        WHERE id IS NOT NULL
    """)
    ```

    ```python title="Step 3: Aggregate data" linenums="1"
    # Aggregate the data
    store.execute_query("""
        CREATE TABLE summary_data AS
        SELECT
            category,
            COUNT(*) as count,
            AVG(value) as avg_value,
            SUM(value) as total_value
        FROM cleaned_data
        GROUP BY category
    """)
    ```

    ```python title="Step 4: Export results" linenums="1"
    from cosmotech.coal.store.native_python import convert_table_as_pylist
    from cosmotech.coal.store.csv import convert_store_table_to_csv
    import pathlib

    # Export to Python list
    summary_data = convert_table_as_pylist("summary_data", store=store)
    print(summary_data)

    # Save to CSV for reporting
    output_path = pathlib.Path("path/to/output/summary.csv")
    convert_store_table_to_csv("summary_data", output_path, store=store)
    ```

## Best practices and tips

!!! tip "Store initialization"
    - Use `reset=True` when you want to start with a fresh database
    - Omit the reset parameter or set it to `False` when you want to maintain data between runs
    - Specify a custom location with the `store_location` parameter if needed

```python title="Store initialization options" linenums="1"
# Fresh store each time
store = Store(reset=True)

# Persistent store at default location
store = Store()

# Persistent store at custom location
import pathlib
custom_path = pathlib.Path("/path/to/custom/location")
store = Store(store_location=custom_path)
```

!!! tip "Table management"
    - Use descriptive table names that reflect the data content
    - Check if tables exist before attempting operations
    - List available tables to explore the database

```python title="Table management" linenums="1"
# Check if a table exists
if store.table_exists("customers"):
    # Do something with the table
    pass

# List all tables
for table_name in store.list_tables():
    print(f"Table: {table_name}")
    # Get schema information
    schema = store.get_table_schema(table_name)
    print(f"Schema: {schema}")
```

!!! warning "Performance considerations"
    - For large datasets, consider chunking data when loading
    - Use SQL to filter data early rather than loading everything into memory
    - Index frequently queried columns for better performance

```python title="Handling large datasets" linenums="1"
# Example of chunking data load
chunk_size = 10000
for i in range(0, len(large_dataset), chunk_size):
    chunk = large_dataset[i:i+chunk_size]
    store_pylist(f"data_chunk_{i//chunk_size}", chunk, store=store)

# Combine chunks with SQL
store.execute_query("""
    CREATE TABLE combined_data AS
    SELECT * FROM data_chunk_0
    UNION ALL
    SELECT * FROM data_chunk_1
    -- Add more chunks as needed
""")
```

## Integration with CosmoTech ecosystem

The datastore is designed to work seamlessly with other components of the CosmoTech Acceleration Library:

- **Data loading**: Load data from various sources into the datastore
- **Scenario management**: Store scenario parameters and results
- **API integration**: Exchange data with CosmoTech APIs
- **Reporting**: Generate reports and visualizations from stored data

This integration makes the datastore a central component in CosmoTech-based data processing workflows.

## Conclusion

The datastore provides a powerful, flexible foundation for data management in your CosmoTech applications. By leveraging its capabilities, you can:

- Simplify data handling across different formats
- Build robust data processing pipelines
- Perform complex data transformations and analyses
- Maintain data persistence between application runs
- Integrate seamlessly with other CosmoTech components

Whether you're working with small datasets or large-scale data processing tasks, the datastore offers the tools you need to manage your data effectively.
