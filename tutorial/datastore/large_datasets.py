# Example of chunking data load
chunk_size = 10000
for i in range(0, len(large_dataset), chunk_size):
    chunk = large_dataset[i : i + chunk_size]
    store_pylist(f"data_chunk_{i//chunk_size}", chunk, store=store)

# Combine chunks with SQL
store.execute_query(
    """
    CREATE TABLE combined_data AS
    SELECT * FROM data_chunk_0
    UNION ALL
    SELECT * FROM data_chunk_1
    -- Add more chunks as needed
"""
)
