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
