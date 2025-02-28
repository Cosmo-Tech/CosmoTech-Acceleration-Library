# Clean and transform the data
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
