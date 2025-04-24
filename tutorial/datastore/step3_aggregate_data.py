# Aggregate the data
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
