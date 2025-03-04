# Format all Python files in the project
python -m black .

# Format a specific directory
python -m black cosmotech/coal/

# Check if files would be reformatted without actually changing them
python -m black --check .

# Show diff of changes without writing files
python -m black --diff .
