# Run all tests
pytest tests/unit/coal/mongodb/

# Run with coverage
pytest tests/unit/coal/mongodb/ --cov=cosmotech.coal.mongodb --cov-report=term-missing
