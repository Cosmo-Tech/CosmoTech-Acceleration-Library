# CosmoTech-Acceleration-Library (CoAL)

Acceleration library for CosmoTech cloud-based solution development.

## Introduction

The CosmoTech Acceleration Library (CoAL) provides a comprehensive set of tools and utilities to accelerate the development of solutions based on the CosmoTech platform. It offers a unified interface for interacting with CosmoTech APIs, managing data, and integrating with various cloud services.

## Main Components

### csm-data

`csm-data` is a powerful CLI tool designed to help CosmoTech solution modelers and integrators interact with multiple systems. It provides ready-to-use commands to send and retrieve data from various systems where a CosmoTech API could be integrated.

```bash
# Get help on available commands
csm-data --help

# Get help on specific command groups
csm-data api --help
```

### datastore

The datastore provides a way to maintain local data during simulations and comes with `csm-data` commands to easily send those data to target systems. It offers:

- Format flexibility (Python dictionaries, CSV files, Pandas DataFrames, PyArrow Tables)
- Persistent storage in SQLite
- SQL query capabilities
- Simplified data pipeline management

```python
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import store_pylist

# Initialize and reset the data store
my_datastore = Store(reset=True)

# Create and store data
my_data = [{"foo": "bar"}, {"foo": "barbar"}, {"foo": "world"}, {"foo": "bar"}]
store_pylist("my_data", my_data)

# Query the data
results = my_datastore.execute_query("SELECT foo, count(*) as line_count FROM my_data GROUP BY foo").to_pylist()
print(results)
# > [{'foo': 'bar', 'line_count': 2}, {'foo': 'barbar', 'line_count': 1}, {'foo': 'world', 'line_count': 1}]
```

### CosmoTech API Integration

CoAL provides comprehensive tools for interacting with the CosmoTech API, allowing you to:

- Authenticate with different identity providers (API Key, Azure Entra, Keycloak)
- Manage workspaces and files
- Work with the Twin Data Layer for graph data
- Handle runners and runs
- Process and transform data
- Build end-to-end workflows

```python
import os
from cosmotech.coal.cosmotech_api.connection import get_api_client

# Set up environment variables for authentication
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

# Get the API client
api_client, connection_type = get_api_client()
print(f"Connected using: {connection_type}")

# Use the client with various API instances
from cosmotech_api.api.organization_api import OrganizationApi
org_api = OrganizationApi(api_client)

# List organizations
organizations = org_api.find_all_organizations()
for org in organizations:
    print(f"Organization: {org.name} (ID: {org.id})")

# Don't forget to close the client when done
api_client.close()
```

### Other Components

- **coal**: Core library with modules for API interaction, data management, etc.
- **csm_data**: CLI tool for data management and integration with various systems
- **orchestrator_plugins**: Plugins that integrate with external orchestration systems
- **translation**: Internationalization support for multiple languages

## Getting Started

### Installation

```bash
pip install cosmotech-acceleration-library
```

### Basic Usage

Check out the tutorials directory for comprehensive examples of how to use the library:

- [CosmoTech API Integration](https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/tutorials/cosmotech-api/)
- [Data Store Usage](https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/tutorials/datastore/)
- [csm-data CLI](https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/tutorials/csm-data/)

## Key Features

### Cloud Service Integration

CoAL provides built-in support for various cloud services:

- **Azure**: Azure Data Explorer (ADX), Azure Storage, Azure Functions
- **AWS**: S3 buckets, and more
- **Database Systems**: PostgreSQL, SingleStore, and others

### Data Management

- Load and transform data from various sources
- Store and query data locally
- Export data to different formats and destinations
- Manage datasets in the CosmoTech platform

### Orchestration Integration

- Provides plugins that integrate with external orchestration systems
- Supports data transfer between orchestration steps
- Offers utilities for handling parameters and configurations
- Enables seamless integration with the CosmoTech platform during orchestrated workflows

## Documentation and Tutorials

Comprehensive documentation is available at [https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/](https://cosmo-tech.github.io/CosmoTech-Acceleration-Library/)

### Tutorials

- **CosmoTech API**: Learn how to interact with the CosmoTech API directly: authentication, workspaces, Twin Data Layer, and more.
- **Data Store**: The datastore is your friend to keep data between orchestration steps. It comes with multiple ways to interact with it.
- **csm-data**: Make full use of `csm-data` commands to connect to services during your orchestration runs.

## Testing and Code Coverage

The CosmoTech Acceleration Library maintains a comprehensive test suite to ensure reliability and stability. We use pytest for testing and pytest-cov for coverage reporting.

### Running Tests

To run the test suite:

```bash
# Install test dependencies
pip install -e ".[test]"

# Run tests with coverage reporting
pytest tests/unit/coal/ --cov=cosmotech.coal --cov-report=term-missing --cov-report=html
```

### Coverage Reports

After running tests with coverage, you can view detailed HTML reports:

```bash
# Open the HTML coverage report
open coverage_html_report/index.html
```

[![codecov](https://codecov.io/gh/Cosmo-Tech/CosmoTech-Acceleration-Library/branch/main/graph/badge.svg)](https://codecov.io/gh/Cosmo-Tech/CosmoTech-Acceleration-Library)

We maintain high test coverage to ensure code quality and reliability. All pull requests are expected to maintain or improve the current coverage levels.

### Test Generation Tools

To help maintain test coverage, we provide tools to identify untested functions and generate test files:

```bash
# Find functions without tests
python find_untested_functions.py

# Generate test files for a specific module
python generate_test_files.py --module cosmotech/coal/module/file.py

# Generate test files for all untested functions
python generate_test_files.py --all
```

These tools help ensure that every function has at least one test, which is a requirement for contributions to the project.

## Contact

For support, feature requests, or contributions, please use the [GitHub repository](https://github.com/Cosmo-Tech/CosmoTech-Acceleration-Library).
