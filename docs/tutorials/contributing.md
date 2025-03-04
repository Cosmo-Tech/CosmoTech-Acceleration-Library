---
description: "Comprehensive guide to contributing to CoAL: from setting up your development environment to submitting a pull request"
---

# Contributing to CoAL

!!! abstract "Objective"
    + Set up your development environment with Black and pre-commit hooks
    + Understand the CoAL architecture and contribution workflow
    + Learn how to implement a new feature with a practical example
    + Master the process of writing unit tests and documentation
    + Successfully submit a pull request

## Introduction

Contributing to the CosmoTech Acceleration Library (CoAL) is a great way to enhance the platform's capabilities and share your expertise with the community. This tutorial will guide you through the entire process of contributing a new feature to CoAL, from setting up your development environment to submitting a pull request.

We'll use a practical example throughout this tutorial: implementing a new store write functionality for MongoDB and creating a corresponding csm-data command. This example will demonstrate all the key aspects of the contribution process, including:

- Setting up your development environment
- Understanding the CoAL architecture
- Implementing new functionality
- Creating CLI commands
- Writing unit tests
- Documenting your work
- Submitting a pull request

By the end of this tutorial, you'll have a solid understanding of how to contribute to CoAL and be ready to implement your own features.

## Setting Up Your Development Environment

Before you start contributing, you need to set up your development environment. This includes forking and cloning the repository, installing dependencies, and configuring code formatting tools.

### Forking and Cloning the Repository

1. Fork the CosmoTech-Acceleration-Library repository on GitHub
2. Clone your fork locally:

    ```bash
    --8<-- "tutorial/contributing/setup/clone_repo.bash"
    ```

3. Add the upstream repository as a remote:

    ```bash
    --8<-- "tutorial/contributing/setup/add_upstream.bash"
    ```

### Installing Dependencies

Install the package in development mode along with all development dependencies:

```bash
--8<-- "tutorial/contributing/setup/install_deps.bash"
```

This will install the package in editable mode, allowing you to make changes to the code without reinstalling it. It will also install all the development dependencies specified in the `pyproject.toml` file.

### Setting Up Black for Code Formatting

CoAL uses [Black](https://github.com/psf/black) for code formatting to ensure consistent code style across the codebase. Black is configured in the `pyproject.toml` file with specific settings for line length, target Python version, and file exclusions.

To manually run Black on your codebase:

```bash
--8<-- "tutorial/contributing/setup/black_commands.bash"
```

### Configuring Pre-commit Hooks

CoAL uses pre-commit hooks to automatically run checks before each commit, including Black formatting, trailing whitespace removal, and test coverage verification.

To install pre-commit:

```bash
--8<-- "tutorial/contributing/setup/precommit_setup.bash"
```

Now, when you commit changes, the pre-commit hooks will automatically run and check your code. If any issues are found, the commit will be aborted, and you'll need to fix the issues before committing again.

The pre-commit configuration includes:

- Trailing whitespace removal
- End-of-file fixer
- YAML syntax checking
- Black code formatting
- Pytest checks with coverage requirements
- Verification that all functions have tests

## Understanding the CoAL Architecture

Before implementing a new feature, it's important to understand the architecture of CoAL and how its components interact.

### Core Modules

CoAL is organized into several key modules:

- **coal**: The core library functionality
    + **store**: Data storage and retrieval
    + **cosmotech_api**: Interaction with the CosmoTech API
    + **aws**: AWS integration
    + **azure**: Azure integration
    + **postgresql**: PostgreSQL integration
    + **utils**: Utility functions
- **csm_data**: CLI commands for data operations
- **orchestrator_plugins**: Plugins for csm-orc
- **translation**: Translation resources

### Store Module Architecture

The store module provides a unified interface for data storage and retrieval. It's built around the `Store` class, which provides methods for:

- Adding and retrieving tables
- Executing SQL queries
- Listing available tables
- Resetting the store

The store module also includes adapters for different data formats:

- **native_python**: Python dictionaries and lists
- **csv**: CSV files
- **pandas**: Pandas DataFrames
- **pyarrow**: PyArrow Tables

External storage systems are implemented as separate modules that interact with the core `Store` class:

- **postgresql**: PostgreSQL integration
- **singlestore**: SingleStore integration

### CLI Command Structure

The `csm_data` CLI is organized into command groups, each focused on specific types of operations:

- **api**: Commands for interacting with the CosmoTech API
- **store**: Commands for working with the CoAL datastore
- **s3-bucket-\***: Commands for S3 bucket operations
- **adx-send-scenariodata**: Command for sending scenario data to Azure Data Explorer
- **az-storage-upload**: Command for uploading to Azure Storage

Each command is implemented as a separate Python file in the appropriate directory, using the Click library for command-line interface creation.

## Implementing a New Store Feature

Now that we understand the architecture, let's implement a new store feature: MongoDB integration. This will allow users to write data from the CoAL datastore to MongoDB.

### Creating the Module Structure

First, we'll create a new module for MongoDB integration:

```bash
--8<-- "tutorial/contributing/mongodb/module_structure.bash"
```

### Implementing the Core Functionality

Now, let's implement the core functionality in `cosmotech/coal/mongodb/store.py`:

```python
--8<-- "tutorial/contributing/mongodb/store.py"
```

### Updating the Package Initialization

Next, we need to update the `__init__.py` file to expose our new function:

```python
--8<-- "tutorial/contributing/mongodb/init.py"
```

### Adding Dependencies

We need to add pymongo as a dependency. Update the `pyproject.toml` file to include pymongo in the optional dependencies:

```toml
--8<-- "tutorial/contributing/mongodb/dependencies.toml"
```

## Creating a new CSM-DATA Command

Now that we have implemented the core functionality, let's create a new csm-data command to expose this functionality to users.

### Creating the Command File

Create a new file for the command:

```bash
--8<-- "tutorial/contributing/command/create_file.bash"
```

### Implementing the Command

Now, let's implement the command:

```python
--8<-- "tutorial/contributing/command/command.py"
```

### Registering the Command

Update the `cosmotech/csm_data/commands/store/__init__.py` file to register the new command:

```python
--8<-- "tutorial/contributing/command/register.py"
```

### Adding Translation Strings

Create translation files for the new command:

1. For English (en-US):

```bash
touch cosmotech/translation/csm_data/en-US/commands/store/dump_to_mongodb.yml
```

```yaml
--8<-- "tutorial/contributing/command/en_translation.yml"
```

2. For French (fr-FR):

```bash
touch cosmotech/translation/csm_data/fr-FR/commands/store/dump_to_mongodb.yml
```

```yaml
--8<-- "tutorial/contributing/command/fr_translation.yml"
```

## Writing Unit Tests

Testing is a critical part of the contribution process. All new functionality must be thoroughly tested to ensure it works as expected and to prevent regressions.

### Creating Test Files

Create test files for the new functionality:

```bash
--8<-- "tutorial/contributing/testing/create_test_files.bash"
```

### Implementing Unit Tests

Now, let's implement the unit tests for the MongoDB store functionality:

```python
--8<-- "tutorial/contributing/testing/store_test.py"
```


### Running the Tests

To run the tests, use pytest:

```bash
--8<-- "tutorial/contributing/testing/run_tests.bash"
```

Make sure all tests pass and that you have adequate code coverage (at least 80%).

## Documentation

Documentation is a critical part of the contribution process. All new features must be documented to ensure users can understand and use them effectively.

### Updating CLI Documentation

Let's add csm-data documentation for our new functionality. Create a new file:

```bash
--8<-- "tutorial/contributing/documentation/create_api_doc.bash"
```

Add the following content:

```markdown
--8<-- "tutorial/contributing/documentation/api_doc.md"
```

The documentation build system will generate the content that will be inserted in the file to add a minimal documentation. You can then add more elements as necessary.

## Pull Request Checklist

--8<-- "docs/pull_request.md:4"

## Conclusion

Congratulations! You've now learned how to contribute to CoAL by implementing a new feature, creating a new csm-data command, writing unit tests, and documenting your work.

By following this tutorial, you've gained practical experience with:

- Setting up your development environment with Black and pre-commit hooks
- Understanding the CoAL architecture
- Implementing new functionality
- Creating CLI commands
- Writing unit tests
- Documenting your work
- Preparing for a pull request

You're now ready to contribute your own features to CoAL and help improve the platform for everyone.

Remember that the CoAL community is here to help. If you have any questions or need assistance, don't hesitate to reach out through GitHub issues or discussions.

Happy contributing!
