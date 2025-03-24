# Contributing to CosmoTech-Acceleration-Library

Thank you for your interest in contributing to the CosmoTech Acceleration Library! This document provides guidelines and instructions for contributing to this project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Testing Requirements](#testing-requirements)
- [Documentation Guidelines](#documentation-guidelines)
- [Pull Request Process](#pull-request-process)
- [Style Guide](#style-guide)

## Code of Conduct

We are committed to providing a friendly, safe, and welcoming environment for all contributors. We expect everyone to be respectful and considerate of others.

## Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally
3. Set up the development environment:
   ```bash
   pip install -e ".[dev]"
   pre-commit install
   ```

## Development Workflow

1. Create a new branch for your feature or bugfix
2. Make your changes
3. Run tests and ensure code coverage is maintained or improved
4. Update documentation as needed
5. Submit a pull request

## Testing Requirements

### Test Coverage

**All contributions to the `cosmotech.coal` module must include appropriate test coverage.** This is a strict requirement to maintain code quality and reliability.

- Write unit tests for all new functionality
- Ensure existing tests pass with your changes
- Maintain or improve the current code coverage percentage
- Use mocking for external services to ensure tests are reliable and fast

### Running Tests

```bash
# Run tests with coverage reporting
pytest tests/unit/coal/ --cov=cosmotech.coal --cov-report=term-missing --cov-report=html
```

### Test Structure

- Place tests in the appropriate subdirectory under `tests/unit/coal/`
- Follow the naming convention `test_module_file.py` to ensure unique test file names
- Use fixtures from `conftest.py` where appropriate
- Mock external dependencies to ensure tests are isolated

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

These tools help ensure that every function has at least one test. When using the generated test files:

1. Verify that the functions actually exist in the module (the generator tries to check this, but may miss some cases)
2. Implement the test logic by replacing the `pass` statements with actual test code
3. Use mocking for external dependencies to ensure tests are isolated

### Coverage Requirements

- New code should aim for at least 80% coverage
- Critical components should have close to 100% coverage
- Use `# pragma: no cover` sparingly and only for code that genuinely cannot be tested
- **Every function must have at least one test** - this is a strict requirement to ensure basic functionality is tested

## Documentation Guidelines

**All new features must be documented.** This includes:

1. **Docstrings**: All public functions, classes, and methods must have clear docstrings following the existing format
2. **Examples**: Include usage examples where appropriate
3. **Tutorials**: For significant features, consider adding a tutorial in the `tutorial/` directory
4. **API Documentation**: Update API documentation if your changes affect the public API

## Pull Request Process

1. Ensure all tests pass and coverage requirements are met
2. Update documentation as needed
3. Write a clear and descriptive pull request description that:
   - Explains the purpose of the changes
   - Describes how the changes address the issue
   - Lists any dependencies that were added or modified
   - Mentions any breaking changes
4. Reference any related issues using the GitHub issue reference syntax
5. Wait for code review and address any feedback

## Style Guide

- Follow the existing code style (we use Black for formatting)
- Run pre-commit hooks before committing to ensure style consistency
- Use meaningful variable and function names
- Keep functions focused on a single responsibility
- Write clear comments for complex logic

## Commit Messages

Write clear, concise commit messages that explain the "why" behind changes. Follow this format:

```
[Component] Short summary of changes (50 chars or less)

More detailed explanation if needed. Wrap lines at 72 characters.
Explain the problem that this commit is solving and why you're solving
it this way.

Fixes #123
```

## Mocking External Services

When writing tests for code that interacts with external services (AWS, Azure, CosmoTech API, etc.), always use mocks to ensure tests are:

1. **Fast**: Tests should run quickly without waiting for external services
2. **Reliable**: Tests should not fail due to network issues or service unavailability
3. **Isolated**: Tests should not depend on external state or configuration
4. **Repeatable**: Tests should produce the same results every time they run

Example of mocking an external service:

```python
@patch('boto3.client')
def test_s3_upload(mock_client):
    # Set up the mock
    mock_s3 = MagicMock()
    mock_client.return_value = mock_s3

    # Test the function
    result = upload_to_s3('file.txt', 'bucket-name')

    # Verify the mock was called correctly
    mock_s3.upload_file.assert_called_once_with('file.txt', 'bucket-name', 'file.txt')
```

Thank you for contributing to the CosmoTech Acceleration Library!
