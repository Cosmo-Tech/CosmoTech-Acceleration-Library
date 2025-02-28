# Black Code Formatter Setup

This project uses [Black](https://github.com/psf/black) for code formatting to ensure consistent code style across the codebase.

## Configuration

Black is configured in the `pyproject.toml` file with the following settings:

```toml
[tool.black]
line-length = 120
target-version = ["py311"]
include = '\.pyi?$'
exclude = '''
/(
    \.git
  | \.hg
  | \.mypy_cache
  | \.tox
  | \.venv
  | _build
  | buck-out
  | build
  | dist
  | generated
  | __pycache__
)/
'''
```

## Installation

Install Black and other development tools:

```bash
pip install -r requirements.dev.txt
```

Or install Black directly:

```bash
pip install black==23.3.0
```

## Usage

### Manual Usage

Run Black on your codebase:

```bash
# Format all Python files in the project
python -m black .

# Format a specific directory
python -m black cosmotech/coal/

# Check if files would be reformatted without actually changing them
python -m black --check .

# Show diff of changes without writing files
python -m black --diff .
```

### Pre-commit Hooks

This project uses pre-commit hooks to automatically run Black before each commit. To set up pre-commit:

1. Install pre-commit:

```bash
pip install pre-commit
```

2. Install the git hooks:

```bash
pre-commit install
```

Now Black will run automatically on the files you've changed when you commit.

### VSCode Integration

If you use VSCode, the included settings in `.vscode/settings.json` will automatically format your code with Black when you save a file. Make sure you have the Python extension installed.

## CI Integration

To enforce Black formatting in your CI pipeline, add a step to run Black in check mode:

```yaml
- name: Check code formatting with Black
  run: |
    python -m pip install black==23.3.0
    python -m black --check .
```

## Migrating Existing Code

When first applying Black to an existing codebase, you might want to:

1. Run Black with `--diff` first to see the changes
2. Consider running it on one module at a time
3. Make the formatting change in a separate commit from functional changes

## Benefits of Using Black

- Eliminates debates about formatting
- Consistent code style across the entire codebase
- Minimal configuration needed
- Widely adopted in the Python community
