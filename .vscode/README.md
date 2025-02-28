# VSCode Configuration for CosmoTech-Acceleration-Library

This directory contains VSCode-specific settings to enhance the development experience for this project.

## settings.json

The `settings.json` file configures VSCode to:

1. Use Black as the Python formatter
2. Format Python files automatically on save
3. Set the line length to 88 characters (Black's default)
4. Organize imports automatically on save

## Usage

These settings will be applied automatically when you open this project in VSCode. Make sure you have the following extensions installed:

- [Python](https://marketplace.visualstudio.com/items?itemName=ms-python.python)

## Installing Black

To use the Black formatter, you need to install it:

```bash
pip install -r requirements.dev.txt
```

Or install Black directly:

```bash
pip install black==23.3.0
