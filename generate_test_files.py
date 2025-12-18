#!/usr/bin/env python3
"""
Script to generate test files for untested functions in cosmotech/coal/.

This script identifies functions in the cosmotech/coal/ module that don't have
corresponding tests and generates test files for them based on a template.
"""

import argparse
import ast
import os
import re
from collections import defaultdict
from pathlib import Path


def get_functions_from_file(file_path):
    """Extract all function and class definitions from a Python file."""
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            tree = ast.parse(f.read())
        except SyntaxError:
            print(f"Syntax error in {file_path}")
            return []

    functions = []

    # Get top-level functions
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.FunctionDef):
            # Skip private functions (starting with _)
            if not node.name.startswith("_"):
                functions.append(node.name)
        elif isinstance(node, ast.ClassDef):
            # Get class methods
            for class_node in ast.iter_child_nodes(node):
                if isinstance(class_node, ast.FunctionDef):
                    # Skip private methods (starting with _)
                    if not class_node.name.startswith("_"):
                        functions.append(f"{node.name}.{class_node.name}")

    return functions


def get_tests_from_file(file_path):
    """Extract all test functions from a Python test file."""
    with open(file_path, "r", encoding="utf-8") as f:
        try:
            content = f.read()
            tree = ast.parse(content)
        except SyntaxError:
            print(f"Syntax error in {file_path}")
            return []

    tests = []

    # Look for test functions (test_*)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
            tests.append(node.name)

    # Also look for function names in the content (for parameterized tests)
    function_pattern = r"def\s+(test_\w+)"
    tests.extend(re.findall(function_pattern, content))

    return list(set(tests))


def map_module_to_test_file(module_path):
    """Map a module path to its corresponding test file path."""
    # Convert module path to test path
    # e.g., cosmotech/coal/aws/s3.py -> tests/unit/coal/test_aws/test_aws_s3.py
    parts = module_path.parts
    if len(parts) < 3 or parts[0] != "cosmotech" or parts[1] != "coal":
        return None

    # Skip __init__.py files
    if parts[-1] == "__init__.py":
        return None

    # Get the module name without extension
    module_name = parts[-1].replace(".py", "")

    # Construct the test file path
    test_dir = Path("tests/unit/coal")
    module_parts = parts[2:-1]  # Skip cosmotech/coal and the file name

    # Create directory structure
    for part in module_parts:
        test_dir = test_dir / f"test_{part}"

    # Create test file name with module path included
    # For example, for cosmotech/coal/azure/adx/ingestion.py, the test file would be test_adx_ingestion.py
    # For cosmotech/coal/azure/blob.py, the test file would be test_azure_blob.py
    if module_parts:
        test_file_name = f"test_{module_parts[-1]}_{module_name}.py"
    else:
        test_file_name = f"test_{module_name}.py"

    test_file = test_dir / test_file_name
    return test_file


def find_untested_functions():
    """Find functions in cosmotech/coal/ that don't have corresponding tests."""
    coal_dir = Path("cosmotech/coal")

    # Dictionary to store functions by module
    module_functions = {}

    # Dictionary to store tests by module
    module_tests = defaultdict(list)

    # Find all Python files in cosmotech/coal/
    for root, _, files in os.walk(coal_dir):
        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                module_path = file_path.relative_to(".")

                # Skip __init__.py files
                if file == "__init__.py":
                    continue

                # Get functions from the module
                functions = get_functions_from_file(file_path)
                if functions:
                    module_functions[module_path] = functions

    # Find all test files in tests/unit/coal/
    test_dir = Path("tests/unit/coal")
    if test_dir.exists():
        for root, _, files in os.walk(test_dir):
            for file in files:
                if file.startswith("test_") and file.endswith(".py"):
                    test_file_path = Path(root) / file
                    tests = get_tests_from_file(test_file_path)
                    module_tests[test_file_path] = tests

    # Check which functions don't have tests
    untested_functions = {}

    for module_path, functions in module_functions.items():
        test_file = map_module_to_test_file(module_path)

        if test_file is None:
            # Skip modules that don't map to a test file
            continue

        if not test_file.exists():
            # If the test file doesn't exist, all functions are untested
            untested_functions[module_path] = functions
            continue

        # Get tests for this module
        tests = module_tests.get(test_file, [])

        # Check which functions don't have corresponding tests
        untested = []
        for func in functions:
            # Check if there's a test for this function
            has_test = False
            for test in tests:
                # Look for test_function_name or test_class_function_name
                # Also check for test patterns like test_class_method_name_additional_info
                # For class methods, also check for test_method_name (without the class name)
                if (
                    test == f"test_{func}"
                    or test == f"test_{func.replace('.', '_')}"
                    or test.startswith(f"test_{func}_")
                    or test.startswith(f"test_{func.replace('.', '_')}_")
                ):
                    has_test = True
                    break

                # Special case for class methods: check if there's a test for just the method name
                if "." in func:
                    class_name, method_name = func.split(".")
                    if test == f"test_{method_name}" or test.startswith(f"test_{method_name}_"):
                        has_test = True
                        break

            if not has_test:
                untested.append(func)

        if untested:
            untested_functions[module_path] = untested

    return untested_functions


def generate_test_file(module_path, functions, overwrite=False):
    """Generate a test file for the given module and functions."""
    # Get the test file path
    test_file = map_module_to_test_file(module_path)
    if test_file is None:
        print(f"Could not map {module_path} to a test file")
        return

    # Create the test directory if it doesn't exist
    test_file.parent.mkdir(parents=True, exist_ok=True)

    # Get the module name and import path
    module_name = module_path.stem
    import_path = (
        f"cosmotech.coal.{'.'.join(module_path.parts[2:-1])}.{module_name}"
        if len(module_path.parts) > 3
        else f"cosmotech.coal.{module_name}"
    )

    # Verify that the functions actually exist in the module
    verified_functions = []
    try:
        module = __import__(import_path, fromlist=["*"])
        for func in functions:
            if "." in func:
                class_name, method_name = func.split(".")
                if hasattr(module, class_name) and hasattr(getattr(module, class_name), method_name):
                    verified_functions.append(func)
                else:
                    print(f"Warning: Function {func} not found in {import_path}")
            else:
                if hasattr(module, func):
                    verified_functions.append(func)
                else:
                    print(f"Warning: Function {func} not found in {import_path}")
    except ImportError as e:
        print(f"Warning: Could not import {import_path}: {e}")
        verified_functions = functions  # Fall back to using all functions

    # If the test file already exists, read it and extract existing tests
    existing_content = ""
    existing_imports = []
    existing_test_classes = {}
    existing_test_functions = []

    if test_file.exists():
        with open(test_file, "r", encoding="utf-8") as f:
            existing_content = f.read()

        # Parse the existing file to extract imports, test classes, and test functions
        try:
            tree = ast.parse(existing_content)

            # Extract imports
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.Import) or isinstance(node, ast.ImportFrom):
                    import_lines = existing_content.splitlines()[node.lineno - 1 : node.end_lineno]
                    existing_imports.extend(import_lines)

            # Extract test classes and their methods
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.ClassDef) and node.name.startswith("Test"):
                    class_lines = existing_content.splitlines()[node.lineno - 1 : node.end_lineno]
                    class_content = "\n".join(class_lines)
                    existing_test_classes[node.name] = {"content": class_content, "methods": []}

                    # Extract test methods
                    for method_node in ast.iter_child_nodes(node):
                        if isinstance(method_node, ast.FunctionDef) and method_node.name.startswith("test_"):
                            existing_test_classes[node.name]["methods"].append(method_node.name)

            # Extract standalone test functions
            for node in ast.iter_child_nodes(tree):
                if isinstance(node, ast.FunctionDef) and node.name.startswith("test_"):
                    existing_test_functions.append(node.name)

        except SyntaxError:
            print(f"Warning: Could not parse existing test file {test_file}")
            # If we can't parse the file, we'll just append our new tests to it

    # If the file exists and we're not overwriting, check if we need to add any tests
    if test_file.exists() and not overwrite:
        # Check if all functions already have tests
        all_tested = True
        for func in verified_functions:
            if "." in func:
                class_name, method_name = func.split(".")
                test_class_name = f"Test{class_name}"
                test_method_name = f"test_{method_name}"

                # Check if the test class exists and has a test for this method
                if (
                    test_class_name not in existing_test_classes
                    or test_method_name not in existing_test_classes[test_class_name]["methods"]
                ):
                    all_tested = False
                    break
            else:
                test_func_name = f"test_{func}"
                test_class_name = f"Test{module_name.capitalize()}Functions"

                # Check if there's a standalone test function or a method in a test class
                if test_func_name not in existing_test_functions and (
                    test_class_name not in existing_test_classes
                    or test_func_name not in existing_test_classes[test_class_name]["methods"]
                ):
                    all_tested = False
                    break

        if all_tested:
            print(f"All functions in {module_path} already have tests, skipping")
            return

        print(f"Adding tests for untested functions in {test_file}")

        # We'll append our new tests to the existing file
        with open(test_file, "a", encoding="utf-8") as f:
            f.write("\n\n# Added tests for previously untested functions\n")

            # Add imports for verified functions if they're not already imported
            top_level_functions = [f for f in verified_functions if "." not in f]
            if top_level_functions:
                import_line = f"from {import_path} import {', '.join(top_level_functions)}"
                if import_line not in existing_content:
                    f.write(f"\n{import_line}\n")

            # Add class definitions for class methods
            class_methods = defaultdict(list)
            for func in verified_functions:
                if "." in func:
                    class_name, method_name = func.split(".")
                    test_class_name = f"Test{class_name}"
                    test_method_name = f"test_{method_name}"

                    # Only add if the test doesn't already exist
                    if (
                        test_class_name not in existing_test_classes
                        or test_method_name not in existing_test_classes[test_class_name]["methods"]
                    ):
                        class_methods[class_name].append(method_name)

            # Generate test classes for untested methods
            for class_name, methods in class_methods.items():
                test_class_name = f"Test{class_name}"

                # If the class already exists, we'll add methods to it
                if test_class_name in existing_test_classes:
                    f.write(f"\n# Additional test methods for {test_class_name}\n")
                    for method in methods:
                        test_method_name = f"test_{method}"
                        if test_method_name not in existing_test_classes[test_class_name]["methods"]:
                            f.write(
                                f"""
    def {test_method_name}(self):
        \"\"\"Test the {method} method.\"\"\"
        # Arrange
        # instance = {class_name}()

        # Act
        # result = instance.{method}()

        # Assert
        # assert result == expected_result
        pass  # TODO: Implement test
"""
                            )
                else:
                    # Create a new test class
                    f.write(
                        f"""
class {test_class_name}:
    \"\"\"Tests for the {class_name} class.\"\"\"
"""
                    )
                    for method in methods:
                        f.write(
                            f"""
    def test_{method}(self):
        \"\"\"Test the {method} method.\"\"\"
        # Arrange
        # instance = {class_name}()

        # Act
        # result = instance.{method}()

        # Assert
        # assert result == expected_result
        pass  # TODO: Implement test
"""
                        )

            # Generate test functions for untested top-level functions
            top_level_functions = [f for f in verified_functions if "." not in f]
            untested_functions = []
            for func in top_level_functions:
                test_func_name = f"test_{func}"
                test_class_name = f"Test{module_name.capitalize()}Functions"

                # Check if there's a standalone test function or a method in a test class
                if test_func_name not in existing_test_functions and (
                    test_class_name not in existing_test_classes
                    or test_func_name not in existing_test_classes[test_class_name]["methods"]
                ):
                    untested_functions.append(func)

            if untested_functions:
                test_class_name = f"Test{module_name.capitalize()}Functions"

                # If the class already exists, we'll add methods to it
                if test_class_name in existing_test_classes:
                    f.write(f"\n# Additional test methods for {test_class_name}\n")
                    for func in untested_functions:
                        test_method_name = f"test_{func}"
                        if test_method_name not in existing_test_classes[test_class_name]["methods"]:
                            f.write(
                                f"""
    def {test_method_name}(self):
        \"\"\"Test the {func} function.\"\"\"
        # Arrange

        # Act
        # result = {func}()

        # Assert
        # assert result == expected_result
        pass  # TODO: Implement test
"""
                            )
                else:
                    # Create a new test class
                    f.write(
                        f"""
class {test_class_name}:
    \"\"\"Tests for top-level functions in the {module_name} module.\"\"\"
"""
                    )
                    for func in untested_functions:
                        f.write(
                            f"""
    def test_{func}(self):
        \"\"\"Test the {func} function.\"\"\"
        # Arrange

        # Act
        # result = {func}()

        # Assert
        # assert result == expected_result
        pass  # TODO: Implement test
"""
                        )

        print(f"Added tests for untested functions to {test_file}")
        return

    # If we're creating a new file or overwriting, generate the complete test file
    content = f"""# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest
from unittest.mock import MagicMock, patch

"""

    # Add imports for verified functions
    top_level_functions = [f for f in verified_functions if "." not in f]
    if top_level_functions:
        content += f"from {import_path} import {', '.join(top_level_functions)}\n"

    # Add class definitions for class methods
    class_methods = defaultdict(list)
    for func in functions:
        if "." in func:
            class_name, method_name = func.split(".")
            class_methods[class_name].append(method_name)

    # Generate test classes
    if class_methods:
        for class_name, methods in class_methods.items():
            content += f"""
class Test{class_name}:
    \"\"\"Tests for the {class_name} class.\"\"\"
"""
            for method in methods:
                content += f"""
    def test_{method}(self):
        \"\"\"Test the {method} method.\"\"\"
        # Arrange
        # instance = {class_name}()

        # Act
        # result = instance.{method}()

        # Assert
        # assert result == expected_result
        pass  # TODO: Implement test
"""

    # Generate test functions for top-level functions
    top_level_functions = [f for f in functions if "." not in f]
    if top_level_functions:
        content += f"""
class Test{module_name.capitalize()}Functions:
    \"\"\"Tests for top-level functions in the {module_name} module.\"\"\"
"""
        for func in top_level_functions:
            content += f"""
    def test_{func}(self):
        \"\"\"Test the {func} function.\"\"\"
        # Arrange

        # Act
        # result = {func}()

        # Assert
        # assert result == expected_result
        pass  # TODO: Implement test
"""

    # Write the test file
    with open(test_file, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Generated test file: {test_file}")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Generate test files for untested functions in cosmotech/coal/")
    parser.add_argument("--module", help="Generate tests for a specific module (e.g., cosmotech/coal/aws/s3.py)")
    parser.add_argument("--all", action="store_true", help="Generate tests for all untested functions")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing test files")
    args = parser.parse_args()

    untested_functions = find_untested_functions()

    if args.module:
        module_path = Path(args.module)
        if module_path in untested_functions:
            generate_test_file(module_path, untested_functions[module_path], args.overwrite)
        else:
            print(f"No untested functions found in {module_path}")
    elif args.all:
        for module_path, functions in untested_functions.items():
            generate_test_file(module_path, functions, args.overwrite)
    else:
        print("Functions without tests:")
        print("=======================")

        for module, functions in sorted(untested_functions.items()):
            if functions:
                print(f"\n{module}:")
                for func in sorted(functions):
                    print(f"  - {func}")

        # Print summary
        total_untested = sum(len(funcs) for funcs in untested_functions.values())
        print(f"\nTotal untested functions: {total_untested}")
        print("\nTo generate test files, use --module or --all")


if __name__ == "__main__":
    main()
