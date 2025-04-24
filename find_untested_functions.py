#!/usr/bin/env python3
"""
Script to find functions in cosmotech/coal/ that don't have corresponding tests.
"""

import os
import ast
import re
from pathlib import Path
from collections import defaultdict


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


def main():
    """Main function."""
    untested_functions = find_untested_functions()

    if not untested_functions:
        print("All functions have tests!")
        return

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


if __name__ == "__main__":
    main()
