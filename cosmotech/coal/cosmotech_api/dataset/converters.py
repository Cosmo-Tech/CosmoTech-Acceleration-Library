# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import csv
import json
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T
from cosmotech.coal.cosmotech_api.dataset.utils import sheet_to_header


def convert_dataset_to_files(dataset_info: Dict[str, Any], target_folder: Optional[Union[str, Path]] = None) -> Path:
    """
    Convert dataset info to files.

    Args:
        dataset_info: Dataset info dict with type, content, name
        target_folder: Optional folder to save files (if None, uses temp dir)

    Returns:
        Path to folder containing files
    """
    dataset_type = dataset_info["type"]
    content = dataset_info["content"]
    name = dataset_info["name"]

    LOGGER.info(T("coal.services.dataset.converting_to_files").format(dataset_type=dataset_type, dataset_name=name))

    if target_folder is None:
        target_folder = Path(tempfile.mkdtemp())
        LOGGER.debug(T("coal.services.dataset.created_temp_folder").format(folder=target_folder))
    else:
        target_folder = Path(target_folder)
        target_folder.mkdir(parents=True, exist_ok=True)
        LOGGER.debug(T("coal.services.dataset.using_folder").format(folder=target_folder))

    if dataset_type in ["adt", "twincache"]:
        return convert_graph_dataset_to_files(content, target_folder)
    else:
        return convert_file_dataset_to_files(content, target_folder, dataset_type)


def convert_graph_dataset_to_files(
    content: Dict[str, List[Dict]], target_folder: Optional[Union[str, Path]] = None
) -> Path:
    """
    Convert graph dataset content to CSV files.

    Args:
        content: Dictionary mapping entity types to lists of entities
        target_folder: Folder to save files (if None, uses temp dir)

    Returns:
        Path to folder containing files
    """
    if target_folder is None:
        target_folder = Path(tempfile.mkdtemp())
        LOGGER.debug(T("coal.services.dataset.created_temp_folder").format(folder=target_folder))
    else:
        target_folder = Path(target_folder)
        target_folder.mkdir(parents=True, exist_ok=True)
        LOGGER.debug(T("coal.services.dataset.using_folder").format(folder=target_folder))
    file_count = 0

    LOGGER.info(
        T("coal.services.dataset.converting_graph_data").format(entity_types=len(content), folder=target_folder)
    )

    for entity_type, entities in content.items():
        if not entities:
            LOGGER.debug(T("coal.services.dataset.skipping_empty_entity").format(entity_type=entity_type))
            continue

        file_path = target_folder / f"{entity_type}.csv"
        LOGGER.debug(T("coal.services.dataset.writing_csv").format(file_name=file_path.name, count=len(entities)))

        fieldnames = sheet_to_header(entities)

        with open(file_path, "w", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames, dialect="unix", quoting=csv.QUOTE_MINIMAL)
            writer.writeheader()

            for entity in entities:
                # Convert values to strings and handle boolean values
                row = {
                    k: str(v).replace("'", '"').replace("True", "true").replace("False", "false")
                    for k, v in entity.items()
                }
                writer.writerow(row)

        file_count += 1
        LOGGER.debug(T("coal.services.dataset.file_written").format(file_path=file_path))

    LOGGER.info(T("coal.services.dataset.files_created").format(count=file_count, folder=target_folder))

    return target_folder


def convert_file_dataset_to_files(
    content: Dict[str, Any],
    target_folder: Optional[Union[str, Path]] = None,
    file_type: str = "",
) -> Path:
    """
    Convert file dataset content to files.

    Args:
        content: Dictionary mapping file names to content
        target_folder: Folder to save files (if None, uses temp dir)
        file_type: Type of file (csv, json, etc.)

    Returns:
        Path to folder containing files
    """
    if target_folder is None:
        target_folder = Path(tempfile.mkdtemp())
        LOGGER.debug(T("coal.services.dataset.created_temp_folder").format(folder=target_folder))
    else:
        target_folder = Path(target_folder)
        target_folder.mkdir(parents=True, exist_ok=True)
        LOGGER.debug(T("coal.services.dataset.using_folder").format(folder=target_folder))
    file_count = 0

    LOGGER.info(
        T("coal.services.dataset.converting_file_data").format(
            file_count=len(content), file_type=file_type, folder=target_folder
        )
    )

    for file_name, file_content in content.items():
        file_path = target_folder / file_name

        # Ensure parent directories exist
        file_path.parent.mkdir(parents=True, exist_ok=True)

        LOGGER.debug(T("coal.services.dataset.writing_file").format(file_name=file_path.name, file_type=file_type))

        if isinstance(file_content, str):
            # Text content
            with open(file_path, "w") as file:
                file.write(file_content)
        elif isinstance(file_content, dict) or isinstance(file_content, list):
            # JSON content
            with open(file_path, "w") as file:
                json.dump(file_content, file, indent=2)
        else:
            # Other content types
            with open(file_path, "w") as file:
                file.write(str(file_content))

        file_count += 1
        LOGGER.debug(T("coal.services.dataset.file_written").format(file_path=file_path))

    LOGGER.info(T("coal.services.dataset.files_created").format(count=file_count, folder=target_folder))

    return target_folder
