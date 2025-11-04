# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

from typing import Dict, List

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.logger import LOGGER


def sheet_to_header(sheet_content: List[Dict]) -> List[str]:
    """
    Extract header fields from sheet content.

    Args:
        sheet_content: List of dictionaries representing sheet rows

    Returns:
        List of field names with id, source, and target fields first if present
    """
    LOGGER.debug(T("coal.services.dataset.extracting_headers").format(rows=len(sheet_content)))

    fieldnames = []
    has_src = False
    has_id = False

    for r in sheet_content:
        for k in r.keys():
            if k not in fieldnames:
                if k in ["source", "target"]:
                    has_src = True
                elif k == "id":
                    has_id = True
                else:
                    fieldnames.append(k)

    # Ensure source/target and id fields come first
    if has_src:
        fieldnames = ["source", "target"] + fieldnames
    if has_id:
        fieldnames = ["id"] + fieldnames

    LOGGER.debug(
        T("coal.services.dataset.headers_extracted").format(
            count=len(fieldnames),
            fields=", ".join(fieldnames[:5]) + ("..." if len(fieldnames) > 5 else ""),
        )
    )

    return fieldnames
