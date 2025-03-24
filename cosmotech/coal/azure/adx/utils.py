# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import dateutil.parser
from typing import Any

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def type_mapping(key: str, key_example_value: Any) -> str:
    """
    Map Python types to ADX types.

    Args:
        key: The name of the key
        key_example_value: A possible value of the key

    Returns:
        str: The name of the type used in ADX
    """
    LOGGER.debug(T("coal.logs.adx.mapping_type").format(key=key, value_type=type(key_example_value).__name__))

    if key == "SimulationRun":
        return "guid"

    try:
        # Use dateutil parser to test if the value could be a date, in case of error it is not
        dateutil.parser.parse(key_example_value, fuzzy=False)
        return "datetime"
    except (ValueError, TypeError):
        pass

    if isinstance(key_example_value, float):
        return "real"

    if isinstance(key_example_value, int):
        return "long"

    # Default case to string
    return "string"
