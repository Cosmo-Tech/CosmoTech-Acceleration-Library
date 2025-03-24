# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Azure services integration module.

This module provides functions for interacting with Azure services like Storage and ADX.
"""

# Re-export storage functions for easier importing
from cosmotech.coal.azure.storage import (
    upload_file,
    upload_folder,
)

# Re-export blob functions for easier importing
from cosmotech.coal.azure.blob import (
    dump_store_to_azure,
)
