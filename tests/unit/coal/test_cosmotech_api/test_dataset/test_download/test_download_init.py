# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import pytest


class TestDownloadInit:
    """Tests for the download module initialization."""

    def test_module_imports(self):
        """Test that the module correctly imports and re-exports functions."""
        # Import the module
        from cosmotech.coal.cosmotech_api.dataset.download import (
            download_adt_dataset,
            download_twingraph_dataset,
            download_legacy_twingraph_dataset,
            download_file_dataset,
            download_dataset_by_id,
        )

        # Verify that the functions are imported correctly
        assert callable(download_adt_dataset)
        assert callable(download_twingraph_dataset)
        assert callable(download_legacy_twingraph_dataset)
        assert callable(download_file_dataset)
        assert callable(download_dataset_by_id)
