# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
AWS services integration module.

This module provides functions for interacting with AWS services like S3.
"""

# Re-export S3 functions for easier importing
from cosmotech.coal.aws.s3 import (
    create_s3_client,
    create_s3_resource,
    upload_file,
    upload_folder,
    download_files,
    upload_data_stream,
    delete_objects,
)
