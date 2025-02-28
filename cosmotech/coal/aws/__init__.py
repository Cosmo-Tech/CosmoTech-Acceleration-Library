# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.

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
