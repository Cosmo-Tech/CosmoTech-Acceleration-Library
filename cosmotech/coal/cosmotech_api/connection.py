# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import os

import cosmotech_api
from azure.identity import EnvironmentCredential

from cosmotech.coal.utils.logger import LOGGER

api_env_keys = {"CSM_API_KEY", "CSM_API_URL"}
azure_env_keys = {"AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID", "CSM_API_URL", "CSM_API_SCOPE"}


def get_api_client() -> (cosmotech_api.ApiClient, str):
    existing_keys = set(os.environ.keys())
    missing_azure_keys = azure_env_keys - existing_keys
    missing_api_keys = api_env_keys - existing_keys
    if all((missing_api_keys, missing_azure_keys)):
        LOGGER.error("No set of environment variables found for a valid Cosmo Tech API connection")
        LOGGER.error("Existing sets are:")
        LOGGER.error(f"  Azure Entra Connection : {', '.join(azure_env_keys)}")
        LOGGER.error(f"  Cosmo Tech API Key : {', '.join(api_env_keys)}")
        raise EnvironmentError("No set of environment variables found for a valid Cosmo Tech API connection")

    if not missing_api_keys:
        configuration = cosmotech_api.Configuration(
            host=os.environ.get("CSM_API_URL"),
        )
        return cosmotech_api.ApiClient(configuration,
                                       os.environ.get("CSM_API_KEY_HEADER", "X-CSM-API-KEY"),
                                       os.environ.get("CSM_API_KEY")), "Cosmo Tech API Key"

    if not missing_azure_keys:
        credentials = EnvironmentCredential()
        token = credentials.get_token(os.environ.get("CSM_API_SCOPE"))

        configuration = cosmotech_api.Configuration(
            host=os.environ.get("CSM_API_URL"),
            access_token=token.token
        )
        return cosmotech_api.ApiClient(configuration), "Azure Entra Connection"

    # TODO : Add KeyCloak Connection here

    raise EnvironmentError("No valid connection available to the Cosmo Tech API")
