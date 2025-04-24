# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
import os
import pathlib
import ssl

import cosmotech_api

from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T

api_env_keys = {"CSM_API_KEY", "CSM_API_URL"}
azure_env_keys = {
    "AZURE_CLIENT_ID",
    "AZURE_CLIENT_SECRET",
    "AZURE_TENANT_ID",
    "CSM_API_URL",
    "CSM_API_SCOPE",
}
keycloak_env_keys = {
    "IDP_TENANT_ID",
    "IDP_CLIENT_ID",
    "IDP_CLIENT_SECRET",
    "IDP_BASE_URL",
    "CSM_API_URL",
}


def get_api_client() -> (cosmotech_api.ApiClient, str):
    existing_keys = set(os.environ.keys())
    missing_azure_keys = azure_env_keys - existing_keys
    missing_api_keys = api_env_keys - existing_keys
    missing_keycloak_keys = keycloak_env_keys - existing_keys
    if all((missing_api_keys, missing_azure_keys, missing_keycloak_keys)):
        LOGGER.error(T("coal.common.errors.no_env_vars"))
        LOGGER.error(T("coal.cosmotech_api.connection.existing_sets"))
        LOGGER.error(T("coal.cosmotech_api.connection.azure_connection").format(keys=", ".join(azure_env_keys)))
        LOGGER.error(T("coal.cosmotech_api.connection.api_key_connection").format(keys=", ".join(api_env_keys)))
        LOGGER.error(T("coal.cosmotech_api.connection.keycloak_connection").format(keys=", ".join(keycloak_env_keys)))
        raise EnvironmentError(T("coal.common.errors.no_env_vars"))

    if not missing_keycloak_keys:
        LOGGER.info(T("coal.cosmotech_api.connection.found_keycloak"))
        from keycloak import KeycloakOpenID

        server_url = os.environ.get("IDP_BASE_URL")
        if server_url[-1] != "/":
            server_url = server_url + "/"
        keycloack_parameters = dict(
            server_url=server_url,
            client_id=os.environ.get("IDP_CLIENT_ID"),
            realm_name=os.environ.get("IDP_TENANT_ID"),
            client_secret_key=os.environ.get("IDP_CLIENT_SECRET"),
        )
        if (ca_cert_path := os.environ.get("IDP_CA_CERT")) and pathlib.Path(ca_cert_path).exists():
            LOGGER.info(T("coal.cosmotech_api.connection.found_cert_authority"))
            keycloack_parameters["verify"] = ca_cert_path
        keycloak_openid = KeycloakOpenID(**keycloack_parameters)

        access_token = keycloak_openid.token(grant_type="client_credentials")

        configuration = cosmotech_api.Configuration(
            host=os.environ.get("CSM_API_URL"),
            access_token=access_token["access_token"],
        )
        return cosmotech_api.ApiClient(configuration), "Keycloak Connection"

    if not missing_api_keys:
        LOGGER.info(T("coal.cosmotech_api.connection.found_api_key"))
        configuration = cosmotech_api.Configuration(
            host=os.environ.get("CSM_API_URL"),
        )
        return (
            cosmotech_api.ApiClient(
                configuration,
                os.environ.get("CSM_API_KEY_HEADER", "X-CSM-API-KEY"),
                os.environ.get("CSM_API_KEY"),
            ),
            "Cosmo Tech API Key",
        )

    if not missing_azure_keys:
        LOGGER.info(T("coal.cosmotech_api.connection.found_azure"))
        from azure.identity import EnvironmentCredential

        credentials = EnvironmentCredential()
        token = credentials.get_token(os.environ.get("CSM_API_SCOPE"))

        configuration = cosmotech_api.Configuration(host=os.environ.get("CSM_API_URL"), access_token=token.token)
        return cosmotech_api.ApiClient(configuration), "Azure Entra Connection"

    raise EnvironmentError(T("coal.common.errors.no_valid_connection"))
