# Example: Setting up connections to the CosmoTech API
import os

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.logger import LOGGER

# Method 1: API Key — requires CSM_API_URL and CSM_API_KEY
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

connection = Connection()
LOGGER.info(f"Connected using: {connection.api_type}")

# Use api_client directly with SDK classes if needed
from cosmotech_api.api.organization_api import OrganizationApi

org_api = OrganizationApi(connection.api_client)
organizations = org_api.find_all_organizations()
for org in organizations:
    print(f"Organization: {org.name} (ID: {org.id})")

# CoAL API wrapper classes inherit Connection and handle auth automatically:
# from cosmotech.coal.cosmotech_api.apis import WorkspaceApi, RunnerApi, DatasetApi
# ws_api = WorkspaceApi()      # auth resolved from environment
# runner_api = RunnerApi()
# dataset_api = DatasetApi()

# Method 2: Azure Entra — requires CSM_API_URL, CSM_API_SCOPE,
#   AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID
"""
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"
os.environ["CSM_API_SCOPE"] = "api://your-app-id/.default"
os.environ["AZURE_CLIENT_ID"] = "your-client-id"
os.environ["AZURE_CLIENT_SECRET"] = "your-client-secret"
os.environ["AZURE_TENANT_ID"] = "your-tenant-id"

connection = Connection()
LOGGER.info(f"Connected using: {connection.api_type}")
"""

# Method 3: Keycloak — requires CSM_API_URL, IDP_BASE_URL, IDP_TENANT_ID,
#   IDP_CLIENT_ID, IDP_CLIENT_SECRET
"""
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"
os.environ["IDP_BASE_URL"] = "https://keycloak.example.com/auth/"
os.environ["IDP_TENANT_ID"] = "your-realm"
os.environ["IDP_CLIENT_ID"] = "your-client-id"
os.environ["IDP_CLIENT_SECRET"] = "your-client-secret"

connection = Connection()
LOGGER.info(f"Connected using: {connection.api_type}")
"""
