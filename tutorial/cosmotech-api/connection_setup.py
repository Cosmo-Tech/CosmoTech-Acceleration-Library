# Example: Setting up connections to the CosmoTech API
import os
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER

# Method 1: Using API Key (set these environment variables before running)
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

# Use the client with various API instances
from cosmotech_api.api.organization_api import OrganizationApi

org_api = OrganizationApi(api_client)

# List organizations
organizations = org_api.find_all_organizations()
for org in organizations:
    print(f"Organization: {org.name} (ID: {org.id})")

# Don't forget to close the client when done
api_client.close()

# Method 2: Using Azure Entra (set these environment variables before running)
"""
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_SCOPE"] = "api://your-app-id/.default"  # Replace with your API scope
os.environ["AZURE_CLIENT_ID"] = "your-client-id"  # Replace with your client ID
os.environ["AZURE_CLIENT_SECRET"] = "your-client-secret"  # Replace with your client secret
os.environ["AZURE_TENANT_ID"] = "your-tenant-id"  # Replace with your tenant ID

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

# Use the client with various API instances
# ...

# Don't forget to close the client when done
api_client.close()
"""

# Method 3: Using Keycloak (set these environment variables before running)
"""
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["IDP_BASE_URL"] = "https://keycloak.example.com/auth/"  # Replace with your Keycloak URL
os.environ["IDP_TENANT_ID"] = "your-realm"  # Replace with your realm
os.environ["IDP_CLIENT_ID"] = "your-client-id"  # Replace with your client ID
os.environ["IDP_CLIENT_SECRET"] = "your-client-secret"  # Replace with your client secret

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

# Use the client with various API instances
# ...

# Don't forget to close the client when done
api_client.close()
"""
