from typing import Optional

from cosmotech.coal.azure.blob import dump_store_to_azure
from cosmotech.coal.store.output.channel_interface import (
    ChannelInterface,
    MissingChannelConfigError,
)
from cosmotech.coal.utils.configuration import Configuration, Dotdict


class AzureStorageChannel(ChannelInterface):
    required_keys = {
        "coal": ["store"],
        "azure": [
            "account_name",
            "container_name",
            "tenant_id",
            "client_id",
            "client_secret",
            "output_type",
            "file_prefix",
        ],
    }
    requirement_string = required_keys

    def send(self, filter: Optional[list[str]] = None) -> bool:
        dump_store_to_azure(
            self.configuration,
            selected_tables=filter,
        )

    def delete(self):
        pass
