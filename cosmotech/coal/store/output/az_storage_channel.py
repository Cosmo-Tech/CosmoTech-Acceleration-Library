from typing import Optional

from cosmotech.coal.azure.blob import delete_azure_blobs, dump_store_to_azure
from cosmotech.coal.store.output.channel_interface import (
    ChannelInterface,
)
from cosmotech.coal.utils.configuration import Dotdict


class AzureStorageChannel(ChannelInterface):
    required_keys = {
        "coal": ["store"],
        "cosmotech": ["runner_id"],
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

    def __init__(self, dct: Dotdict = None):
        super().__init__(dct)
        self.configuration.azure.file_prefix = (
            self.configuration.cosmotech.runner_id + "/" + self.configuration.azure.file_prefix
        )

    def send(self, filter: Optional[list[str]] = None) -> bool:
        dump_store_to_azure(
            self.configuration,
            selected_tables=filter,
        )

    def delete(self):
        delete_azure_blobs(self.configuration)
