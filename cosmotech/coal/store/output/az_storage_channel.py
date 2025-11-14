from typing import Optional

from cosmotech.coal.azure.blob import dump_store_to_azure
from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.utils.configuration import Configuration


class AzureStorageChannel(ChannelInterface):

    def __init__(self):
        self.configuration = Configuration()

    def send(self, tables_filter: Optional[list[str]] = None) -> bool:
        dump_store_to_azure(
            store_folder=self.configuration.azure.store_folder,
            account_name=self.configuration.azure.account_name,
            container_name=self.configuration.azure.container_name,
            tenant_id=self.configuration.azure.tenant_id,
            client_id=self.configuration.azure.client_id,
            client_secret=self.configuration.azure.client_secret,
            output_type=self.configuration.azure.output_type,
            file_prefix=self.configuration.azure.file_prefix,
            selected_tables=tables_filter,
        )
