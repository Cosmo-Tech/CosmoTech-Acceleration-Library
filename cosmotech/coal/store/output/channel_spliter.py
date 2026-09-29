from typing import Optional

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.store.output.aws_channel import AwsChannel
from cosmotech.coal.store.output.az_storage_channel import AzureStorageChannel
from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.store.output.postgres_channel import PostgresChannel
from cosmotech.coal.utils.configuration import Dotdict
from cosmotech.coal.utils.logger import LOGGER


class ChannelSpliter(ChannelInterface):
    """Channel that fans out the store's data to every configured output channel."""

    requirement_string: str = "(Requires any working interface)"
    targets = list()
    available_interfaces: dict[str, ChannelInterface] = {
        "s3": AwsChannel,
        "az_storage": AzureStorageChannel,
        "postgres": PostgresChannel,
    }

    def __init__(self, dct: Dotdict = None):
        super().__init__(dct)
        self.targets = list()
        if "outputs" not in self.configuration:
            raise AttributeError(T("coal.store.output.split.no_targets"))
        for output in self.configuration.outputs:
            channel = self.available_interfaces[output.type]
            _i = channel(output.conf)
            if _i.is_available():
                self.targets.append(_i)
            else:
                LOGGER.warning(
                    T("coal.store.output.split.requirements").format(
                        interface_name=channel.__class__.__name__, requirements=channel.requirement_string
                    )
                )
        if not self.targets:
            raise AttributeError(T("coal.store.output.split.no_targets"))

    def send(self, filter: Optional[list[str]] = None) -> bool:
        """Send the store's data through every available target channel.

        Args:
            filter: Optional list of table names to restrict the send to

        Returns:
            True if at least one target channel sent its data successfully
        """
        any_ok = False
        for i in self.targets:
            try:
                any_ok = i.send(filter=filter) or any_ok
            except Exception:
                LOGGER.error(T("coal.store.output.split.send.error").format(interface_name=i.__class__.__name__))
                if len(self.targets) < 2:
                    raise
        return any_ok

    def delete(self) -> bool:
        """Delete the data previously sent through every available target channel.

        Returns:
            True if at least one target channel deleted its data successfully
        """
        any_ok = False
        for i in self.targets:
            try:
                any_ok = i.delete() or any_ok
            except Exception:
                LOGGER.error(T("coal.store.output.split.delete.error").format(interface_name=i.__class__.__name__))
                if len(self.targets) < 2:
                    raise
        return any_ok
