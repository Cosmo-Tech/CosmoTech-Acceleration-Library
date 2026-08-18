"""Send store data to the output channels configured for the runner."""

from cosmotech.coal.store.output.channel_spliter import ChannelSpliter
from cosmotech.coal.utils.configuration import Configuration

# Configuration loads the outputs from CONFIG_FILE_PATH or the mounted
# /mnt/coal/coal-config.toml file.
channel_spliter = ChannelSpliter(Configuration())

# Send every table in the store to all available configured outputs.
channel_spliter.send()

# To send only selected tables, pass their names as a filter.
# channel_spliter.send(filter=["summary_data"])
