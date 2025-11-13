from typing import Optional

from cosmotech.coal.postgresql.runner import (
    send_runner_metadata_to_postgresql_from_conf,
)
from cosmotech.coal.postgresql.store import dump_store_to_postgresql_from_conf
from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.utils.configuration import Configuration


class PostgresChannel(ChannelInterface):

    def __init__(self):
        self.configuration = Configuration()

    def send(self, tables_filter: Optional[list[str]] = None) -> bool:
        run_id = send_runner_metadata_to_postgresql_from_conf(self.configuration)
        dump_store_to_postgresql_from_conf(
            self.configuration,
            store_folder=self.configuration.cosmotech.dataset_absolute_path,
            selected_tables=tables_filter,
            fk_id=run_id,
        )
