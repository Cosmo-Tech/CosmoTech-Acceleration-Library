from typing import Optional

from cosmotech.coal.postgresql.runner import (
    remove_runner_metadata_from_postgresql,
    send_runner_metadata_to_postgresql,
)
from cosmotech.coal.postgresql.store import dump_store_to_postgresql_from_conf
from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.utils.configuration import Configuration, Dotdict


class PostgresChannel(ChannelInterface):
    required_keys = {
        "cosmotech": ["dataset_absolute_path", "organization_id", "workspace_id", "runner_id"],
        "postgres": [
            "host",
            "post",
            "db_name",
            "db_schema",
            "user_name",
            "user_password",
        ],
    }
    requirement_string = required_keys

    def __init__(self, dct: Dotdict = None):
        self.configuration = Configuration(dct)

    def send(self, tables_filter: Optional[list[str]] = None) -> bool:
        run_id = send_runner_metadata_to_postgresql(self.configuration)
        dump_store_to_postgresql_from_conf(
            self.configuration,
            store_folder=self.configuration.cosmotech.dataset_absolute_path,
            selected_tables=tables_filter,
            fk_id=run_id,
        )

    def delete(self):
        # removing metadata will trigger cascade delete on real data
        remove_runner_metadata_from_postgresql(self.configuration)
