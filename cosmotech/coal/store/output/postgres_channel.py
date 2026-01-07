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
        "coal": ["store"],
        "cosmotech": ["organization_id", "workspace_id", "runner_id"],
        "postgres": [
            "host",
            "port",
            "db_name",
            "db_schema",
            "user_name",
            "user_password",
        ],
    }
    requirement_string = required_keys

    def send(self, filter: Optional[list[str]] = None) -> bool:
        run_id = send_runner_metadata_to_postgresql(self.configuration)
        dump_store_to_postgresql_from_conf(
            configuration=self.configuration,
            selected_tables=filter,
            fk_id=run_id,
            replace=False,
        )

    def delete(self):
        # removing metadata will trigger cascade delete on real data
        remove_runner_metadata_from_postgresql(self.configuration)
