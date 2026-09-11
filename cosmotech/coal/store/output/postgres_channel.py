from typing import Optional

import adbc_driver_manager
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.postgresql.runner import (
    create_metadata,
    get_metadata_last_run,
    remove_run_metadata_from_postgresql,
    remove_runner_metadata_from_postgresql,
    send_runner_metadata_to_postgresql,
)
from cosmotech.coal.postgresql.store import (
    add_fk_constraints,
    dump_store_to_postgresql_from_conf,
)
from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.utils.configuration import Dotdict
from cosmotech.coal.utils.logger import LOGGER


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

    def __init__(self, dct: Dotdict = None):
        super().__init__(dct)
        # set setup_db to default to True for compatibility
        self.configuration.setup_db = self.configuration.safe_get("setup_db", True)

    def send(self, filter: Optional[list[str]] = None) -> bool:
        if self.configuration.setup_db:
            create_metadata(self.configuration)

        # get old run id (present in metadata table)
        last_run_ids = get_metadata_last_run(self.configuration)
        rollout_run_id_list = [run_id for run_id in last_run_ids if run_id != self.configuration.cosmotech.run_id]

        # add new run id in metadata table
        new_run_id = self.configuration.cosmotech.run_id
        send_runner_metadata_to_postgresql(self.configuration)

        try:
            # Send store's tables to PSQL
            dump_store_to_postgresql_from_conf(
                configuration=self.configuration,
                selected_tables=filter,
                fk_id=new_run_id,
                replace=False,
            )

            if self.configuration.setup_db:
                add_fk_constraints(self.configuration)
        except adbc_driver_manager.ProgrammingError as e:
            # remove newly created id
            LOGGER.error(T("coal.store.output.postgres_channel.data_send_fail"))
            remove_run_metadata_from_postgresql(self.configuration, new_run_id)
            raise e
        # rollout old run id
        LOGGER.info(T("coal.store.output.postgres_channel.output_rollout").format(run_id=rollout_run_id_list))
        for last_run_id in rollout_run_id_list:
            remove_run_metadata_from_postgresql(self.configuration, last_run_id)

    def delete(self):
        # removing metadata will trigger cascade delete on real data
        remove_runner_metadata_from_postgresql(self.configuration)
