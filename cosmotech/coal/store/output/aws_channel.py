from io import BytesIO
from typing import Optional

import pyarrow.csv as pc
import pyarrow.parquet as pq
from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.aws import S3
from cosmotech.coal.store.output.channel_interface import ChannelInterface
from cosmotech.coal.store.store import Store
from cosmotech.coal.utils.configuration import Configuration, Dotdict
from cosmotech.coal.utils.logger import LOGGER


class AwsChannel(ChannelInterface):
    required_keys = {
        "cosmotech": [
            "dataset_absolute_path",
        ],
        "s3": ["access_key_id", "endpoint_url", "secret_access_key"],
    }
    requirement_string = required_keys

    def __init__(self, dct: Dotdict = None):
        self.configuration = Configuration(dct)
        self._s3 = S3(self.configuration)

    def send(self, filter: Optional[list[str]] = None) -> bool:

        _s = Store(store_location=self.configuration.cosmotech.parameters_absolute_path)

        if self._s3.output_type not in ("sqlite", "csv", "parquet"):
            LOGGER.error(T("coal.common.errors.data_invalid_output_type").format(output_type=self._s3.output_type))
            raise ValueError(T("coal.common.errors.data_invalid_output_type").format(output_type=self._s3.output_type))

        if self._s3.output_type == "sqlite":
            _file_path = _s._database_path
            _file_name = "db.sqlite"
            _uploaded_file_name = self.configuration.s3.bucket_prefix + _file_name
            LOGGER.info(
                T("coal.common.data_transfer.file_sent").format(file_path=_file_path, uploaded_name=_uploaded_file_name)
            )
            self._s3.upload_file(_file_path, _uploaded_file_name)
        else:
            tables = list(_s.list_tables())
            if filter:
                tables = [t for t in tables if t in filter]

            for table_name in tables:
                _data_stream = BytesIO()
                _file_name = None
                _data = _s.get_table(table_name)
                if not len(_data):
                    LOGGER.info(T("coal.common.data_transfer.table_empty").format(table_name=table_name))
                    continue
                if self._s3.output_type == "csv":
                    _file_name = table_name + ".csv"
                    pc.write_csv(_data, _data_stream)
                elif self._s3.output_type == "parquet":
                    _file_name = table_name + ".parquet"
                    pq.write_table(_data, _data_stream)
                LOGGER.info(
                    T("coal.common.data_transfer.sending_table").format(
                        table_name=table_name, output_type=self._s3.output_type
                    )
                )
                self._s3.upload_data_stream(
                    data_stream=_data_stream,
                    file_name=_file_name,
                )

    def delete(self):
        self._s3.delete_objects()
