import dateutil.parser
import os
from typing import Union
import pandas as pd

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties, ReportLevel
from azure.kusto.ingest.status import KustoIngestStatusQueues, SuccessMessage, FailureMessage

from enum import Enum
import time
from typing import Iterator


class IngestionStatus(Enum):
    QUEUED = 'QUEUED'
    SUCCESS = 'SUCCESS'
    FAILURE = 'FAILURE'
    UNKNOWN = 'UNKNOWN'
    TIMEOUT = 'TIMED OUT'


class ADXQueriesWrapper:
    """
    Wrapping class to ADX
    """

    def __init__(self,
                 database: str,
                 cluster_url: Union[str, None] = None,
                 ingest_url: Union[str, None] = None,
                 cluster_name: Union[str, None] = None,
                 cluster_region: Union[str, None] = None):

        if cluster_name and cluster_region:
            cluster_url = f"https://{cluster_name}.{cluster_region}.kusto.windows.net"
            ingest_url = f"https://ingest-{cluster_name}.{cluster_region}.kusto.windows.net"

        try:
            az_client_id = os.environ['AZURE_CLIENT_ID']
            az_client_secret = os.environ['AZURE_CLIENT_SECRET']
            az_tenant_id = os.environ['AZURE_TENANT_ID']

            self.cluster_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(cluster_url,
                                                                                                     az_client_id,
                                                                                                     az_client_secret,
                                                                                                     az_tenant_id)
            self.ingest_kcsb = KustoConnectionStringBuilder.with_aad_application_key_authentication(ingest_url,
                                                                                                    az_client_id,
                                                                                                    az_client_secret,
                                                                                                    az_tenant_id)
        except KeyError:
            self.cluster_kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster_url)
            self.ingest_kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(ingest_url)
        self.kusto_client = KustoClient(self.cluster_kcsb)
        self.ingest_client = QueuedIngestClient(self.ingest_kcsb)
        self.database = database

        self.timeout = 900

        self.ingest_status = dict()
        self.ingest_times = dict()

    @staticmethod
    def type_mapping(key: str, key_example_value) -> str:
        """
        This method is used to replace the type name from python to the one used in ADX
        :param key: the name of the key
        :param key_example_value: a possible value of the key
        :return: the name of the type used in ADX
        """

        if key == "SimulationRun":
            return "guid"

        try:
            # Use dateutil parser to test if the value could be a date, in case of error it is not
            dateutil.parser.parse(key_example_value, fuzzy=False)
            return "datetime"
        except (ValueError, TypeError):
            pass

        if type(key_example_value) is float:
            return "real"

        if type(key_example_value) is int:
            return "long"

        # Default case to string
        return "string"

    def send_to_adx(self, dict_list: list, table_name: str, ignore_table_creation: bool = True,
                    drop_by_tag: str = None):
        """
        Will take a list of dict items and send them to a given table in ADX
        :param dict_list: list of dict objects requiring to have the same keys
        :param table_name: The name of the table in which the data should be sent
        :param ignore_table_creation: If set to True won't try to create a table to send the data
        :param drop_by_tag: Tag used for the drop by capacity of the Cosmotech API
        :return: A boolean check if the data have been sent to ADX
        """

        if not ignore_table_creation:
            # If the target table does not exist create it
            # First create the columns types needed for the table
            types = {k: self.type_mapping(k, dict_list[0][k]) for k in dict_list[0].keys()}
            # Then try to create the table
            if not self.create_table(table_name, types):
                print(f"Error creating table {table_name}.")
                return False

        # Create a dataframe with the data to write and send them to ADX
        df = pd.DataFrame(dict_list)
        ingestion_result = self.ingest_dataframe(table_name, df, drop_by_tag)
        return ingestion_result

    def ingest_dataframe(self, table_name: str, dataframe: pd.DataFrame, drop_by_tag: str = None):
        """
        Write the content of dataframe to a table
        :param table_name: name of the target table
        :param dataframe: dataframe containing the data to be written
        :param drop_by_tag: Tag used for the drop by capacity of the Cosmotech API
        :return: None
        """
        drop_by_tags = [drop_by_tag] if (drop_by_tag is not None) else None
        properties = IngestionProperties(database=self.database, table=table_name, data_format=DataFormat.CSV,
                                         drop_by_tags=drop_by_tags, report_level=ReportLevel.FailuresAndSuccesses)
        client = self.ingest_client
        ingestion_result = client.ingest_from_dataframe(dataframe, ingestion_properties=properties)
        self.ingest_status[str(ingestion_result.source_id)] = IngestionStatus.QUEUED
        self.ingest_times[str(ingestion_result.source_id)] = time.time()
        return ingestion_result

    def check_ingestion_status(self, source_ids: list[str],
                               timeout: int = None,
                               logs: bool = False) -> Iterator[tuple[str, IngestionStatus]]:
        remaining_ids = []
        for source_id in source_ids:
            if source_id not in self.ingest_status:
                self.ingest_status[source_id] = IngestionStatus.UNKNOWN
                self.ingest_times[source_id] = time.time()
            if self.ingest_status[source_id] not in [IngestionStatus.QUEUED, IngestionStatus.UNKNOWN]:
                yield source_id, self.ingest_status[source_id]
            else:
                remaining_ids.append(source_id)

        qs = KustoIngestStatusQueues(self.ingest_client)

        def get_messages(queues):
            _r = []
            for q in queues:
                _r.extend(((q, m) for m in q.receive_messages(messages_per_page=32, visibility_timeout=1)))
            return _r

        successes = get_messages(qs.success._get_queues())
        failures = get_messages(qs.failure._get_queues())

        if logs:
            print(f"Success messages: {len(successes)}")
            print(f"Failure messages: {len(failures)}")
        non_sent_ids = remaining_ids[:]
        for messages, cast_func, status in [(successes, SuccessMessage, IngestionStatus.SUCCESS),
                                            (failures, FailureMessage, IngestionStatus.FAILURE)]:
            for _q, _m in messages:
                dm = cast_func(_m.content)
                to_check_ids = remaining_ids[:]
                for source_id in to_check_ids:
                    if dm.IngestionSourceId == str(source_id):
                        self.ingest_status[source_id] = status
                        if logs:
                            print(f"Found status for {source_id}: {status.value}")
                        _q.delete_message(_m)
                        remaining_ids.remove(source_id)
                        break
                else:
                    # The message did not correspond to a known ID
                    continue
                break
            else:
                # No message was found on the current list of messages for the given IDs
                continue
            break
        else:
            for source_id in remaining_ids:
                if time.time() - self.ingest_times[source_id] > ([timeout, self.timeout][timeout is None]):
                    self.ingest_status[source_id] = IngestionStatus.TIMEOUT
        for source_id in non_sent_ids:
            yield source_id, self.ingest_status[source_id]

    def _clear_ingestion_status_queues(self, confirmation: bool = False):
        """
        Dangerous operation that will fully clear all data in the ingestion status queues
        Those queues are common to all databases in the ADX Cluster so don't ut this unless you know what you are doing
        :param confirmation: Unless confirmation is set to True, won't do anything
        :return:
        """
        if confirmation:
            qs = KustoIngestStatusQueues(self.ingest_client)
            while not qs.success.is_empty():
                qs.success.pop(32)
            while not qs.failure.is_empty():
                qs.failure.pop(32)

    def run_command_query(self, query: str):
        """
        Execute a command query on the database
        :param query: the query to execute
        :return: the results of the query
        """
        client = self.kusto_client
        return client.execute_mgmt(self.database, query)

    def run_query(self, query: str):
        """
        Execute a simple query on the database
        :param query: the query to execute
        :return: the results of the query
        """
        client = self.kusto_client
        return client.execute(self.database, query)

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists on the database
        :param table_name: The table to look for
        :return: does the table exits ?
        """
        get_tables_query = f".show database ['{self.database}'] schema| distinct TableName"
        tables = self.run_query(get_tables_query)
        for r in tables.primary_results[0]:
            if table_name == r[0]:
                return True
        return False

    def create_table(self, table_name: str, schema: dict) -> bool:
        """
        Create a table on the database
        :param table_name: the name of the table
        :param schema: the schema associated to the table
        :return: Is the table created ?
        """
        create_query = f".create-merge table {table_name}("
        for column_name, column_type in schema.items():
            create_query += f"{column_name}:{column_type},"
        create_query = create_query[:-1] + ")"
        try:
            self.run_query(create_query)
        except Exception as e:
            print(e)
            return False
        return True
