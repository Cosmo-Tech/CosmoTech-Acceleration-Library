import dateutil.parser
import os
from typing import Union
import pandas as pd

from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import QueuedIngestClient, IngestionProperties


class ADXQueriesWrapper:
    """
    Wrapping class to ADX
    """

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
                    drop_by_tag: str = None) -> bool:
        """
        Will take a list of dict items and send them to a given table in ADX
        :param dict_list: list of dict objects requiring to have the same keys
        :param table_name: The name of the table in which the data should be sent
        :param ignore_table_creation: If set to True won't try to create a table to send the data
        :param drop_by_tag: Tag used for the drop by capacity of the Cosmotech API
        :return: A boolean check if the data have been sent to ADX
        """

        if not ignore_table_creation and not self.table_exists(table_name):
            # If the target table does not exist create it
            # First create the columns types needed for the table
            types = {k: self.type_mapping(k, dict_list[0][k]) for k in dict_list[0].keys()}
            # Then try to create the table
            if not self.create_table(table_name, types):
                print(f"Error creating table {table_name}.")
                return False

        # Create a dataframe with the data to write and send them to ADX
        df = pd.DataFrame(dict_list)
        self.ingest_dataframe(table_name, df, drop_by_tag)
        return True

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
                                         drop_by_tags=drop_by_tags)
        client = self.ingest_client
        client.ingest_from_dataframe(dataframe, ingestion_properties=properties)

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
        # If the table exists no need to create it
        if self.table_exists(table_name):
            return True

        create_query = f".create table {table_name}("
        for column_name, column_type in schema.items():
            create_query += f"{column_name}:{column_type},"
        create_query = create_query[:-1] + ")"
        try:
            self.run_query(create_query)
        except Exception as e:
            print(e)
            return False
        return True

    def __init__(self,
                 database: str,
                 cluster_url: Union[str, None] = None,
                 ingest_url: Union[str, None] = None,
                 cluster_name: Union[str, None] = None,
                 cluster_region: Union[str, None] = None):

        if cluster_name and cluster_region:
            cluster_url = f"https://{cluster_name}.{cluster_region}.kusto.windows.net"
            ingest_url = f"https://ingest-{cluster_name}.{cluster_region}.kusto.windows.net"

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
        self.kusto_client = KustoClient(self.cluster_kcsb)
        self.ingest_client = QueuedIngestClient(self.ingest_kcsb)
        self.database = database
