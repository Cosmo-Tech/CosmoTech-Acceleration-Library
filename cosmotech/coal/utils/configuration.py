import os
import toml
import configparser


class Dotdict(dict):
    """dot.notation access to dictionary attributes"""
    def __setattr__(self, key, val):
        dd = Dotdict(val) if type(val) is dict else val
        self.__setitem__(key, dd)

    __getattr__ = dict.__getitem__
    __delattr__ = dict.__delitem__

    def __init__(self, dct):
        for k, v in dct.items():
            self[k] = Dotdict(v) if type(v) is dict else v

    def merge(self, d):
        for k, v in d.items():
            if type(v) is Dotdict and k in self:
                v.merge(self[k])
        self.update(self | d)


class Configuration(configparser.ConfigParser):
    CONFIG_PATH = os.environ.get('CONFIG_FILE_PATH', default=None)

    # HARD CODED ENVVAR CONVERSION
    CONVERSION_DICT = {"secrets": {
        "log_level": "LOG_LEVEL",
        "s3": {
            "access_key_id": "AWS_ACCESS_KEY_ID",
            "endpoint_url": "AWS_ENDPOINT_URL",
            "secret_access_key": "AWS_SECRET_ACCESS_KEY"
        },
        "azure": {
            "account_name": "AZURE_ACCOUNT_NAME",
            "client_id": "AZURE_CLIENT_ID",
            "client_secret": "AZURE_CLIENT_SECRET",
            "container_name": "AZURE_CONTAINER_NAME",
            "data_explorer_database_name": "AZURE_DATA_EXPLORER_DATABASE_NAME",
            "data_explorer_resource_ingest_uri": "AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI",
            "data_explorer_resource_uri": "AZURE_DATA_EXPLORER_RESOURCE_URI",
            "storage_blob_name": "AZURE_STORAGE_BLOB_NAME",
            "storage_sas_url": "AZURE_STORAGE_SAS_URL",
            "tenant_id": "AZURE_TENANT_ID"
        },
        "cosmotech": {
            "api_url": "CSM_API_URL",
            "container_mode": "CSM_CONTAINER_MODE",
            "data_adx_tag": "CSM_DATA_ADX_TAG",
            "data_adx_wait_ingestion": "CSM_DATA_ADX_WAIT_INGESTION",
            "data_blob_prefix": "CSM_DATA_BLOB_PREFIX",
            "data_bucket_name": "CSM_DATA_BUCKET_NAME",
            "data_bucket_prefix": "CSM_DATA_BUCKET_PREFIX",
            "data_prefix": "CSM_DATA_PREFIX",
            "dataset_absolute_path": "CSM_DATASET_ABSOLUTE_PATH",
            "organization_id": "CSM_ORGANIZATION_ID",
            "parameters_absolute_path": "CSM_PARAMETERS_ABSOLUTE_PATH",
            "psql_force_password_encoding": "CSM_PSQL_FORCE_PASSWORD_ENCODING",
            "run_id": "CSM_RUN_ID",
            "runner_id": "CSM_RUNNER_ID",
            "run_template_id": "CSM_RUN_TEMPLATE_ID",
            "s3_ca_bundle": "CSM_S3_CA_BUNDLE",
            "scenario_id": "CSM_SCENARIO_ID",
            "send_datawarehouse_datasets": "CSM_SEND_DATAWAREHOUSE_DATASETS",
            "send_datawarehouse_parameters": "CSM_SEND_DATAWAREHOUSE_PARAMETERS",
            "workspace_id": "CSM_WORKSPACE_ID",
            "fetch_dataset": "FETCH_DATASET",
            "fetch_datasets_in_parallel": "FETCH_DATASETS_IN_PARALLEL"
        },
        "postgres": {
            "db_name": "POSTGRES_DB_NAME",
            "db_schema": "POSTGRES_DB_SCHEMA",
            "host_port": "POSTGRES_HOST_PORT",
            "host_uri": "POSTGRES_HOST_URI",
            "user_name": "POSTGRES_USER_NAME",
            "user_password": "POSTGRES_USER_PASSWORD"
        },
        "single_store": {
            "db": "SINGLE_STORE_DB",
            "host": "SINGLE_STORE_HOST",
            "password": "SINGLE_STORE_PASSWORD",
            "port": "SINGLE_STORE_PORT",
            "tables": "SINGLE_STORE_TABLES",
            "username": "SINGLE_STORE_USERNAME"
        }
    }}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.CONFIG_PATH:
            self.configuration = Dotdict(toml.load(self.CONFIG_PATH))
        else:
            self.configuration = Dotdict(self.CONVERSION_DICT)

        # convert value to env
        def env_swap_recusion(dic):
            for k, v in dic.items():
                if type(v) is Dotdict:
                    env_swap_recusion(v)
                elif type(v) is str:
                    dic[k] = os.environ.get(v, v)
        env_swap_recusion(self.configuration.secrets)

        # set secret section back to respective section
        self.configuration.merge(self.configuration.secrets)
        del self.configuration.secrets
