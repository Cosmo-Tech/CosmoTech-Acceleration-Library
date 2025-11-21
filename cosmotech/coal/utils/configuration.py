import os
import tomllib

from cosmotech.orchestrator.utils.translate import T

from cosmotech.coal.utils.logger import LOGGER


class Dotdict(dict):
    """dot.notation access to dictionary attributes"""

    def __setattr__(self, key, val):
        dd = Dotdict(val, root=self.root) if isinstance(val, dict) else val
        self.__setitem__(key, dd)

    def __getattr__(self, key):
        _v = self.__getitem__(key)
        if isinstance(_v, str) and _v.startswith("$"):
            _r = self.root
            for _p in _v[1:].split("."):
                _r = _r[_p]
            return _r
        return _v

    __delattr__ = dict.__delitem__

    def __init__(self, dct=dict(), root=None):
        object.__setattr__(self, "root", self if root is None else root)

        def update(data):
            if isinstance(data, dict):
                return Dotdict(data, root=self.root)
            elif isinstance(data, list):
                return [update(d) for d in data]
            return data

        for k, v in dct.items():
            self[k] = update(v)

    def merge(self, d):
        for k, v in d.items():
            if isinstance(v, Dotdict) and k in self:
                self[k].merge(v)
            else:
                self[k] = v


class Configuration(Dotdict):

    # HARD CODED ENVVAR CONVERSION
    CONVERSION_DICT = {
        "outputs": [
            {
                "type": "postgres",
                "conf": {
                    "cosmotech": {
                        "dataset_absolute_path": "$cosmotech.dataset_absolute_path",
                        "organization_id": "$cosmotech.organization_id",
                        "workspace_id": "$cosmotech.workspace_id",
                        "runner_id": "$cosmotech.runner_id",
                    },
                    "postgres": {
                        "host": "$postgres.host",
                        "post": "$postgres.post",
                        "db_name": "$postgres.db_name",
                        "db_schema": "$postgres.db_schema",
                        "user_name": "$postgres.user_name",
                        "user_password": "$postgres.user_password",
                    },
                },
            }
        ],
        "secrets": {
            "log_level": "LOG_LEVEL",
            "s3": {
                "access_key_id": "AWS_ACCESS_KEY_ID",
                "endpoint_url": "AWS_ENDPOINT_URL",
                "secret_access_key": "AWS_SECRET_ACCESS_KEY",
                "bucket_prefix": "CSM_DATA_BUCKET_PREFIX",
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
                "tenant_id": "AZURE_TENANT_ID",
            },
            "cosmotech": {
                "api_url": "CSM_API_URL",
                "container_mode": "CSM_CONTAINER_MODE",
                "data_adx_tag": "CSM_DATA_ADX_TAG",
                "data_adx_wait_ingestion": "CSM_DATA_ADX_WAIT_INGESTION",
                "data_blob_prefix": "CSM_DATA_BLOB_PREFIX",
                "data_bucket_name": "CSM_DATA_BUCKET_NAME",
                "data_prefix": "CSM_DATA_PREFIX",
                "dataset_absolute_path": "CSM_DATASET_ABSOLUTE_PATH",
                "organization_id": "CSM_ORGANIZATION_ID",
                "parameters_absolute_path": "CSM_PARAMETERS_ABSOLUTE_PATH",
                "run_id": "CSM_RUN_ID",
                "runner_id": "CSM_RUNNER_ID",
                "run_template_id": "CSM_RUN_TEMPLATE_ID",
                "s3_ca_bundle": "CSM_S3_CA_BUNDLE",
                "scenario_id": "CSM_SCENARIO_ID",
                "send_datawarehouse_datasets": "CSM_SEND_DATAWAREHOUSE_DATASETS",
                "send_datawarehouse_parameters": "CSM_SEND_DATAWAREHOUSE_PARAMETERS",
                "workspace_id": "CSM_WORKSPACE_ID",
                "fetch_dataset": "FETCH_DATASET",
                "fetch_datasets_in_parallel": "FETCH_DATASETS_IN_PARALLEL",
            },
            "postgres": {
                "db_name": "POSTGRES_DB_NAME",
                "db_schema": "POSTGRES_DB_SCHEMA",
                "port": "POSTGRES_HOST_PORT",
                "host": "POSTGRES_HOST_URI",
                "user_name": "POSTGRES_USER_NAME",
                "user_password": "POSTGRES_USER_PASSWORD",
                "password_encoding": "CSM_PSQL_FORCE_PASSWORD_ENCODING",
            },
            "single_store": {
                "db": "SINGLE_STORE_DB",
                "host": "SINGLE_STORE_HOST",
                "password": "SINGLE_STORE_PASSWORD",
                "port": "SINGLE_STORE_PORT",
                "tables": "SINGLE_STORE_TABLES",
                "username": "SINGLE_STORE_USERNAME",
            },
        },
    }

    def __init__(self, dct: dict = None):
        if dct:
            super().__init__(dct)
        elif config_path := os.environ.get("CONFIG_FILE_PATH", default=None):
            with open(config_path, "rb") as f:
                super().__init__(tomllib.load(f))
        else:
            LOGGER.info(T("coal.utils.configuration.no_config_file"))
            super().__init__(self.CONVERSION_DICT)

        if "secrets" in self:
            self.secrets = self._env_swap_recusion(self.secrets)
            # set secret section back to respective section
            self.merge(self.secrets)
            del self.secrets

    # convert value to env
    def _env_swap_recusion(self, dic):
        for k, v in dic.items():
            if isinstance(v, Dotdict):
                dic[k] = self._env_swap_recusion(v)
                # remove value not found
                dic[k] = {k: v for k, v in dic[k].items() if v is not None}
            elif isinstance(v, list):
                dic[k] = list(self._env_swap_recusion(_v) for _v in v)
            elif isinstance(v, str):
                dic[k] = os.environ.get(v)
        return dic

    def merge_in(self, dic):
        trans_dic = self._env_swap_recusion(dic)
        self._merge(trans_dic)


ENVIRONMENT_CONFIGURATION = Configuration()
