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
            try:
                for _p in _v[1:].split("."):
                    _r = _r.get(_p, None)
                return _r
            except (KeyError, AttributeError):
                LOGGER.warning("dotdict Ref {_v} doesn't exist")
                return None
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
                # this trigger dict to dotdict conversion at merge
                self.__setattr__(k, v)


class Configuration(Dotdict):
    # Env var set by the cosmotech api at runtime
    API_ENV_DICT = {
        "secrets": {
            "cosmotech": {
                "twin_cache": {
                    "host": "TWIN_CACHE_HOST",
                    "port": "TWIN_CACHE_PORT",
                    "password": "TWIN_CACHE_PASSWORD",
                    "username": "TWIN_CACHE_USERNAME",
                },
                "idp": {
                    "tenant_id": "IDP_TENANT_ID",
                    "client_id": "IDP_CLIENT_ID",
                    "client_secret": "IDP_CLIENT_SECRET",
                },
                "api": {"url": "CSM_API_URL", "scope": "CSM_API_SCOPE"},
                "dataset_absolute_path": "CSM_DATASET_ABSOLUTE_PATH",
                "parameters_absolute_path": "CSM_PARAMETERS_ABSOLUTE_PATH",
                "organization_id": "CSM_ORGANIZATION_ID",
                "workspace_id": "CSM_WORKSPACE_ID",
                "runner_id": "CSM_RUNNER_ID",
                "run_id": "CSM_RUN_ID",
                "run_template_id": "CSM_RUN_TEMPLATE_ID",
            }
        }
    }
    # HARD CODED ENVVAR CONVERSION
    CONVERSION_DICT = {
        "secrets": {
            "log_level": "LOG_LEVEL",
            "s3": {
                "access_key_id": "AWS_ACCESS_KEY_ID",
                "endpoint_url": "AWS_ENDPOINT_URL",
                "secret_access_key": "AWS_SECRET_ACCESS_KEY",
                "bucket_name": "CSM_DATA_BUCKET_NAME",
                "bucket_prefix": "CSM_DATA_BUCKET_PREFIX",
                "ca_bundle": "CSM_S3_CA_BUNDLE",
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
                "data_blob_prefix": "CSM_DATA_BLOB_PREFIX",
                "data_prefix": "CSM_DATA_PREFIX",
                "storage_sas_url": "AZURE_STORAGE_SAS_URL",
                "tenant_id": "AZURE_TENANT_ID",
            },
            "cosmotech": {
                "data_adx_tag": "CSM_DATA_ADX_TAG",
                "data_adx_wait_ingestion": "CSM_DATA_ADX_WAIT_INGESTION",
                "send_datawarehouse_datasets": "CSM_SEND_DATAWAREHOUSE_DATASETS",
                "send_datawarehouse_parameters": "CSM_SEND_DATAWAREHOUSE_PARAMETERS",
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

    # HARD CODED configmap mount path set in K8s simulation pod by API run function
    K8S_CONFIG = "/mnt/coal/coal-config.toml"

    def __init__(self, dct: dict = None):
        if dct:
            super().__init__(dct)
        elif config_path := os.environ.get("CONFIG_FILE_PATH", default=None):
            with open(config_path, "rb") as config:
                super().__init__(tomllib.load(config))
        elif os.path.isfile(self.K8S_CONFIG):
            with open(self.K8S_CONFIG) as config:
                super().__init__(tomllib.load(config))
        else:
            LOGGER.info(T("coal.utils.configuration.no_config_file"))
            super().__init__(self.CONVERSION_DICT)

        # add coal.store default value if ont define
        if self.safe_get("coal.store") is None:
            self.merge({"coal": {"store": "$cosmotech.parameters_absolute_path"}})
        # add envvar set by the API
        self.merge(Dotdict(self.API_ENV_DICT))

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
                dic[k] = {k: v for k, v in dic[k].items() if v is not None}
            elif isinstance(v, list):
                dic[k] = list(self._env_swap_recusion(_v) for _v in v)
            elif isinstance(v, str):
                dic[k] = os.environ.get(v)
        # remove value not found
        dic = {k: v for k, v in dic.items() if v}
        return dic

    def merge_in(self, dic):
        trans_dic = self._env_swap_recusion(dic)
        self.merge(trans_dic)

    def safe_get(self, key, default=None):
        try:
            _r = self
            for _k in key.split("."):
                _r = _r.get(_k, default)
            return _r
        except (KeyError, AttributeError) as err:
            LOGGER.warning(err)
            return default


ENVIRONMENT_CONFIGURATION = Configuration()
