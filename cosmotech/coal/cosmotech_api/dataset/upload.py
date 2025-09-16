import pathlib

from cosmotech_api import Dataset
from cosmotech_api import DatasetPartTypeEnum
from cosmotech_api.api.dataset_api import DatasetApi
from cosmotech_api.api.dataset_api import DatasetCreateRequest
from cosmotech_api.api.dataset_api import DatasetPartCreateRequest
import pprint

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER

LOGGER.info("Generating dataset content")


def upload_dataset(organization_id, workspace_id, dataset_name, dataset_dir) -> Dataset:
    dataset_path = pathlib.Path(dataset_dir)

    with get_api_client()[0] as client:
        d_api = DatasetApi(client)
        _files = list(_p for _p in dataset_path.rglob("*") if _p.is_file())
        d_request = DatasetCreateRequest(
            name=dataset_name,
            parts=list(
                DatasetPartCreateRequest(
                    name=_p.name,
                    description=str(_p.relative_to(dataset_path)),
                    sourceName=str(_p.relative_to(dataset_path)),
                    type=DatasetPartTypeEnum.FILE,
                )
                for _p in _files
            ),
        )
        pprint.pprint(d_request.to_dict())
        d_ret = d_api.create_dataset(
            organization_id,
            workspace_id,
            d_request,
            files=list((str(_p.relative_to(dataset_path)), _p.open("rb").read()) for _p in _files),
        )
    return d_ret
