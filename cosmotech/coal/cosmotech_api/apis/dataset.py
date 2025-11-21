# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from pathlib import Path
from typing import Optional, Union

from cosmotech.orchestrator.utils.translate import T
from cosmotech_api import (
    Dataset,
)
from cosmotech_api import DatasetApi as BaseDatasetApi
from cosmotech_api import (
    DatasetCreateRequest,
    DatasetPartCreateRequest,
    DatasetPartTypeEnum,
)

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION, Configuration
from cosmotech.coal.utils.logger import LOGGER


class DatasetApi(BaseDatasetApi, Connection):

    def __init__(
        self,
        configuration: Configuration = ENVIRONMENT_CONFIGURATION,
    ):
        Connection.__init__(self, configuration)
        BaseDatasetApi.__init__(self, self.api_client)

        LOGGER.debug(T("coal.cosmotech_api.initialization.dataset_api_initialized"))

    def download_dataset(self, dataset_id) -> Dataset:
        dataset = self.get_dataset(
            organization_id=self.configuration.cosmotech.organization_id,
            workspace_id=self.configuration.cosmotech.workspace_id,
            dataset_id=dataset_id,
        )

        dataset_dir = self.configuration.cosmotech.dataset_absolute_path
        dataset_dir_path = Path(dataset_dir) / dataset_id
        for part in dataset.parts:
            part_file_path = dataset_dir_path / part.source_name
            part_file_path.parent.mkdir(parents=True, exist_ok=True)
            data_part = self.download_dataset_part(
                organization_id=self.configuration.cosmotech.organization_id,
                workspace_id=self.configuration.cosmotech.workspace_id,
                dataset_id=dataset_id,
                dataset_part_id=part.id,
            )
            with open(part_file_path, "wb") as binary_file:
                binary_file.write(data_part)
            LOGGER.debug(
                T("coal.services.dataset.part_downloaded").format(part_name=part.source_name, file_path=part_file_path)
            )
        return dataset

    @staticmethod
    def path_to_parts(_path, part_type) -> list[tuple[str, Path, DatasetPartTypeEnum]]:
        if (_path := Path(_path)).is_dir():
            return list((str(_p.relative_to(_path)), _p, part_type) for _p in _path.rglob("*") if _p.is_file())
        return list(((_path.name, _path, part_type),))

    def upload_dataset(
        self,
        dataset_name: str,
        as_files: Optional[list[Union[Path, str]]] = (),
        as_db: Optional[list[Union[Path, str]]] = (),
    ) -> Dataset:
        _parts = list()

        for _f in as_files:
            _parts.extend(self.path_to_parts(_f, DatasetPartTypeEnum.FILE))

        for _db in as_db:
            _parts.extend(self.path_to_parts(_db, DatasetPartTypeEnum.DB))

        d_request = DatasetCreateRequest(
            name=dataset_name,
            parts=list(
                DatasetPartCreateRequest(
                    name=_p_name,
                    description=_p_name,
                    sourceName=_p_name,
                    type=_type,
                )
                for _p_name, _, _type in _parts
            ),
        )

        d_ret = self.create_dataset(
            self.configuration.cosmotech.organization_id,
            self.configuration.cosmotech.workspace_id,
            d_request,
            files=list((_p[0], _p[1].open("rb").read()) for _p in _parts),
        )

        LOGGER.info(T("coal.services.dataset.dataset_created").format(dataset_id=d_ret.id))
        return d_ret
