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
        tags: Optional[list[str]] = None,
        additional_data: Optional[dict] = None,
    ) -> Dataset:
        """Upload a new dataset with optional tags and additional data.

        Args:
            dataset_name: The name of the dataset to create
            as_files: List of file paths to upload as FILE type parts
            as_db: List of file paths to upload as DB type parts
            tags: Optional list of tags to associate with the dataset
            additional_data: Optional dictionary of additional metadata

        Returns:
            The created Dataset object
        """
        _parts = list()

        for _f in as_files:
            _parts.extend(self.path_to_parts(_f, DatasetPartTypeEnum.FILE))

        for _db in as_db:
            _parts.extend(self.path_to_parts(_db, DatasetPartTypeEnum.DB))

        d_request = DatasetCreateRequest(
            name=dataset_name,
            tags=tags,
            additional_data=additional_data,
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

    def upload_dataset_parts(
        self,
        dataset_id: str,
        as_files: Optional[list[Union[Path, str]]] = (),
        as_db: Optional[list[Union[Path, str]]] = (),
        replace_existing: bool = False,
    ) -> Dataset:
        """Upload parts to an existing dataset.

        Args:
            dataset_id: The ID of the existing dataset
            as_files: List of file paths to upload as FILE type parts
            as_db: List of file paths to upload as DB type parts
            replace_existing: If True, replace existing parts with same name

        Returns:
            The updated Dataset object
        """
        # Get current dataset to check existing parts
        current_dataset = self.get_dataset(
            organization_id=self.configuration.cosmotech.organization_id,
            workspace_id=self.configuration.cosmotech.workspace_id,
            dataset_id=dataset_id,
        )

        # Build set of existing part names and their IDs for quick lookup
        existing_parts = {part.source_name: part.id for part in (current_dataset.parts or [])}

        # Collect parts to upload
        _parts = list()
        for _f in as_files:
            _parts.extend(self.path_to_parts(_f, DatasetPartTypeEnum.FILE))
        for _db in as_db:
            _parts.extend(self.path_to_parts(_db, DatasetPartTypeEnum.DB))

        # Process each part
        for _p_name, _p_path, _type in _parts:
            if _p_name in existing_parts:
                if replace_existing:
                    # Delete existing part before creating new one
                    self.delete_dataset_part(
                        organization_id=self.configuration.cosmotech.organization_id,
                        workspace_id=self.configuration.cosmotech.workspace_id,
                        dataset_id=dataset_id,
                        dataset_part_id=existing_parts[_p_name],
                    )
                    LOGGER.info(T("coal.services.dataset.part_replaced").format(part_name=_p_name))
                else:
                    LOGGER.warning(T("coal.services.dataset.part_skipped").format(part_name=_p_name))
                    continue

            # Create new part
            part_request = DatasetPartCreateRequest(
                name=_p_name,
                description=_p_name,
                sourceName=_p_name,
                type=_type,
            )

            self.create_dataset_part(
                organization_id=self.configuration.cosmotech.organization_id,
                workspace_id=self.configuration.cosmotech.workspace_id,
                dataset_id=dataset_id,
                dataset_part_create_request=part_request,
                file=(_p_name, _p_path.open("rb").read()),
            )
            LOGGER.debug(T("coal.services.dataset.part_uploaded").format(part_name=_p_name))

        # Return updated dataset
        updated_dataset = self.get_dataset(
            organization_id=self.configuration.cosmotech.organization_id,
            workspace_id=self.configuration.cosmotech.workspace_id,
            dataset_id=dataset_id,
        )

        LOGGER.info(T("coal.services.dataset.parts_uploaded").format(dataset_id=dataset_id))
        return updated_dataset
