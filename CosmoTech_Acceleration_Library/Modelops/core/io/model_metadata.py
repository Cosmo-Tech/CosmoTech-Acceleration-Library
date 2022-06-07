# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
from datetime import datetime

from CosmoTech_Acceleration_Library.Modelops.core.common.redis_handler import RedisHandler
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil

logger = logging.getLogger(__name__)

class ModelMetadata(RedisHandler):
    """
    Model Metadata management class for cached data
    """

    last_modified_date_key = "lastModifiedDate"
    last_version_key = "lastVersion"
    source_url_key = "adtUrl"
    graph_name_key = "graphName"
    graph_rotation_key = "graphRotation"

    def get_metadata(self) -> dict:
        """
        Get the metadata of the graph
        :return: the dict containing all graph metadata
        """
        return self.r.hgetall(self.metadata_key)

    def get_last_graph_version(self) -> str:
        """
        Get the current last version of the graph
        :return: the graph last version
        """
        return self.get_metadata()[self.last_version_key]

    def get_graph_name(self) -> str:
        """
        Get the graph's name
        :return: the graph's name
        """
        return self.name

    def get_graph_source_url(self) -> str:
        """
        Get the datasource of the graph
        :return: the datasource of the graph
        """
        return self.get_metadata()[self.source_url_key]

    def get_graph_rotation(self) -> str:
        """
        Get the graph rotation of the graph
        :return: the graph rotation of the graph
        """
        return self.get_metadata()[self.graph_rotation_key]

    def get_last_modified_date(self) -> datetime:
        """
        Get the last modified date of the graph
        :return: the last modified date of the graph
        """
        metadata_last_version = self.get_metadata()[self.last_modified_date_key]
        return ModelUtil.convert_str_to_datetime(metadata_last_version)

    def set_all_metadata(self, metadata: dict):
        """
        Set the metadata of the graph
        :param metadata the metadata to set
        :raise Exception if the current version is greater than the new one
        """
        current_metadata = self.get_metadata()
        if self.last_version_key in current_metadata:
            current_version = int(self.get_last_graph_version())
            new_version = int(metadata[self.last_version_key])
            if new_version > current_version:
                logger.debug(f"Metatadata to set : {metadata}")
                self.r.hmset(self.metadata_key, metadata)
            else:
                raise Exception(f"The current version {current_version} is equal or greater than the version to set: {new_version}")
        else:
            logger.debug(f"Metatadata to set : {metadata}")
            self.r.hmset(self.metadata_key, metadata)

    def set_metadata(self,
                     last_graph_version: int,
                     graph_source_url: str,
                     graph_rotation: int) -> dict:
        """
        Set the metadata of the graph
        :param last_graph_version the new version
        :param graph_source_url the source url
        :param graph_rotation the graph rotation
        :return the metadata set
        :raise Exception if the current version is greater than the new one
        """
        metadata = {
            self.last_version_key: str(last_graph_version),
            self.graph_name_key: self.name,
            self.source_url_key: graph_source_url,
            self.graph_rotation_key: str(graph_rotation),
            self.last_modified_date_key: ModelUtil.convert_datetime_to_str(datetime.utcnow())
        }
        logger.debug(f"Metatadata to set : {metadata}")
        self.set_all_metadata(metadata=metadata)

    def set_last_graph_version(self, last_graph_version: int):
        """
        Set the current last version of the graph
        :param last_graph_version the new version
        """
        self.r.hset(self.metadata_key, self.last_version_key, str(last_graph_version))
        logger.debug(f"Graph last_graph_version to set : {str(last_graph_version)}")
        self.update_last_modified_date()

    def set_graph_source_url(self, graph_source_url: str):
        """
        Set the datasource of the graph
        :param graph_source_url the source url
        """
        self.r.hset(self.metadata_key, self.source_url_key, graph_source_url)
        logger.debug(f"Graph source_url to set : {str(graph_source_url)}")
        self.update_last_modified_date()

    def set_graph_rotation(self, graph_rotation: int):
        """
        Set the graph rotation of the graph
        :param graph_rotation the graph rotation
        """
        self.r.hset(self.metadata_key, self.graph_rotation_key, str(graph_rotation))
        logger.debug(f"Graph graph_rotation to set : {str(graph_rotation)}")
        self.update_last_modified_date()

    def update_last_modified_date(self):
        """
        Update the last modified date of the graph
        """
        self.r.hset(self.metadata_key, self.last_modified_date_key, ModelUtil.convert_datetime_to_str(datetime.utcnow()))

    def update_last_version(self):
        """
        Update the last version of the graph
        """
        current_metadata = self.get_metadata()
        if self.last_version_key in current_metadata:
            current_version = int(self.get_last_graph_version())
            current_version += 1
            self.set_last_graph_version(str(current_version))
            self.update_last_modified_date()
        else:
            self.set_last_graph_version("0")
            self.update_last_modified_date()
