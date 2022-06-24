# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
from pathlib import Path

from CosmoTech_Acceleration_Library.Modelops.core.common.redis_handler import RedisHandler
from CosmoTech_Acceleration_Library.Modelops.core.io.model_metadata import ModelMetadata
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil

logger = logging.getLogger(__name__)


class GraphHandler(RedisHandler):
    """
    Class that handle Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, password: str = None, source_url: str = "", graph_rotation: int = 1):
        super().__init__(host=host, port=port, name=name, password=password)
        logger.debug("GraphHandler init")
        self.graph = self.r.graph(name)
        self.m_metadata = ModelMetadata(host=host, port=port, name=name, password=password)
        current_metadata = self.m_metadata.get_metadata()
        if not current_metadata:
            logger.debug("Create metadata key")
            self.m_metadata.set_metadata(last_graph_version=0, graph_source_url=source_url,
                                         graph_rotation=graph_rotation)


class VersionedGraphHandler(GraphHandler):
    """
    Class that handle Versioned Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, version: int, password: str = None, source_url: str = "",
                 graph_rotation: int = 1):
        super().__init__(host=host, port=port, name=name, password=password, source_url=source_url,
                         graph_rotation=graph_rotation)
        logger.debug("VersionedGraphHandler init")
        self.version = None
        self.versioned_name = None
        self.fill_versioned_graph_data(name, version)

    def fill_versioned_graph_data(self, graph_name: str, version: int):
        """
        Fill data about version and create new graph
        :param graph_name: the graph name
        :param version: the version
        """
        versioned_name = ModelUtil.build_graph_version_name(graph_name, version)
        self.versioned_name = versioned_name
        self.version = version
        self.graph = self.r.graph(versioned_name)
        self.m_metadata.set_last_graph_version(version)


class RotatedGraphHandler(VersionedGraphHandler):
    """
    Class that handle Rotated Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, password: str = None, version: int = -1, source_url: str = "",
                 graph_rotation: int = 1):
        super().__init__(host=host, port=port, name=name, password=password, source_url=source_url, version=version,
                         graph_rotation=graph_rotation)
        logger.debug("RotatedGraphHandler init")
        self.graph_rotation = graph_rotation
        new_version = version
        if version == -1:
            logger.debug("Handle Rotation Graph")
            new_version = self.handle_graph_rotation(name, graph_rotation)
        self.fill_versioned_graph_data(name, new_version)
        self.m_metadata.set_graph_rotation(graph_rotation=graph_rotation)

    def handle_graph_rotation(self, graph_name: str, graph_rotation: int) -> int:
        """
        Handle graph rotation (delete the oldest graph if the amount of graph is upper than graph rotation
        :param graph_name: the graph name
        :param graph_rotation: the amount of graph to keep
        :return: the graph version to create
        """
        matching_keys = self.r.keys(ModelUtil.build_graph_key_pattern(graph_name))
        graph_versions = []
        for graph_key in matching_keys:
            graph_versions.append(graph_key.split(":")[-1])
        min_version = 0
        max_version = 0
        if len(graph_versions) > 0:
            min_version = min([int(x) for x in graph_versions if x.isnumeric()])
            max_version = max([int(x) for x in graph_versions if x.isnumeric()])
        logger.debug(f"{graph_name} minimal version is: {min_version}")
        logger.debug(f"{graph_name} maximal version is: {max_version}")
        if len(matching_keys) >= graph_rotation:
            oldest_graph_version_to_delete = ModelUtil.build_graph_version_name(graph_name, min_version)
            self.r.delete(oldest_graph_version_to_delete)
            logger.debug(f"Graph {oldest_graph_version_to_delete} deleted")
        return max_version + 1


class ExportableGraphHandler(VersionedGraphHandler):
    """
    Class that handle Exportable Versioned Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, version: int, password: str = None, source_url: str = "", export_dir: str = "/"):
        super().__init__(host=host, port=port, name=name, version=version, password=password, source_url=source_url)
        logger.debug("ExportableGraphHandler init")
        if export_dir != "":
            Path(export_dir).mkdir(parents=True, exist_ok=True)
            self.export_dir = export_dir
        else:
            self.export_dir = self.default_export_dir
