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

    def __init__(self, host: str, port: int, name: str, password: str = None, source_url: str = "", graph_rotation: int = 3):
        super().__init__(host=host, port=port, name=name, password=password)
        logger.debug("GraphHandler init")
        self.graph = self.r.graph(name)
        self.name = name
        self.m_metadata = ModelMetadata(host=host, port=port, name=name, password=password)
        current_metadata = self.m_metadata.get_metadata()
        if not current_metadata:
            if graph_rotation is None:
                graph_rotation = 3
            logger.debug("Create metadata key")
            self.m_metadata.set_metadata(last_graph_version=0, graph_source_url=source_url,
                                         graph_rotation=graph_rotation)


class VersionedGraphHandler(GraphHandler):
    """
    Class that handle Versioned Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, version: int = None, password: str = None, source_url: str = "",
                 graph_rotation: int = None):
        super().__init__(host=host, port=port, name=name, password=password, source_url=source_url,
                         graph_rotation=graph_rotation)
        logger.debug("VersionedGraphHandler init")
        self.version = version
        if version is None:
            self.version = self.m_metadata.get_last_graph_version()
        self.versioned_name = ModelUtil.build_graph_version_name(self.name, self.version)
        self.graph = self.r.graph(self.versioned_name)


class RotatedGraphHandler(VersionedGraphHandler):
    """
    Class that handle Rotated Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, password: str = None, version: int = None, source_url: str = "",
                 graph_rotation: int = None):
        super().__init__(host=host, port=port, name=name, password=password, source_url=source_url, version=version,
                         graph_rotation=graph_rotation)
        logger.debug("RotatedGraphHandler init")
        self.graph_rotation = self.m_metadata.get_graph_rotation()

    def get_all_versions(self):
        matching_graph_keys = self.r.keys(ModelUtil.build_graph_key_pattern(self.name))
        versions = []
        for graph_key in matching_graph_keys:
            versions.append(graph_key.split(":")[-1])
        return versions

    def handle_graph_rotation(func):
        """
        Decorator to do stuff then handle graph rotation (delete the oldest graph if the amount of graph is greater than graph rotation)
        """

        def handle(self, *args, **kwargs):
            graph_versions = self.get_all_versions()

            breakpoint()
            if len(graph_versions) > 0:
                max_version = max([int(x) for x in graph_versions if x.isnumeric()])
            else:
                max_version = 0
            # upgrade current graph to max_version+1
            self.version = max_version + 1
            self.version_name = ModelUtil.build_graph_version_name(self.name, self.version)
            self.graph = self.r.graph(self.version_name)
            logger.debug(f'Using graph updated version {self.version_name}')

            # do function on new graph
            func(self, *args, **kwargs)

            # get max version to manage case func not using (hence creating) graph
            graph_versions = [int(v) for v in self.get_all_versions()]
            graph_versions.sort()
            graph_versions.reverse()
            to_remove = graph_versions[int(self.graph_rotation):]

            # remove all older versions
            for v in to_remove:
                oldest_graph_version_to_delete = ModelUtil.build_graph_version_name(self.name, v)
                self.r.delete(oldest_graph_version_to_delete)
                logger.debug(f"Graph {oldest_graph_version_to_delete} deleted")

            # upgrade metadata last version to +1 after function execution
            self.m_metadata.set_last_graph_version(self.version)

        return handle


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
