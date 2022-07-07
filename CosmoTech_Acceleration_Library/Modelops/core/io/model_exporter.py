# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import time

from CosmoTech_Acceleration_Library.Modelops.core.common.graph_handler import ExportableGraphHandler
from CosmoTech_Acceleration_Library.Modelops.core.common.writer.CsvWriter import CsvWriter
from CosmoTech_Acceleration_Library.Modelops.core.decorators.model_decorators import do_if_graph_exist
from CosmoTech_Acceleration_Library.Modelops.core.io.model_reader import ModelReader

logger = logging.getLogger(__name__)


class ModelExporter(ExportableGraphHandler):
    """
    Model Exporter for cached data
    """

    def __init__(self, host: str, port: int, name: str, version: int, password: str = None, export_dir: str = "/"):
        super().__init__(host=host, port=port, name=name, version=version, password=password, export_dir=export_dir)
        self.mr = ModelReader(host=host, port=port, name=name, password=password, version=version)

    @do_if_graph_exist
    def export_all_twins(self):
        """
        Export all twins
        :return: Csv files containing all twin instances exported into {export_dir} folder named by twin type
        """
        logger.debug("Start exporting twins...")
        logger.debug("Get twin types...")
        get_types_start = time.time()
        twin_names = self.mr.get_twin_types()
        get_types_end = time.time() - get_types_start
        logger.debug(f"Get twin types took {get_types_end} s")

        for twin_name in twin_names:
            logger.debug(f"Get twin info for type {twin_name} ...")
            get_twin_info_start = time.time()
            headers = self.mr.get_twin_properties_by_type(twin_name)
            twin_results = self.mr.get_twins_by_type(twin_name)
            get_twin_info_end = time.time() - get_twin_info_start
            logger.debug(f"Get twin info for type {twin_name} took {get_twin_info_end} s")
            logger.debug(f"Export twin info for type {twin_name} ...")
            export_twin_info_start = time.time()
            CsvWriter.write_twin_data(self.export_dir, twin_name, twin_results, headers)
            export_twin_info_end = time.time() - export_twin_info_start
            logger.debug(f"Export twin info for type {twin_name} took {export_twin_info_end} s")
            logger.debug(f"Twins exported :{twin_name}")
        logger.debug("... End exporting twins")

    @do_if_graph_exist
    def export_all_relationships(self):
        """
        Export all relationships
        :return: Csv files containing all relationship instances exported into {export_dir}
        folder named by relationship type
        """
        logger.debug("Start exporting relationships...")
        logger.debug("Get relationship types...")
        get_relationship_types_start = time.time()
        relationship_names = self.mr.get_relationship_types()
        get_relationship_types_end = time.time() - get_relationship_types_start
        logger.debug(f"Get relationship types took {get_relationship_types_end} s")

        for relationship_name in relationship_names:
            logger.debug(f"Get relationship info for type {relationship_name} ...")
            get_relationship_info_start = time.time()
            headers = self.mr.get_relationship_properties_by_type(relationship_name)
            relationship_result = self.mr.get_relationships_by_type(relationship_name)
            get_relationship_info_end = time.time() - get_relationship_info_start
            logger.debug(f"Get relationship info for type {relationship_name} took {get_relationship_info_end} s")
            logger.debug(f"Export relationship info for type {relationship_name} ...")
            export_relationship_info_start = time.time()
            CsvWriter.write_relationship_data(self.export_dir, relationship_name, relationship_result, headers)
            export_relationship_info_end = time.time() - export_relationship_info_start
            logger.debug(f"Export relationship info for type {relationship_name} took {export_relationship_info_end} s")
            logger.debug(f"Relationships exported :{relationship_name}")
        logger.debug("... End exporting relationships")

    @do_if_graph_exist
    def export_all_data(self):
        """
        Export all data
        :return: a bunch of csv files corresponding to graph data
        """
        self.export_all_twins()
        self.export_all_relationships()
