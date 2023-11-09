# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging

from redisgraph_bulk_loader.bulk_insert import bulk_insert

from CosmoTech_Acceleration_Library.Modelops.core.common.graph_handler import GraphHandler

logger = logging.getLogger(__name__)


class ModelImporter(GraphHandler):
    """
    Model Exporter for cached data
    """

    @GraphHandler.handle_graph_replace
    def bulk_import(self, twin_file_paths: list = [], relationship_file_paths: list = [], enforce_schema: bool = False):
        """
        Import all csv data
        :param twin_file_paths: the file paths of all twin csv files
        :param relationship_file_paths: the file paths of all relationship csv files
        :param enforce_schema: True if the schema is defined within headers (default False)
        `Enforce_schema documentation <https://github.com/RedisGraph/redisgraph-bulk-loader#input-schemas>`_
        :return: Csv files containing all twin instances exported into {export_dir} folder named by twin type
        """
        command_parameters = ['--host', self.host, '--port', self.port]

        if enforce_schema:
            command_parameters.append('--enforce-schema')

        for twin_file_path in twin_file_paths:
            if twin_file_path != "":
                command_parameters.append('--nodes')
                command_parameters.append(twin_file_path)

        for relationship_file_path in relationship_file_paths:
            if relationship_file_path != "":
                command_parameters.append('--relations')
                command_parameters.append(relationship_file_path)

        command_parameters.append(self.graph.name)
        logger.debug(command_parameters)

        if self.password is not None:
            command_parameters.append('--password')
            command_parameters.append(self.password)
        # TODO: Think about use '--index Label:Property' command parameters to create indexes on default id properties
        try:
            bulk_insert(command_parameters)
        except SystemExit as e:
            print(e)
