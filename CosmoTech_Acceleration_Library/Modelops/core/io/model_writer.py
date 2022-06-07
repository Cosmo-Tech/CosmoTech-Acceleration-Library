# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging

from CosmoTech_Acceleration_Library.Modelops.core.common.graph_handler import RotatedGraphHandler
from CosmoTech_Acceleration_Library.Modelops.core.decorators.model_decorators import update_last_modified_date
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil

logger = logging.getLogger(__name__)

class ModelWriter(RotatedGraphHandler):
    """
    Model Writer for cached data
    """

    @update_last_modified_date
    def create_twin(self, twin_type: str, properties: dict):
        """
        Create a twin
        :param twin_type: the twin type
        :param properties: the twin properties
        """
        create_query = ModelUtil.create_twin_query(twin_type, properties)
        logger.debug(f"Query: {create_query}")
        self.graph.query(create_query)

    @update_last_modified_date
    def create_relationship(self, relationship_type: str, properties: dict):
        """
        Create a relationship
        :param relationship_type: the relationship type
        :param properties: the relationship properties
        """
        create_rel = ModelUtil.create_relationship_query(relationship_type, properties)
        logger.debug(f"Query: {create_rel}")
        self.graph.query(create_rel)


