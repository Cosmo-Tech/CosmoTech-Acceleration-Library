# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging

from CosmoTech_Acceleration_Library.Modelops.core.common.graph_handler import GraphHandler
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil
from redis.commands.graph.query_result import QueryResult

logger = logging.getLogger(__name__)


class ModelReader(GraphHandler):
    """
    Model Reader for cached data
    """

    def get_twin_types(self) -> list:
        """
        Get twin types
        :return: twin types list
        """
        return [item for sublist in self.graph.labels() for item in sublist]

    def get_twins_by_type(self, twin_type: str, limit: int = 0) -> QueryResult:
        """
        Get twins by type
        :param twin_type: the twin type requested
        :param limit: the limit number of twin retrieved
        :return: the twin list corresponding to twin type parameter
        """
        twin_query = f'MATCH (node:{twin_type}) RETURN node'
        if limit != 0:
            twin_query = f'{twin_query} LIMIT {str(limit)}'
        logger.debug(f"Query : {twin_query}")
        return self.graph.query(twin_query, read_only=True)

    def get_twin_properties_by_type(self, twin_type: str) -> list:
        """
        Get twin properties regarding a twin_type
        Note: this will work if all twin (with the same type) have same properties set
        :param twin_type: the twin type
        :return: the properties list
        """
        result = []
        twin_result = self.get_twins_by_type(twin_type, 1)
        result_set = twin_result.result_set
        if result_set and result_set[0]:
            for key, val in result_set[0][0].properties.items():
                if str(key) != ModelUtil.dt_id_key:
                    result.append(str(key))
                else:
                    result.append(ModelUtil.id_key)
        return result

    def get_relationship_types(self) -> list:
        """
        Get relationship types
        :return: relationship types list
        """
        return [item for sublist in self.graph.relationship_types() for item in sublist]

    def get_relationships_by_type(self, relationship_type: str, limit: int = 0) -> QueryResult:
        """
        Get relationships by type
        :param relationship_type: the relationship type requested
        :param limit: the limit number of twin retrieved
        :return: the relationship list corresponding to relationship type parameter
        """
        rel_query = f'MATCH (n)-[relation:{relationship_type}]->(m) RETURN n.{ModelUtil.dt_id_key} as {ModelUtil.source_key}, ' \
                    f'm.{ModelUtil.dt_id_key} as {ModelUtil.target_key}, relation'
        if limit != 0:
            rel_query = f'{rel_query} LIMIT {str(limit)}'
        logger.debug(f"Query : {rel_query}")
        return self.graph.query(rel_query, read_only=True)

    def get_relationship_properties_by_type(self, relationship_type: str) -> list:
        """
        Get relationship properties regarding a relationship_type
        Note: this will work if all relationship (with the same type) have same properties set
        :param relationship_type: the relationship type
        :return: the properties list
        """
        result = [ModelUtil.source_key, ModelUtil.target_key]
        relationship_result = self.get_relationships_by_type(relationship_type, 1)
        result_set = relationship_result.result_set
        if result_set and result_set[0]:
            # relationship
            for key, val in result_set[0][2].properties.items():
                if not str(key) in result:
                    if str(key) == ModelUtil.dt_id_key:
                        result.append(ModelUtil.id_key)
                    elif str(key) != ModelUtil.src_key and str(key) != ModelUtil.dest_key:
                        result.append(str(key))
        return result

    def query(self, query: str, params: dict = None, timeout: int = None, read_only: bool = False) -> QueryResult:
        """
        Run specified query
        :param query: the query to run
        :param params: the parameters for the query if any
        :param timeout: a specific timeout
        :param read_only: executes a readonly query if set to True
        :return: the QueryResult corresponding to specified query
        """
        logger.debug(f"Query : {query} with params : {params}")
        return self.graph.query(q=query, params=params, timeout=timeout, read_only=read_only)

    def exists(self, key) -> bool:
        """
        Check if a key exists in Redis
        :param key: the key
        :return: True if exists else False
        """
        return False if self.r.exists(key) == 0 else True
