# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import time
from pathlib import Path
import redis
from functools import lru_cache

from CosmoTech_Acceleration_Library.Modelops.core.common.graph_handler import GraphHandler
from CosmoTech_Acceleration_Library.Modelops.core.common.writer.CsvWriter import CsvWriter
from CosmoTech_Acceleration_Library.Modelops.core.io.model_reader import ModelReader

logger = logging.getLogger(__name__)


class ModelExporter(GraphHandler):
    """
    Model Exporter for cached data
    """

    def __init__(self, host: str, port: int, name: str, password: str = None, export_dir: str = "/"):
        super().__init__(host=host, port=port, name=name, password=password)
        Path(export_dir).mkdir(parents=True, exist_ok=True)
        self.export_dir = export_dir

        self.mr = ModelReader(host=host, port=port, name=name, password=password)
        self.labels = [label[0] for label in self.graph.labels()]
        self.relationships = [relation[0] for relation in self.graph.relationship_types()]
        self.already_exported_nodes = {}
        self.already_exported_edges = []

    @GraphHandler.do_if_graph_exist
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
            twin_results = self.mr.get_twins_by_type(twin_name)
            get_twin_info_end = time.time() - get_twin_info_start
            logger.debug(f"Get twin info for type {twin_name} took {get_twin_info_end} s")

            logger.debug(f"Export twin info for type {twin_name} ...")
            export_twin_info_start = time.time()
            CsvWriter.write_twin_data(self.export_dir, twin_name, twin_results)
            export_twin_info_end = time.time() - export_twin_info_start
            logger.debug(f"Export twin info for type {twin_name} took {export_twin_info_end} s")

            logger.debug(f"Twins exported :{twin_name}")
        logger.debug("... End exporting twins")

    @GraphHandler.do_if_graph_exist
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
            relationship_result = self.mr.get_relationships_by_type(relationship_name)
            get_relationship_info_end = time.time() - get_relationship_info_start
            logger.debug(f"Get relationship info for type {relationship_name} took {get_relationship_info_end} s")

            logger.debug(f"Export relationship info for type {relationship_name} ...")
            export_relationship_info_start = time.time()
            CsvWriter.write_relationship_data(self.export_dir, relationship_name, relationship_result)
            export_relationship_info_end = time.time() - export_relationship_info_start
            logger.debug(f"Export relationship info for type {relationship_name} took {export_relationship_info_end} s")

            logger.debug(f"Relationships exported :{relationship_name}")
        logger.debug("... End exporting relationships")

    @GraphHandler.do_if_graph_exist
    def export_all_data(self):
        """
        Export all data
        :return: a bunch of csv files corresponding to graph data
        """
        self.export_all_twins()
        self.export_all_relationships()

    @GraphHandler.do_if_graph_exist
    def export_from_queries(self, queries: list):
        """
        Export data from queries
        Queries must be Cypher queries and return nodes and relationships objects to be exported
        Multiple instances of the same node or relationship will not be exported

        :param queries: list of queries to execute (Cypher queries)
        :return: None writes csv files corresponding to the results of the queries in the parameters
        """
        logger.info("Start exporting data from queries...")
        # foreach query, execute it and get nodes and relationships
        for query in queries:
            logger.info(f"Export data from query {query} ...")
            export_data_from_query_start = time.time()
            query_result = self.mr.query(query, read_only=True)

            # foreach query result, get nodes and relationships
            nodes_by_label = {key: [] for key in self.labels}
            edges_by_relation = {key: [] for key in self.relationships}
            for result in query_result.result_set:
                for data in result:
                    if type(data) == redis.commands.graph.node.Node:
                        if data.id not in self.already_exported_nodes:
                            self.already_exported_nodes.update({data.id: data.properties.get('id')})
                            nodes_by_label[data.label].append(data)
                    elif type(data) == redis.commands.graph.edge.Edge:
                        if data.id not in self.already_exported_edges:
                            self.already_exported_edges.append(data.id)
                            edges_by_relation[data.relation].append(data)

            # write node data into csv file
            for label, nodes in nodes_by_label.items():
                if nodes:
                    nodes_rows = [node.properties for node in nodes]
                    CsvWriter.write_data(self.export_dir, label, nodes_rows)

            # write edge data into csv file
            for relation, edges in edges_by_relation.items():
                if edges:
                    # add source and target to edge properties
                    edges_rows = []
                    for edge in edges:
                        logger.debug(f"Get source and target for edge {edge.id} ...")
                        edge.properties['source'] = self.get_node_id_from_sys_id(edge.src_node)
                        edge.properties['target'] = self.get_node_id_from_sys_id(edge.dest_node)
                        edges_rows.append(edge.properties)
                    CsvWriter.write_data(self.export_dir, relation, edges_rows)

            export_data_from_query_end = time.time() - export_data_from_query_start
            logger.debug(f"Export data from query took {export_data_from_query_end} s")

            logger.debug("Data from query exported")
        logger.info("... End exporting data from queries")

    @lru_cache
    def get_node_id_from_sys_id(self, sys_id: int) -> int:
        """
        Get node id from system id (RedisGraph id)
        :param sys_id: system id
        :return: node id
        """
        if sys_id in self.already_exported_nodes:
            return self.already_exported_nodes[sys_id]
        node_query = "MATCH (n) WHERE ID(n) = $id RETURN n.id"
        return self.mr.query(node_query, params={'id': sys_id}).result_set[0][0]
