# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import json
import logging

from datetime import datetime
from redis.commands.graph.edge import Edge
from redis.commands.graph.node import Node
from redis.commands.helpers import stringify_param_value
from redis.commands.graph.query_result import QueryResult

logger = logging.getLogger(__name__)


class ModelUtil:
    """
    Utility class for Redis management
    """

    # ADT variables
    source_key = 'source'
    target_key = 'target'
    id_key = 'id'

    # Redis/Csm variables
    src_key = 'src'
    dest_key = 'dest'
    dt_id_key = 'id'

    @staticmethod
    def dict_to_cypher_parameters(parameters: dict) -> str:
        """
        Convert a dict to usable Cypher parameters object
        :param parameters: parameters dict
        :return: string representing parameters as Cyper Parameters
        """

        cypher_list = []
        for key, value in parameters.items():
            formatted_value = stringify_param_value(value)
            if isinstance(value, str):
                try:
                    json.loads(value)
                    formatted_value = json.dumps(value)
                except ValueError:
                    logger.debug(f"{value} is not a jsonString, use the raw value")
            cypher_list.append(f"{key} : {formatted_value}")
        joined_list = ', '.join(cypher_list)
        return '{' + joined_list + '}'

    @staticmethod
    def create_index_query(entity_name: str, entity_property_name: str) -> str:
        """
        Create an index query
        :param entity_name: the entity name on which you want to define an index
        :param entity_property_name:  the entity property name on which you want to define an index
        :return: the create index query
        """
        return f"CREATE INDEX ON :{entity_name}({entity_property_name})"

    @staticmethod
    def create_twin_query(twin_type: str, properties: dict) -> str:
        """
        Create a twin query
        :param twin_type:the future twin name
        :param properties: the properties of the twin
        :return: the create twin query
        """
        if ModelUtil.dt_id_key in properties:
            cypher_params = ModelUtil.dict_to_cypher_parameters(properties)
            return f"CREATE (:{twin_type} {cypher_params})"
        raise Exception(f"When you create a twin, you should define at least {ModelUtil.dt_id_key} properties ")

    @staticmethod
    def create_relationship_query(relationship_type: str, properties: dict) -> str:
        """
        Create a relationship query
        :param relationship_type: the future relationship name
        :param properties: the properties of the relationship (should contain 'src' and 'dest' properties)
        :return: the create relationship query
        """

        if ModelUtil.src_key in properties and ModelUtil.dest_key in properties:
            cypher_params = ModelUtil.dict_to_cypher_parameters(properties)
            return f"MATCH (n), (m) WHERE n.{ModelUtil.dt_id_key} = '{properties.get(ModelUtil.src_key)}' " \
                   f"AND m.{ModelUtil.dt_id_key} = '{properties.get(ModelUtil.dest_key)}' " \
                   f"CREATE (n)-[r:{relationship_type} {cypher_params}]->(m) RETURN r"
        raise Exception(
            f"When you create a relationship, you should define at least {ModelUtil.src_key} and {ModelUtil.dest_key} properties "
        )

    @staticmethod
    def dict_to_json(obj: dict) -> str:
        """
        Transform a dict to a json string
        :param obj: the dict
        :return: the json string corresponding
        """
        return json.dumps(obj, indent=2)

    @staticmethod
    def result_set_to_json(query_result: QueryResult) -> list:
        """
        Transform a QueryResult object to a json string list
        :param query_result: the QueryResult object
        :return: the json string list
        """
        flattened_headers = [item for sublist in query_result.header for item in sublist]
        headers_without_integers = [x for x in flattened_headers if not isinstance(x, int)]
        result_list = []
        for result in query_result.result_set:
            result_dict = {}
            for i in range(len(headers_without_integers)):
                obj = result[i]
                if isinstance(obj, Edge) or isinstance(obj, Node):
                    result_dict[headers_without_integers[i]] = obj.properties
                else:
                    result_dict[headers_without_integers[i]] = obj
            result_list.append(ModelUtil.dict_to_json(result_dict))
        return result_list

    @staticmethod
    def print_query_result(query_result: QueryResult) -> None:
        """
        Pretty print a QueryResult
        :param query_result: the QueryResult to print
        """
        list_to_print = ModelUtil.result_set_to_json(query_result)
        for result in list_to_print:
            print(result)

    @staticmethod
    def convert_datetime_to_str(date: datetime) -> str:
        """
        Convert a datetime to a str
        :param date: the datetime
        :return: the string representing the datetime
        """
        return date.strftime('%Y/%m/%d - %H:%M:%S')

    @staticmethod
    def convert_str_to_datetime(date_str: str) -> datetime:
        """
        Convert a datetime to a str
        :param date_str: the str representing a date
        :return: the datetime corresponding to date_str
        """
        date_time_obj = datetime.strptime(date_str, '%Y/%m/%d - %H:%M:%S')
        return date_time_obj

    @staticmethod
    def build_graph_key_pattern(graph_name: str) -> str:
        return graph_name + ":*"
