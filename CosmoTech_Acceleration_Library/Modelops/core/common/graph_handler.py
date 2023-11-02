# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging
import functools

from CosmoTech_Acceleration_Library.Modelops.core.common.redis_handler import RedisHandler

logger = logging.getLogger(__name__)


class GraphHandler(RedisHandler):
    """
    Class that handle Graph Redis information
    """

    def __init__(self, host: str, port: int, name: str, password: str = None):
        super().__init__(host=host, port=port, name=name, password=password)
        logger.debug("GraphHandler init")
        self.name = name
        self.graph = self.r.graph(name)

    def do_if_graph_exist(function):
        """
        Function decorator that run the function annotated if graph exists
        :param function: the function annotated
        """

        @functools.wraps(function)
        def wrapper(self, *args, **kwargs):
            if self.r.exists(self.name) != 0:
                function(self, *args, **kwargs)
            else:
                raise Exception(f"{self.name} does not exist!")

        return wrapper

    def handle_graph_replace(func):
        """
        Decorator to do stuff then handle graph rotation (delete the oldest graph if the amount of graph is greater than graph rotation)
        """

        def handle(self, *args, **kwargs):
            self.graph = self.r.graph(f'{self.name}_tmp')
            logger.debug(f'Using graph {self.name}_tmp for copy')

            # do function on new graph
            func(self, *args, **kwargs)

            # action complete on graph_tmp with no error replacing graph by graph_tmp
            self.r.eval(
                """local o = redis.call('DUMP', KEYS[1]);\
                   redis.call('RENAME', KEYS[1], KEYS[2]);\
                   redis.call('RESTORE', KEYS[1], 0, o)""", 2, f'{self.name}_tmp', self.name)
            # remove tmp graph
            self.r.delete(f'{self.name}_tmp')
            # set back the graph
            self.graph = self.r.graph(self.name)

        return handle
