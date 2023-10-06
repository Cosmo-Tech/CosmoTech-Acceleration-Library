# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging

import redis

logger = logging.getLogger(__name__)


class RedisHandler:
    """
    Class that handle Redis informations
    """

    def __init__(self, host: str, port: int, name: str, password: str = None):
        logger.debug("RedisHandler init")
        self.host = host
        self.port = port
        self.name = name
        self.password = password
        self.r = redis.Redis(host=host, port=port, password=password, decode_responses=True)
