# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import logging

import redis

logger = logging.getLogger(__name__)


class RedisHandler:
    """
    Class that handle Redis informations
    """

    def __init__(self, host: str, port: int, name: str):
        logger.debug("RedisHandler init")
        self.host = host
        self.port = port
        self.name = name
        self.r = redis.Redis(host=host, port=port, decode_responses=True)
        self.metadata_key = name + "MetaData"
