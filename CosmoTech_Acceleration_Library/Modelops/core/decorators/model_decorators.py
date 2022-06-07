# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import functools

from CosmoTech_Acceleration_Library.Modelops.core.common.graph_handler import GraphHandler, ExportableGraphHandler


def update_last_version(function):
    """
    Function decorator that update metadata last version after calling the function annotated
    :param function: the function annotated
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        self = args[0]
        if isinstance(self, GraphHandler):
            function(*args, **kwargs)
            self.m_metadata.update_last_version()
        else:
            function(*args, **kwargs)
    return wrapper


def update_last_modified_date(function):
    """
    Function decorator that update metadata last modified date after calling the function annotated
    :param function: the function annotated
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        self = args[0]
        if isinstance(self, GraphHandler):
            function(*args, **kwargs)
            self.m_metadata.update_last_modified_date()
        else:
            function(*args, **kwargs)

    return wrapper


def do_if_graph_exist(function):
    """
    Function decorator that run the function annotated if versioned graph exists
    :param function: the function annotated
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        self = args[0]
        version_graph_name = self.versioned_name
        if isinstance(self, ExportableGraphHandler):
            key_count = self.r.exists(version_graph_name)
            if key_count != 0:
                function(*args, **kwargs)
            else:
                raise Exception(f"{version_graph_name} does not exist!")
        else:
            function(*args, **kwargs)
    return wrapper
