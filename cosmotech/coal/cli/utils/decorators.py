# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import os
import webbrowser
from functools import wraps

from cosmotech.coal.utils import WEB_DOCUMENTATION_ROOT
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.utils.logger import LOGGER


def require_env(envvar, envvar_desc):
    def wrap_function(func):
        @wraps(func)
        def f(*args, **kwargs):
            if envvar not in os.environ:
                raise EnvironmentError(f"Missing the following environment variable: {envvar}")
            return func(*args, **kwargs)

        f.__doc__ = "\n".join(
            [f.__doc__ or "", f"Requires env var `{envvar:<15}` *{envvar_desc}*  "])
        return f

    return wrap_function


def web_help(effective_target="", base_url=WEB_DOCUMENTATION_ROOT):
    documentation_url = base_url + effective_target

    def open_documentation(ctx: click.Context, param, value):
        if value:
            if not webbrowser.open(documentation_url):
                LOGGER.warning(f"Failed to open: {documentation_url}")
            else:
                LOGGER.info(f"Opened {documentation_url} in your navigator")
            ctx.exit(0)

    def wrap_function(func):
        @wraps(func)
        @click.option("--web-help",
                      is_flag=True,
                      help="Open the web documentation",
                      is_eager=True,
                      callback=open_documentation)
        def f(*args, **kwargs):
            if kwargs.get('web_help'):
                return
            if 'web_help' in kwargs:
                del kwargs['web_help']
            return func(*args, **kwargs)

        return f

    return wrap_function
