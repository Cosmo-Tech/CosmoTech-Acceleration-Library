# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.
from cosmotech.coal.cli.commands.legacy.generate_orchestrator import generate_orchestrator
from cosmotech.coal.cli.commands.legacy.init_local_parameter_folder import init_local_parameter_folder
from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help


@click.group()
@web_help("csm-data/legacy")
def legacy():
    """Cosmo Tech legacy API group

This group will allow you to connect to the CosmoTech API and migrate solutions from pre-3.0 version to 3.X compatible solutions
    """
    pass


legacy.add_command(init_local_parameter_folder, "init-local-parameter-folder")
legacy.add_command(generate_orchestrator, "generate-orchestrator")
