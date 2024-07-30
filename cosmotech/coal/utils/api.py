# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json
import pathlib
from typing import Optional

import cosmotech_api
import yaml
from cosmotech_api.api.solution_api import Solution
from cosmotech_api.api.solution_api import SolutionApi
from cosmotech_api.api.workspace_api import Workspace
from cosmotech_api.api.workspace_api import WorkspaceApi
from cosmotech_api.exceptions import ServiceException

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER


def read_solution_file(solution_file) -> Optional[Solution]:
    solution_path = pathlib.Path(solution_file)
    if solution_path.suffix in [".yaml", ".yml"]:
        open_function = yaml.safe_load
    elif solution_path.suffix == ".json":
        open_function = json.load
    else:
        LOGGER.error(f"{solution_file} is not a `.yaml` or `.json` file")
        return None
    with solution_path.open() as _sf:
        solution_content = open_function(_sf)
    LOGGER.info(f"Loaded {solution_path.absolute()}")
    _solution = Solution(_configuration=cosmotech_api.Configuration(),
                         _spec_property_naming=True,
                         **solution_content)
    LOGGER.debug(json.dumps(_solution.to_dict(), indent=2, default=str))
    return _solution


def get_solution(organization_id, workspace_id) -> Optional[Solution]:
    LOGGER.info("Configuration to the api set")
    with get_api_client()[0] as api_client:
        api_w = WorkspaceApi(api_client)

        LOGGER.info("Loading Workspace information to get Solution ID")
        try:
            r_data: Workspace = api_w.find_workspace_by_id(organization_id=organization_id, workspace_id=workspace_id)
        except ServiceException as e:
            LOGGER.error(f"Workspace [green bold]{workspace_id}[/] was not found "
                         f"in Organization [green bold]{organization_id}[/]")
            LOGGER.debug(e.body)
            return None
        solution_id = r_data.solution.solution_id

        api_sol = SolutionApi(api_client)
        sol: Solution = api_sol.find_solution_by_id(organization_id=organization_id, solution_id=solution_id)
    return sol
