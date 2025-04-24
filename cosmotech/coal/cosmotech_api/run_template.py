# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Run Template operations module.

This module provides functions for interacting with Run Templates,
including downloading and extracting handlers.
"""

import pathlib
from io import BytesIO
from zipfile import BadZipfile, ZipFile
from typing import List

from cosmotech_api.api.solution_api import SolutionApi
from cosmotech_api.api.workspace_api import Workspace, WorkspaceApi
from cosmotech_api.exceptions import ServiceException

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def load_run_template_handlers(
    organization_id: str,
    workspace_id: str,
    run_template_id: str,
    handler_list: str,
) -> bool:
    """
    Download and extract run template handlers.

    Args:
        organization_id: Organization ID
        workspace_id: Workspace ID
        run_template_id: Run Template ID
        handler_list: Comma-separated list of handlers to download

    Returns:
        True if all handlers were downloaded successfully, False otherwise

    Raises:
        ValueError: If the workspace or solution is not found
    """
    has_errors = False
    with get_api_client()[0] as api_client:
        api_w = WorkspaceApi(api_client)

        LOGGER.info(T("coal.cosmotech_api.run_template.loading_solution"))
        try:
            r_data: Workspace = api_w.find_workspace_by_id(organization_id=organization_id, workspace_id=workspace_id)
        except ServiceException as e:
            LOGGER.error(
                T("coal.cosmotech_api.workspace.not_found").format(
                    workspace_id=workspace_id, organization_id=organization_id
                )
            )
            LOGGER.debug(T("coal.cosmotech_api.run_template.error_details").format(details=e.body))
            raise ValueError(f"Workspace {workspace_id} not found in organization {organization_id}")
        solution_id = r_data.solution.solution_id

        api_sol = SolutionApi(api_client)
        handler_list = handler_list.replace("handle-parameters", "parameters_handler")
        root_path = pathlib.Path("../csm_orc_port")
        template_path = root_path / run_template_id
        for handler_id in handler_list.split(","):
            handler_path: pathlib.Path = template_path / handler_id
            LOGGER.info(
                T("coal.cosmotech_api.run_template.querying_handler").format(
                    handler=handler_id, template=run_template_id
                )
            )
            try:
                rt_data = api_sol.download_run_template_handler(
                    organization_id=organization_id,
                    solution_id=solution_id,
                    run_template_id=run_template_id,
                    handler_id=handler_id,
                )
            except ServiceException as e:
                LOGGER.error(
                    T("coal.cosmotech_api.run_template.handler_not_found").format(
                        handler=handler_id,
                        template=run_template_id,
                        solution=solution_id,
                    )
                )
                LOGGER.debug(T("coal.cosmotech_api.run_template.error_details").format(details=e.body))
                has_errors = True
                continue
            LOGGER.info(T("coal.cosmotech_api.run_template.extracting_handler").format(path=handler_path.absolute()))
            handler_path.mkdir(parents=True, exist_ok=True)

            try:
                with ZipFile(BytesIO(rt_data)) as _zip:
                    _zip.extractall(handler_path)
            except BadZipfile:
                LOGGER.error(T("coal.cosmotech_api.run_template.handler_not_zip").format(handler=handler_id))
                has_errors = True
        if has_errors:
            LOGGER.error(T("coal.cosmotech_api.run_template.run_issues"))
            return False
        return True
