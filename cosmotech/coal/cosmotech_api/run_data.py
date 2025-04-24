# Copyright (C) - 2023 - 2025 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

"""
Run Data Service operations module.

This module provides functions for interacting with the Run Data Service,
including sending and loading data.
"""

import json
import pathlib
from csv import DictReader, DictWriter
from typing import Dict, List, Any, Optional, Set

from cosmotech_api import SendRunDataRequest, RunDataQuery
from cosmotech_api.api.run_api import RunApi

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.store.store import Store
from cosmotech.coal.store.native_python import convert_table_as_pylist
from cosmotech.coal.utils.logger import LOGGER
from cosmotech.orchestrator.utils.translate import T


def send_csv_to_run_data(
    source_folder: str,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    run_id: str,
) -> None:
    """
    Send CSV files to the Run Data Service.

    Args:
        source_folder: Folder containing CSV files
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        run_id: Run ID
    """
    source_dir = pathlib.Path(source_folder)

    if not source_dir.exists():
        LOGGER.error(T("coal.common.file_operations.not_found").format(source_folder=source_dir))
        raise FileNotFoundError(f"{source_dir} does not exist")

    with get_api_client()[0] as api_client:
        api_run = RunApi(api_client)
        for csv_path in source_dir.glob("*.csv"):
            with open(csv_path) as _f:
                dr = DictReader(_f)
                table_name = csv_path.name.replace(".csv", "")
                LOGGER.info(T("coal.cosmotech_api.run_data.sending_to_table").format(table_name=f"CD_{table_name}"))
                LOGGER.debug(T("coal.services.database.column_list").format(columns=dr.fieldnames))
                data = []

                for row in dr:
                    n_row = dict()
                    for k, v in row.items():
                        if isinstance(v, str):
                            try:
                                n_row[k] = json.loads(v)
                            except json.decoder.JSONDecodeError:
                                n_row[k] = v
                        else:
                            n_row[k] = v
                    data.append(n_row)

                LOGGER.info(T("coal.services.database.row_count").format(count=len(data)))
                api_run.send_run_data(
                    organization_id,
                    workspace_id,
                    runner_id,
                    run_id,
                    SendRunDataRequest(id=table_name, data=data),
                )


def send_store_to_run_data(
    store_folder: str,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    run_id: str,
) -> None:
    """
    Send store data to the Run Data Service.

    Args:
        store_folder: Folder containing the store
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        run_id: Run ID
    """
    source_dir = pathlib.Path(store_folder)

    if not source_dir.exists():
        LOGGER.error(T("coal.common.file_operations.not_found").format(source_folder=source_dir))
        raise FileNotFoundError(f"{source_dir} does not exist")

    with get_api_client()[0] as api_client:
        api_run = RunApi(api_client)
        _s = Store()
        for table_name in _s.list_tables():
            LOGGER.info(T("coal.cosmotech_api.run_data.sending_to_table").format(table_name=f"CD_{table_name}"))
            data = convert_table_as_pylist(table_name)
            if not len(data):
                LOGGER.info(T("coal.services.database.no_rows"))
                continue
            fieldnames = _s.get_table_schema(table_name).names
            for row in data:
                for field in fieldnames:
                    if row[field] is None:
                        del row[field]
            LOGGER.debug(T("coal.services.database.column_list").format(columns=fieldnames))
            LOGGER.info(T("coal.services.database.row_count").format(count=len(data)))
            api_run.send_run_data(
                organization_id,
                workspace_id,
                runner_id,
                run_id,
                SendRunDataRequest(id=table_name, data=data),
            )


def load_csv_from_run_data(
    target_folder: str,
    organization_id: str,
    workspace_id: str,
    runner_id: str,
    run_id: str,
    file_name: str = "results",
    query: str = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'",
) -> None:
    """
    Load data from the Run Data Service and save it as a CSV file.

    Args:
        target_folder: Folder to save the CSV file to
        organization_id: Organization ID
        workspace_id: Workspace ID
        runner_id: Runner ID
        run_id: Run ID
        file_name: Name of the CSV file to create
        query: SQL query to execute
    """
    target_dir = pathlib.Path(target_folder)
    target_dir.mkdir(parents=True, exist_ok=True)

    with get_api_client()[0] as api_client:
        api_run = RunApi(api_client)
        query_result = api_run.query_run_data(
            organization_id, workspace_id, runner_id, run_id, RunDataQuery(query=query)
        )
        if query_result.result:
            LOGGER.info(T("coal.services.database.query_results").format(count=len(query_result.result)))
            with open(target_dir / (file_name + ".csv"), "w") as _f:
                headers = set()
                for r in query_result.result:
                    headers = headers | set(r.keys())
                dw = DictWriter(_f, fieldnames=sorted(headers))
                dw.writeheader()
                dw.writerows(query_result.result)
            LOGGER.info(T("coal.services.database.saved_results").format(file=f"{target_dir / file_name}.csv"))
        else:
            LOGGER.info(T("coal.services.database.no_results"))
