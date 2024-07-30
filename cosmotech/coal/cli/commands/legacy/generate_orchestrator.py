# Copyright (C) - 2023 - 2024 - Cosmo Tech
# This document and all information contained herein is the exclusive property -
# including all intellectual property rights pertaining thereto - of Cosmo Tech.
# Any use, reproduction, translation, broadcasting, transmission, distribution,
# etc., to any person is prohibited unless it has been previously and
# specifically authorized by written means by Cosmo Tech.

import json

from cosmotech.orchestrator.core.orchestrator import Orchestrator
from cosmotech.orchestrator.core.step import Step
from cosmotech.orchestrator.utils.json import CustomJSONEncoder
from cosmotech_api.api.solution_api import RunTemplate
from cosmotech_api.api.solution_api import Solution

from cosmotech.coal.cli.utils.click import click
from cosmotech.coal.cli.utils.decorators import web_help
from cosmotech.coal.utils.api import get_solution
from cosmotech.coal.utils.api import read_solution_file
from cosmotech.coal.utils.logger import LOGGER


@click.group()
@web_help("csm-data/legacy/generate-orchestrator")
def generate_orchestrator():
    """Base command for the json generator using legacy files  
Check the help of the sub commands for more information:  
- `cloud` requires access to a fully deployed solution  
- `solution` requires a `Solution.yaml` file"""
    pass


@generate_orchestrator.command()
@click.argument("solution_file",
                type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
                required=True,
                nargs=1)
@click.argument("output",
                type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
                required=True,
                nargs=1)
@click.argument("run-template-id",
                required=True)
@click.option("--describe/--no-describe",
              show_default=True, default=False,
              help="Show a description of the generated template after generation")
@web_help("csm-data/legacy/generate-orchestrator/from-file")
def from_file(solution_file, run_template_id, output, describe):
    """Read SOLUTION_FILE to get a RUN_TEMPLATE_ID and generate an orchestrator file at OUTPUT"""
    if _solution := read_solution_file(solution_file):
        return generate_from_solution(sol=_solution, run_template_id=run_template_id, output=output, describe=describe)
    return 1


@generate_orchestrator.command()
@click.argument("output",
                type=click.Path(file_okay=True, dir_okay=False, readable=True, writable=True),
                required=True,
                nargs=1)
@click.option("--organization-id",
              envvar="CSM_ORGANIZATION_ID",
              show_envvar=True,
              help="The id of an organization in the cosmotech api",
              metavar="o-##########",
              required=True)
@click.option("--workspace-id",
              envvar="CSM_WORKSPACE_ID",
              show_envvar=True,
              help="The id of a solution in the cosmotech api",
              metavar="w-##########",
              required=True)
@click.option("--run-template-id",
              envvar="CSM_RUN_TEMPLATE_ID",
              show_envvar=True,
              help="The name of the run template in the cosmotech api",
              metavar="NAME",
              required=True)
@click.option("--describe/--no-describe",
              show_default=True, default=False,
              help="Show a description of the generated template after generation")
@web_help("csm-data/legacy/generate-orchestrator/from-api")
def from_api(workspace_id, organization_id, run_template_id, output, describe):
    """Connect to the cosmotech API to download a run template and generate an orchestrator file at OUTPUT"""

    if sol := get_solution(organization_id=organization_id,
                           workspace_id=workspace_id):
        return generate_from_solution(sol=sol, run_template_id=run_template_id, output=output, describe=describe)
    return 1


def generate_from_solution(sol: Solution, run_template_id, output: str, describe: bool = False):
    LOGGER.info(f"Searching [green bold]{run_template_id}[/] in the solution")
    if _t := [t for t in sol.run_templates if t.id == run_template_id]:
        template: RunTemplate = _t[0]
    else:
        LOGGER.error(f"Run template [green bold]{run_template_id}[/] was not found.")
        raise click.Abort()
    LOGGER.info(f"Found [green bold]{run_template_id}[/] in the solution generating json file")
    generate_from_template(template, output)
    if describe:
        f = Orchestrator()
        c, s, g = f.load_json_file(output, False, True)
        LOGGER.debug(g)
        for k, v in s.items():
            LOGGER.info(v[0])


def generate_from_template(template: RunTemplate, output: str):
    steps = []
    previous = None
    LOGGER.debug(template)
    if template.fetch_datasets is not False or template.fetch_scenario_parameters:
        LOGGER.info("- [green]fetch_scenario_parameters[/] step found")
        _s = Step(id="fetch_scenario_parameters",
                  commandId="csm-orc fetch-scenariorun-data",
                  stop_library_load=True)
        previous = "fetch_scenario_parameters"
        steps.append(_s)

    def run_template_phase(name, condition, source, _previous, default):
        _steps = []
        template_is_active = template.get(condition) if template.get(condition) is not None else default
        if template_is_active:
            if template.get(source) == "cloud":
                LOGGER.info(f"- [green]{name}_cloud[/] step found")
                _name = f"{name}_cloud"
                _step_dl_cloud = Step(id=_name,
                                      command="csm-orc",
                                      arguments=["fetch-cloud-steps"],
                                      useSystemEnvironment=True,
                                      environment={
                                          "CSM_ORGANIZATION_ID": {
                                              "description": "The id of an organization in the cosmotech api"
                                          },
                                          "CSM_WORKSPACE_ID": {
                                              "description": "The id of a workspace in the cosmotech api"
                                          },
                                          "CSM_RUN_TEMPLATE_ID": {
                                              "description": "The name of the run template in the cosmotech api",
                                              "value": template.id
                                          },
                                          "CSM_CONTAINER_MODE": {
                                              "description": "A list of handlers to download (comma separated)",
                                              "value": name
                                          },
                                          "CSM_API_URL": {
                                              "description": "The url to a Cosmotech API"
                                          },
                                          "CSM_API_SCOPE": {
                                              "description": "The identification scope of a Cosmotech API"
                                          },
                                          "AZURE_TENANT_ID": {
                                              "description": "An Azure Tenant ID"
                                          },
                                          "AZURE_CLIENT_ID": {
                                              "description": "An Azure Client ID having access to the Cosmotech API"
                                          },
                                          "AZURE_CLIENT_SECRET": {
                                              "description": "The secret for the Azure Client"
                                          },
                                          "LOG_LEVEL": {
                                              "description": "Either CRITICAL, ERROR, WARNING, INFO or DEBUG",
                                              "defaultValue": "INFO"
                                          },
                                      },
                                      )
                if _previous:
                    _step_dl_cloud.precedents = [_previous]
                _previous = _name
                _steps.append(_step_dl_cloud)
            LOGGER.info(f"- [green]{name}[/] step found")
            _run_step = Step(id=name,
                             commandId="csm-orc run-step",
                             environment={
                                 "CSM_RUN_TEMPLATE_ID": {
                                     "description": "The name of the run template defined in the cosmotech api "
                                                    "and available in the project",
                                     "value": template.id
                                 },
                                 "CSM_CONTAINER_MODE": {
                                     "description": "The steps run during a run-step",
                                     "value": name
                                 },
                             },
                             stop_library_load=True)
            if template.csm_simulation is not None:
                _run_step.environment["CSM_SIMULATION"].defaultValue = template.csm_simulation
            if _previous:
                _run_step.precedents = [_previous]
            _previous = name
            _steps.append(_run_step)
        return _previous, _steps

    previous, new_steps = run_template_phase("parameters_handler", "apply_parameters", "parameters_handler_source",
                                             previous, False)
    steps.extend(new_steps)
    previous, new_steps = run_template_phase("validator", "validate_data", "validator_source", previous, False)
    steps.extend(new_steps)
    if template.send_datasets_to_data_warehouse is True or template.send_input_parameters_to_data_warehouse is True:
        LOGGER.info("- [green]send_to_adx[/] step found")
        _send_to_adx_step = Step(id="send_to_adx",
                                 command="csm-orc",
                                 arguments=["send-to-adx"],
                                 useSystemEnvironment=True,
                                 environment={
                                     "AZURE_TENANT_ID": {
                                         "description": "An Azure Tenant ID"
                                     },
                                     "AZURE_CLIENT_ID": {
                                         "description": "An Azure Client ID having access to the Cosmotech API"
                                     },
                                     "AZURE_CLIENT_SECRET": {
                                         "description": "The secret for the Azure Client"
                                     },
                                     "LOG_LEVEL": {
                                         "description": "Either CRITICAL, ERROR, WARNING, INFO or DEBUG",
                                         "defaultValue": "INFO"
                                     },
                                     "CSM_DATASET_ABSOLUTE_PATH": {
                                         "description": "A local folder to store the main dataset content"
                                     },
                                     "CSM_PARAMETERS_ABSOLUTE_PATH": {
                                         "description": "A local folder to store the parameters content"
                                     },
                                     "CSM_SIMULATION_ID": {
                                         "description": "The id of the simulation run"
                                     },
                                     "AZURE_DATA_EXPLORER_RESOURCE_URI": {
                                         "description": "the ADX cluster path "
                                                        "(URI info can be found into ADX cluster page)"
                                     },
                                     "AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI": {
                                         "description": "The ADX cluster ingest path "
                                                        "(URI info can be found into ADX cluster page)"
                                     },
                                     "AZURE_DATA_EXPLORER_DATABASE_NAME": {
                                         "description": "The targeted database name"
                                     },
                                     "CSM_SEND_DATAWAREHOUSE_PARAMETERS": {
                                         "description": "whether or not to send parameters "
                                                        "(parameters path is mandatory then)",
                                         "defaultValue": json.dumps(
                                             template.send_input_parameters_to_data_warehouse is True)
                                     },
                                     "CSM_SEND_DATAWAREHOUSE_DATASETS": {
                                         "description": "whether or not to send datasets "
                                                        "(parameters path is mandatory then)",
                                         "defaultValue": json.dumps(
                                             template.send_datasets_to_data_warehouse is True)
                                     },
                                     "WAIT_FOR_INGESTION": {
                                         "description": "Toggle waiting for the ingestion results",
                                         "defaultValue": "false"
                                     },
                                 })
        if previous:
            _send_to_adx_step.precedents = [previous]
        previous = "send_to_adx"
        steps.append(_send_to_adx_step)
    previous, new_steps = run_template_phase("prerun", "pre_run", "pre_run_source", previous, False)
    steps.extend(new_steps)
    previous, new_steps = run_template_phase("engine", "run", "run_source", previous, True)
    steps.extend(new_steps)
    previous, new_steps = run_template_phase("postrun", "post_run", "post_run_source", previous, False)
    steps.extend(new_steps)
    LOGGER.debug(json.dumps({"steps": steps}, cls=CustomJSONEncoder, indent=2))
    LOGGER.info(f"{len(steps)} step{'s' if len(steps) > 1 else ''} found, writing json file")
    json.dump({"steps": steps}, open(output, "w"), cls=CustomJSONEncoder, indent=2)


if __name__ == "__main__":
    generate_orchestrator()
