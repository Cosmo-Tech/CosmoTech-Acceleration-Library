---
description: "Helpers"
---
# **cosmotech.coal.cosmotech_api**

::: cosmotech.coal.cosmotech_api.apis.DatasetApi
    options:
      members:
        - download_dataset
        - download_parameter
        - path_to_parts
        - upload_dataset
        - upload_dataset_parts

<br/>

::: cosmotech.coal.cosmotech_api.apis.RunApi
    options:
      members:
        - get_run_metadata

<br/>

::: cosmotech.coal.cosmotech_api.apis.RunnerApi
    options:
      members:
        - get_runner_metadata
        - download_runner_data

<br/>

::: cosmotech.coal.cosmotech_api.apis.WorkspaceApi
    options:
      members:
        - list_filtered_workspace_files
        - download_workspace_file
        - upload_workspace_file

<br/>

::: cosmotech.coal.cosmotech_api.objects.Connection
    options:
      members:
        - get_api_client

<br/>

::: cosmotech.coal.cosmotech_api.objects.Parameters
    options:
      members:
        - format_parameters_list
        - write_parameters_to_json
        - write_parameters_to_csv
        - write_parameters
