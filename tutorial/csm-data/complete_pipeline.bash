# 1. Download runner data from the API
csm-data api run-load-data \
  --organization-id "$CSM_ORGANIZATION_ID" \
  --workspace-id "$CSM_WORKSPACE_ID" \
  --runner-id "$CSM_RUNNER_ID" \
  --dataset-absolute-path "$CSM_DATASET_ABSOLUTE_PATH" \
  --parameters-absolute-path "$CSM_PARAMETERS_ABSOLUTE_PATH" \
  --write-json \
  --fetch-dataset

# 2. Load data into the datastore for processing
csm-data store load-csv-folder \
  --folder-path "$CSM_DATASET_ABSOLUTE_PATH" \
  --reset

# 3. Run your simulation (using your own code)
# ...

# 4. Send results to Azure Data Explorer for analysis
csm-data adx-send-runnerdata \
  --dataset-absolute-path "$CSM_DATASET_ABSOLUTE_PATH" \
  --parameters-absolute-path "$CSM_PARAMETERS_ABSOLUTE_PATH" \
  --runner-id "$CSM_RUNNER_ID" \
  --adx-uri "$AZURE_DATA_EXPLORER_RESOURCE_URI" \
  --adx-ingest-uri "$AZURE_DATA_EXPLORER_RESOURCE_INGEST_URI" \
  --database-name "$AZURE_DATA_EXPLORER_DATABASE_NAME" \
  --send-datasets \
  --wait
