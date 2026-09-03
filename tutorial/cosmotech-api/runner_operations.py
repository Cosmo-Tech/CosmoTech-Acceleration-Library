# Example: Working with runners and runs in the CosmoTech API
from cosmotech.orchestrator.utils.logger import get_logger

from cosmotech.coal.cosmotech_api.apis import RunnerApi
from cosmotech.coal.utils.configuration import ENVIRONMENT_CONFIGURATION as EC

logger = get_logger("MyProject.runner_work")

# Use Coal configuration to setup connection and RunnerApi object
runner_api = RunnerApi()

# Example 1: Get runner metadata
metadata = runner_api.get_runner_metadata()
logger.info(f"Runner name:  {metadata.get('name')}")
logger.info(f"Runner state: {metadata.get('state')}")

# Optionally scope the returned fields:
# metadata = runner_api.get_runner_metadata(
#     include=["parametersValues", "datasetList"]
# )

# Example 2: Download runner parameters and datasets
runner_api.download_runner_data(download_datasets=True)
logger.info(f"Parameters saved to: {EC.cosmotech.parameter_absolute_path}")
logger.info(f"Datasets   saved to: {EC.cosmotech.dataset_asbsolute_path}")
