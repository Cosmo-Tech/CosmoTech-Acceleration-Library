# Example: Working with datasets in the CosmoTech API
from cosmotech.coal.cosmotech_api.apis import DatasetApi

dataset_id = "my_dataset_id"

# Use Coal configuration to setup connection and DatasetApi object
dataset_api = DatasetApi()

# Upload a single file as a dataset
dataset_api.upload_dataset(
    dataset_id=dataset_id,
    file_path="/tmp/data/customers.csv",
)

# Upload multiple parts from a folder (one part per file)
dataset_api.upload_dataset_parts(
    dataset_id=dataset_id,
    folder_path="/tmp/data/parts/",
)

# Download a dataset to a local directory
dataset_api.download_dataset(
    dataset_id=dataset_id,
)
