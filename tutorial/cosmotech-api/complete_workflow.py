# Example: Complete workflow using the CosmoTech API
import csv
import json
import os
import pathlib

from cosmotech.coal.cosmotech_api.apis import DatasetApi, RunnerApi, WorkspaceApi
from cosmotech.coal.utils.configuration import Configuration
from cosmotech.coal.utils.logger import LOGGER

os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

organization_id = "your-organization-id"
workspace_id = "your-workspace-id"
runner_id = "your-runner-id"
output_dataset_id = "your-output-dataset-id"

workflow_dir = pathlib.Path("./workflow_example")
input_dir = workflow_dir / "input"
processed_dir = workflow_dir / "processed"
output_dir = workflow_dir / "output"
for d in (input_dir, processed_dir, output_dir):
    d.mkdir(exist_ok=True, parents=True)

# Build a Configuration scoped to this runner
config = Configuration()
config.cosmotech.organization_id = organization_id
config.cosmotech.workspace_id = workspace_id
config.cosmotech.runner_id = runner_id
config.cosmotech.parameters_absolute_path = str(input_dir / "parameters")
config.cosmotech.dataset_absolute_path = str(input_dir / "datasets")
pathlib.Path(config.cosmotech.parameters_absolute_path).mkdir(exist_ok=True, parents=True)
pathlib.Path(config.cosmotech.dataset_absolute_path).mkdir(exist_ok=True, parents=True)

# Step 1: Download runner parameters and datasets
print("\n=== Step 1: Download Runner Data ===")
runner_api = RunnerApi(config)
runner_api.download_runner_data(download_datasets=True)
print(f"Runner data downloaded to {input_dir}")

# Step 2: Process the data
print("\n=== Step 2: Process Data ===")

customers_file = pathlib.Path(config.cosmotech.dataset_absolute_path) / "customers.csv"
if not customers_file.exists():
    print("Creating sample customers.csv for demonstration")
    with open(customers_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "city", "spending"])
        writer.writerow(["c1", "Alice", "30", "New York", "1500"])
        writer.writerow(["c2", "Bob", "25", "San Francisco", "2000"])
        writer.writerow(["c3", "Charlie", "35", "Chicago", "1200"])

customers = []
with open(customers_file, "r") as f:
    for row in csv.DictReader(f):
        row["loyalty_score"] = str(
            round(int(row["spending"]) / 100 + (int(row["age"]) - 20) / 10, 1)
        )
        customers.append(row)

processed_file = processed_dir / "customers_with_loyalty.csv"
with open(processed_file, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=list(customers[0].keys()))
    writer.writeheader()
    writer.writerows(customers)

print(f"Processed data written to {processed_file}")

# Step 3: Upload the processed file to the workspace
print("\n=== Step 3: Upload Processed Data to Workspace ===")
try:
    ws_api = WorkspaceApi(config)
    uploaded_name = ws_api.upload_workspace_file(
        organization_id,
        workspace_id,
        str(processed_file),
        "processed_data/",
        overwrite=True,
    )
    print(f"Uploaded as: {uploaded_name}")
except Exception as e:
    print(f"Error uploading file: {e}")

# Step 4: Update a dataset with the processed output
print("\n=== Step 4: Update Output Dataset ===")
try:
    dataset_api = DatasetApi(config)
    dataset_api.upload_dataset(
        organization_id=organization_id,
        dataset_id=output_dataset_id,
        file_path=str(processed_file),
    )
    print(f"Dataset {output_dataset_id} updated")
except Exception as e:
    print(f"Error updating dataset: {e}")

# Step 5: Generate a summary report
print("\n=== Step 5: Generate Report ===")
avg_loyalty = sum(float(c["loyalty_score"]) for c in customers) / len(customers)
report = {
    "runner_id": runner_id,
    "statistics": {
        "total_customers": len(customers),
        "average_loyalty_score": round(avg_loyalty, 1),
    },
    "top_customers": sorted(customers, key=lambda c: float(c["loyalty_score"]), reverse=True)[:2],
}

report_file = output_dir / "customer_report.json"
with open(report_file, "w") as f:
    json.dump(report, f, indent=2)

print(f"Report saved to {report_file}")
print(f"Total customers: {report['statistics']['total_customers']}")
print(f"Avg loyalty score: {report['statistics']['average_loyalty_score']}")
print("\nWorkflow completed successfully!")

