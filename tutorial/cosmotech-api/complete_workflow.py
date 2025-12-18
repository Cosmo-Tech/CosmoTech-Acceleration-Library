# Example: Complete workflow using the CosmoTech API
import csv
import json
import os
import pathlib

from cosmotech_api.api.dataset_api import DatasetApi
from cosmotech_api.api.twin_graph_api import TwinGraphApi

from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.runner import (
    download_runner_data,
    get_runner_data,
)
from cosmotech.coal.cosmotech_api.twin_data_layer import CSVSourceFile
from cosmotech.coal.cosmotech_api.workspace import (
    download_workspace_file,
    list_workspace_files,
    upload_workspace_file,
)
from cosmotech.coal.utils.logger import LOGGER

# Set up environment variables for authentication
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

# Organization, workspace, and runner IDs
organization_id = "your-organization-id"  # Replace with your organization ID
workspace_id = "your-workspace-id"  # Replace with your workspace ID
runner_id = "your-runner-id"  # Replace with your runner ID
twin_graph_id = "your-twin-graph-id"  # Replace with your twin graph ID

# Create directories for our workflow
workflow_dir = pathlib.Path("./workflow_example")
workflow_dir.mkdir(exist_ok=True, parents=True)

input_dir = workflow_dir / "input"
processed_dir = workflow_dir / "processed"
output_dir = workflow_dir / "output"

input_dir.mkdir(exist_ok=True, parents=True)
processed_dir.mkdir(exist_ok=True, parents=True)
output_dir.mkdir(exist_ok=True, parents=True)

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

try:
    # Step 1: Download runner data (parameters and datasets)
    print("\n=== Step 1: Download Runner Data ===")

    runner_data = get_runner_data(organization_id, workspace_id, runner_id)
    print(f"Runner name: {runner_data.name}")

    result = download_runner_data(
        organization_id=organization_id,
        workspace_id=workspace_id,
        runner_id=runner_id,
        parameter_folder=str(input_dir / "parameters"),
        dataset_folder=str(input_dir / "datasets"),
        write_json=True,
        write_csv=True,
    )

    print(f"Downloaded runner data to {input_dir}")

    # Step 2: Process the data
    print("\n=== Step 2: Process Data ===")

    # For this example, we'll create a simple transformation:
    # - Read a CSV file from the input
    # - Transform it
    # - Write the result to the processed directory

    # Let's assume we have a "customers.csv" file in the input directory
    customers_file = input_dir / "datasets" / "customers.csv"

    # If the file doesn't exist for this example, create a sample one
    if not customers_file.exists():
        print("Creating sample customers.csv file for demonstration")
        customers_file.parent.mkdir(exist_ok=True, parents=True)
        with open(customers_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "name", "age", "city", "spending"])
            writer.writerow(["c1", "Alice", "30", "New York", "1500"])
            writer.writerow(["c2", "Bob", "25", "San Francisco", "2000"])
            writer.writerow(["c3", "Charlie", "35", "Chicago", "1200"])

    # Read the customers data
    customers = []
    with open(customers_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            customers.append(row)

    print(f"Read {len(customers)} customers from {customers_file}")

    # Process the data: calculate a loyalty score based on age and spending
    for customer in customers:
        age = int(customer["age"])
        spending = int(customer["spending"])

        # Simple formula: loyalty score = spending / 100 + (age - 20) / 10
        loyalty_score = round(spending / 100 + (age - 20) / 10, 1)
        customer["loyalty_score"] = str(loyalty_score)

    # Write the processed data
    processed_file = processed_dir / "customers_with_loyalty.csv"
    with open(processed_file, "w", newline="") as f:
        fieldnames = ["id", "name", "age", "city", "spending", "loyalty_score"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)

    print(f"Processed data written to {processed_file}")

    # Step 3: Upload the processed file to the workspace
    print("\n=== Step 3: Upload Processed Data to Workspace ===")

    try:
        uploaded_file = upload_workspace_file(
            api_client,
            organization_id,
            workspace_id,
            str(processed_file),
            "processed_data/",  # Destination in the workspace
            overwrite=True,
        )
        print(f"Uploaded processed file as: {uploaded_file}")
    except Exception as e:
        print(f"Error uploading file: {e}")

    # Step 4: Create a dataset from the processed data
    print("\n=== Step 4: Create Dataset from Processed Data ===")

    # This step would typically involve:
    # 1. Creating a dataset in the CosmoTech API
    # 2. Uploading files to the dataset

    """
    # Create a dataset
    dataset_api = DatasetApi(api_client)

    new_dataset = {
        "name": "Customers with Loyalty Scores",
        "description": "Processed customer data with calculated loyalty scores",
        "tags": ["processed", "customers", "loyalty"]
    }

    try:
        dataset = dataset_api.create_dataset(
            organization_id=organization_id,
            workspace_id=workspace_id,
            dataset=new_dataset
        )

        dataset_id = dataset.id
        print(f"Created dataset with ID: {dataset_id}")

        # Upload the processed file to the dataset
        # This would typically involve additional API calls
        # ...

    except Exception as e:
        print(f"Error creating dataset: {e}")
    """

    # Step 5: Send data to the Twin Data Layer
    print("\n=== Step 5: Send Data to Twin Data Layer ===")

    # Parse the processed CSV file for the Twin Data Layer
    customer_csv = CSVSourceFile(processed_file)

    # Generate a Cypher query for creating nodes
    customer_query = customer_csv.generate_query_insert()
    print(f"Generated Cypher query for Customer nodes:")
    print(customer_query)

    # In a real scenario, you would send this data to the Twin Data Layer
    """
    twin_graph_api = TwinGraphApi(api_client)

    # For each customer, create a node in the Twin Data Layer
    with open(processed_file, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Create parameters for the Cypher query
            params = {k: v for k, v in row.items()}

            # Execute the query
            twin_graph_api.run_twin_graph_cypher_query(
                organization_id=organization_id,
                workspace_id=workspace_id,
                twin_graph_id=twin_graph_id,
                twin_graph_cypher_query={
                    "query": customer_query,
                    "parameters": params
                }
            )
    """

    # Step 6: Generate a report
    print("\n=== Step 6: Generate Report ===")

    # Calculate some statistics
    total_customers = len(customers)
    avg_age = sum(int(c["age"]) for c in customers) / total_customers
    avg_spending = sum(int(c["spending"]) for c in customers) / total_customers
    avg_loyalty = sum(float(c["loyalty_score"]) for c in customers) / total_customers

    # Create a report
    report = {
        "report_date": "2025-02-28",
        "runner_id": runner_id,
        "statistics": {
            "total_customers": total_customers,
            "average_age": round(avg_age, 1),
            "average_spending": round(avg_spending, 2),
            "average_loyalty_score": round(avg_loyalty, 1),
        },
        "top_customers": sorted(customers, key=lambda c: float(c["loyalty_score"]), reverse=True)[
            :2
        ],  # Top 2 customers by loyalty score
    }

    # Write the report to a JSON file
    report_file = output_dir / "customer_report.json"
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)

    print(f"Report generated and saved to {report_file}")

    # Print a summary of the report
    print("\nReport Summary:")
    print(f"Total Customers: {report['statistics']['total_customers']}")
    print(f"Average Age: {report['statistics']['average_age']}")
    print(f"Average Spending: {report['statistics']['average_spending']}")
    print(f"Average Loyalty Score: {report['statistics']['average_loyalty_score']}")
    print("\nTop Customers by Loyalty Score:")
    for i, customer in enumerate(report["top_customers"], 1):
        print(f"{i}. {customer['name']} (Score: {customer['loyalty_score']})")

    print("\nWorkflow completed successfully!")

finally:
    # Always close the API client when done
    api_client.close()
