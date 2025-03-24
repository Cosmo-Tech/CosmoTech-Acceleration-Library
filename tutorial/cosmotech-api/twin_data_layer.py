# Example: Working with the Twin Data Layer in the CosmoTech API
import os
import pathlib
import csv
from cosmotech.coal.cosmotech_api.connection import get_api_client
from cosmotech.coal.cosmotech_api.twin_data_layer import CSVSourceFile
from cosmotech_api.api.twin_graph_api import TwinGraphApi
from cosmotech.coal.utils.logger import LOGGER

# Set up environment variables for authentication
os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

# Organization and workspace IDs
organization_id = "your-organization-id"  # Replace with your organization ID
workspace_id = "your-workspace-id"  # Replace with your workspace ID
twin_graph_id = "your-twin-graph-id"  # Replace with your twin graph ID

# Get the API client
api_client, connection_type = get_api_client()
LOGGER.info(f"Connected using: {connection_type}")

try:
    # Create a TwinGraphApi instance
    twin_graph_api = TwinGraphApi(api_client)

    # Example 1: Create sample CSV files for nodes and relationships

    # Create a directory for our sample data
    data_dir = pathlib.Path("./tdl_sample_data")
    data_dir.mkdir(exist_ok=True, parents=True)

    # Create a sample nodes CSV file (Person nodes)
    persons_file = data_dir / "Person.csv"
    with open(persons_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "age", "city"])
        writer.writerow(["p1", "Alice", "30", "New York"])
        writer.writerow(["p2", "Bob", "25", "San Francisco"])
        writer.writerow(["p3", "Charlie", "35", "Chicago"])

    # Create a sample relationships CSV file (KNOWS relationships)
    knows_file = data_dir / "KNOWS.csv"
    with open(knows_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["src", "dest", "since"])
        writer.writerow(["p1", "p2", "2020"])
        writer.writerow(["p2", "p3", "2021"])
        writer.writerow(["p3", "p1", "2019"])

    print(f"Created sample CSV files in {data_dir}")

    # Example 2: Parse CSV files and generate Cypher queries

    # Parse the nodes CSV file
    person_csv = CSVSourceFile(persons_file)
    print(f"Parsed {person_csv.object_type} CSV file:")
    print(f"  Is node: {person_csv.is_node}")
    print(f"  Fields: {person_csv.fields}")
    print(f"  ID column: {person_csv.id_column}")

    # Generate a Cypher query for creating nodes
    person_query = person_csv.generate_query_insert()
    print(f"\nGenerated Cypher query for {person_csv.object_type}:")
    print(person_query)

    # Parse the relationships CSV file
    knows_csv = CSVSourceFile(knows_file)
    print(f"\nParsed {knows_csv.object_type} CSV file:")
    print(f"  Is node: {knows_csv.is_node}")
    print(f"  Fields: {knows_csv.fields}")
    print(f"  Source column: {knows_csv.source_column}")
    print(f"  Target column: {knows_csv.target_column}")

    # Generate a Cypher query for creating relationships
    knows_query = knows_csv.generate_query_insert()
    print(f"\nGenerated Cypher query for {knows_csv.object_type}:")
    print(knows_query)

    # Example 3: Send data to the Twin Data Layer (commented out as it requires an actual twin graph)
    """
    # For nodes, you would typically:
    with open(persons_file, "r") as f:
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
                    "query": person_query,
                    "parameters": params
                }
            )
    
    # For relationships, you would typically:
    with open(knows_file, "r") as f:
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
                    "query": knows_query,
                    "parameters": params
                }
            )
    """

    # Example 4: Query data from the Twin Data Layer (commented out as it requires an actual twin graph)
    """
    # Execute a Cypher query to get all Person nodes
    result = twin_graph_api.run_twin_graph_cypher_query(
        organization_id=organization_id,
        workspace_id=workspace_id,
        twin_graph_id=twin_graph_id,
        twin_graph_cypher_query={
            "query": "MATCH (p:Person) RETURN p.id, p.name, p.age, p.city",
            "parameters": {}
        }
    )
    
    # Process the results
    print("\nPerson nodes in the Twin Data Layer:")
    for record in result.records:
        print(f"  - {record}")
    """

finally:
    # Always close the API client when done
    api_client.close()
