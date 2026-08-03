# Example: Working with the Twin Data Layer via the CosmoTech API SDK
#
# NOTE: CoAL no longer provides Twin Data Layer helpers (CSVSourceFile,
# generate_query_insert, etc.). TDL operations must be performed directly
# through the cosmotech_api SDK's TwinGraphApi.
import csv
import os
import pathlib

from cosmotech_api.api.twin_graph_api import TwinGraphApi

from cosmotech.coal.cosmotech_api.objects.connection import Connection
from cosmotech.coal.utils.logger import LOGGER

os.environ["CSM_API_URL"] = "https://api.cosmotech.com"  # Replace with your API URL
os.environ["CSM_API_KEY"] = "your-api-key"  # Replace with your actual API key

organization_id = "your-organization-id"
workspace_id = "your-workspace-id"
twin_graph_id = "your-twin-graph-id"

connection = Connection()
twin_graph_api = TwinGraphApi(connection.api_client)

# Create sample CSV data for nodes and relationships
data_dir = pathlib.Path("./tdl_sample_data")
data_dir.mkdir(exist_ok=True, parents=True)

persons_file = data_dir / "Person.csv"
with open(persons_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "name", "age", "city"])
    writer.writerow(["p1", "Alice", "30", "New York"])
    writer.writerow(["p2", "Bob", "25", "San Francisco"])

knows_file = data_dir / "KNOWS.csv"
with open(knows_file, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["src", "dest", "since"])
    writer.writerow(["p1", "p2", "2020"])

# Example: send node rows to TDL using raw Cypher queries
create_person_query = (
    "MERGE (p:Person {id: $id}) "
    "SET p.name = $name, p.age = $age, p.city = $city"
)

"""
# Uncomment to run against an actual twin graph:
with open(persons_file, "r") as f:
    for row in csv.DictReader(f):
        twin_graph_api.run_twin_graph_cypher_query(
            organization_id=organization_id,
            workspace_id=workspace_id,
            twin_graph_id=twin_graph_id,
            twin_graph_cypher_query={"query": create_person_query, "parameters": row},
        )

create_knows_query = (
    "MATCH (a:Person {id: $src}), (b:Person {id: $dest}) "
    "MERGE (a)-[r:KNOWS]->(b) SET r.since = $since"
)

with open(knows_file, "r") as f:
    for row in csv.DictReader(f):
        twin_graph_api.run_twin_graph_cypher_query(
            organization_id=organization_id,
            workspace_id=workspace_id,
            twin_graph_id=twin_graph_id,
            twin_graph_cypher_query={"query": create_knows_query, "parameters": row},
        )
"""
