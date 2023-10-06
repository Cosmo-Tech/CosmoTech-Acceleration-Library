import pytest
import redis
import csv
import os

from redis.commands.graph import Node
from redis.commands.graph import Edge

from ..io.model_reader import ModelReader
from ..io.model_writer import ModelWriter
from ..io.model_importer import ModelImporter
from ..io.model_exporter import ModelExporter

GRAPH_NAME = 'test_graph'


def ping_redis(host, port):
    r = redis.Redis(host=host, port=port)
    return r.ping()


@pytest.fixture(scope='session')
def redis_service(docker_ip, docker_services):
    """ensure redis is up and running"""

    host = docker_ip
    port = docker_services.port_for("redis", 6379)
    redis_client = redis.Redis(host=host, port=port)

    docker_services.wait_until_responsive(timeout=5, pause=0.2, check=redis_client.ping)
    return {"host": host, "port": port}


@pytest.fixture
def redis_client(redis_service):
    return redis.Redis(redis_service["host"], redis_service["port"])


def test_redis(redis_client):
    r = redis_client
    assert r.ping()


@pytest.fixture
def redis_graph_setup(redis_client):
    graphs = []

    def _redis_graph_setup(name):
        g = redis_client.graph(f'{name}')
        graphs.append(g)
        return g

    yield _redis_graph_setup

    graphs[0].delete()


def test_io_model_reader(redis_graph_setup, redis_service):

    g = redis_graph_setup(GRAPH_NAME)
    node1 = Node(label="node", properties={"prop": "val"})
    g.add_node(node1)
    node2 = Node(label="node", properties={"prop": "val"})
    g.add_node(node2)
    rel1 = Edge(node1, "rel", node2, properties={"rel_prop": "val"})
    g.add_edge(rel1)
    g.flush()

    mr = ModelReader(redis_service['host'], redis_service['port'], GRAPH_NAME)
    assert mr.exists(g.name)

    # twin test
    assert ['node'] == mr.get_twin_types()

    result = mr.get_twins_by_type('node').result_set
    assert 2 == len(result)
    assert node1.label == result[0][0].label
    assert node1.properties == result[0][0].properties
    assert node2.label == result[1][0].label
    assert node2.properties == result[1][0].properties

    result = mr.get_twin_properties_by_type('node')
    assert ['prop'] == result

    # rel test
    assert ['rel'] == mr.get_relationship_types()

    result = mr.get_relationships_by_type('rel').result_set
    assert 1 == len(result)
    assert rel1.relation == result[0][2].relation
    assert rel1.properties == result[0][2].properties

    result = mr.get_relationship_properties_by_type('rel')
    assert ['source', 'target', 'rel_prop'] == result


def test_io_model_writer(redis_graph_setup, redis_service):

    mw = ModelWriter(redis_service['host'], redis_service['port'], GRAPH_NAME)
    mw.create_twin('node', {'id': 'node_id1', 'prop': 'val'})
    mw.create_twin('node', {'id': 'node_id2', 'prop': 'val'})
    mw.create_relationship('rel', {'src': 'node_id1', 'dest': 'node_id2', 'prop': 'val'})

    g = redis_graph_setup(GRAPH_NAME)
    assert [['node']] == g.labels()

    result = g.query("MATCH (n:node) return n").result_set
    assert 2 == len(result)
    assert 'node' == result[0][0].label
    assert {'id': 'node_id1', 'prop': 'val'} == result[0][0].properties
    assert 'node' == result[1][0].label
    assert {'id': 'node_id2', 'prop': 'val'} == result[1][0].properties

    result = g.query("MATCH ()-[r:rel]->() return r").result_set
    assert 1 == len(result)
    assert 'rel' == result[0][0].relation
    assert {'src': 'node_id1', 'dest': 'node_id2', 'prop': 'val'} == result[0][0].properties


def test_io_model_importer(redis_client, redis_graph_setup, redis_service, tmpdir):

    # create csv for import
    path_nodes = os.path.join(tmpdir, 'nodes.csv')
    with open(path_nodes, 'w') as f:
        csvw = csv.DictWriter(f, ['id', 'prop'])
        csvw.writeheader()
        csvw.writerow({'id': 'node_id1', 'prop': 'val'})
        csvw.writerow({'id': 'node_id2', 'prop': 'val'})

    path_edges = os.path.join(tmpdir, 'edges.csv')
    with open(path_edges, 'w') as f:
        csvw = csv.DictWriter(f, ['src', 'dest', 'prop'])
        csvw.writeheader()
        csvw.writerow({'src': 'node_id1', 'dest': 'node_id2', 'prop': 'val'})

    mi = ModelImporter(redis_service['host'], redis_service['port'], GRAPH_NAME)
    mi.bulk_import([path_nodes], [path_edges])
    # double call to validate replacement management
    mi.bulk_import([path_nodes], [path_edges])

    g = redis_graph_setup(GRAPH_NAME)
    result = g.query("MATCH (n:nodes) return n").result_set
    assert 2 == len(result)
    assert 'nodes' == result[0][0].label
    assert {'id': 'node_id1', 'prop': 'val'} == result[0][0].properties
    assert 'nodes' == result[1][0].label
    assert {'id': 'node_id2', 'prop': 'val'} == result[1][0].properties

    result = g.query("MATCH ()-[r:edges]->() return r").result_set
    assert 1 == len(result)
    assert 'edges' == result[0][0].relation
    assert {'prop': 'val'} == result[0][0].properties


def test_io_model_exporter(redis_graph_setup, redis_service, tmpdir):

    g = redis_graph_setup(GRAPH_NAME)
    node1 = Node(label="node", properties={"id": "node1", "prop": "val"})
    g.add_node(node1)
    node2 = Node(label="node", properties={"id": "node2", "prop": "val"})
    g.add_node(node2)
    rel1 = Edge(node1, "rel", node2, properties={"rel_prop": "val"})
    g.add_edge(rel1)
    g.flush()

    me = ModelExporter(redis_service['host'], redis_service['port'], GRAPH_NAME, export_dir=tmpdir)
    me.export_all_data()

    assert ['rel.csv', 'node.csv'] == os.listdir(tmpdir)

    with open(os.path.join(tmpdir, 'node.csv')) as f:
        csvr = csv.DictReader(f)
        assert set(['id', 'prop']) == set(csvr.fieldnames)
        rows = list(csvr)
        assert 2 == len(rows)

    with open(os.path.join(tmpdir, 'rel.csv')) as f:
        csvr = csv.DictReader(f)
        assert set(['source', 'target', 'rel_prop']) == set(csvr.fieldnames)
        rows = list(csvr)
        assert 1 == len(rows)
