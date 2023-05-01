# CosmoTech-Acceleration-Library
Acceleration library for CosmoTech cloud based solution development

## Code organisation

In project root directory you'll find 4 main directories:

* CosmoTech_Acceleration_Library: containing all Cosmo Tech libraries to accelerate interaction with Cosmo Tech solutions
* data: a bunch of csv files on which samples are based
* samples: a bunch of python scripts to demonstrate how to use the library
* doc: for schema or specific documentation

## Accelerators

TODO

## Modelops library

The aim of this library is to simplify the model accesses via python code.

The library can be used by Data Scientists, Modelers, Developers, ...

### Utility classes

* `ModelImporter(host: str, port: int, name: str, version: int, graph_rotation:int = 1)` : will allow you to bulk import data from csv files with schema enforced (`samples/Modelops/Bulk_Import_from_CSV_with_schema.py`) or not (`samples/Modelops/Bulk_Import_from_CSV_without_schema.py`) (see [documentation](https://github.com/RedisGraph/redisgraph-bulk-loader#input-schemas) for further details)
* `ModelExporter(host: str, port: int, name: str, version: int, export_dir: str = '/')` : will allow you to export data from a model cache instance
* `ModelReader(host: str, port: int, name: str, version: int)` : will allow you to read data from a model cache instance ([object returned](https://github.com/RedisGraph/redisgraph-py/blob/master/redisgraph/query_result.py))
* `ModelWriter(host: str, port: int, name: str, version: int, graph_rotation:int = 1)` : will allow you to write data into a model instance
* `ModelUtil` : a bunch of utilities to manipulate and facilitate interaction with model instance (result_set_to_json, print_query_result, ... )
* `ModelMetadata`: will allow you to management graph metadata

## How-to

`python setup.py install --user`

## Setting up a development environment with Docker
* Make sure you have [Docker installed](https://docs.docker.com/get-docker/) on your machine.
* Clone the repository to your local machine using Git. For example, you can run the following command in your terminal: 
`git clone https://github.com/Cosmo-Tech/CosmoTech-Acceleration-Library.git`
* Navigate to the project directory:
`cd CosmoTech-Acceleration-Library`
* Create a Docker container:
* For Windows:
`CMD: docker run -it --name cosmotech-acceleration-library-dev -w /opt -v %cd%:/opt python:3.11 bash`
`PoweShell: docker run -it --name cosmotech-acceleration-library-dev -w /opt -v ${PWD}:/opt python:3.11 bash`
* For Linux:
`docker run -it --name cosmotech-acceleration-library-dev -w /opt -v $(PWD):/opt python:3.11 bash`
* Install the required dependencies:
`pip install -r requirements.txt`
* Install the pytest package:
`pip install pytest`
* Run the unit tests to make sure everything is working properly:
`pytest`
* Starts a Docker container 
`docker container start -i cosmotech-acceleration-library-dev`



