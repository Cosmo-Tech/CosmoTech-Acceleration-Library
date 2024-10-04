# CosmoTech-Acceleration-Library
Acceleration library for CosmoTech cloud based solution development

## csm-data

`csm-data` is a CLI made to give CosmoTech solution modelers and integrators accelerators to start interacting with multiple systems.

It gives a first entrypoint to get ready to use commands to send and retrieve data from a number of systems in which a Cosmo Tech API could be integrated

## data-store

The data store gives a way to keep local data during simulations and comes with `csm-data` commands to easily send those data to a target system allowing to easily send results anywhere.


# Legacy part
The following description is tied to the legacy part of CoAL that is getting slowly moved to the new code organization before a 1.0.0 release

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
