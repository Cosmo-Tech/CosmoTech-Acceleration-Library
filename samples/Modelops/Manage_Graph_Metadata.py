# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
from CosmoTech_Acceleration_Library.Modelops.core.io.model_writer import ModelWriter

if __name__ == '__main__':
    mw = ModelWriter(host='localhost', port=6379, name='newGraph')
    mw.create_twin("MyTwin", {"dt_id": "1234", "property1": "This is a property", "property2List": ["This is a list "
                                                                                                    "element",
                                                                                                    "This is another "
                                                                                                    "list element"]})
    graph_metadata = mw.m_metadata.get_metadata()
    get_last_graph_version = mw.m_metadata.get_last_graph_version()
    get_graph_name = mw.m_metadata.get_graph_name()
    get_graph_source_url = mw.m_metadata.get_graph_source_url()
    get_graph_rotation = mw.m_metadata.get_graph_rotation()
    get_last_modified_date = mw.m_metadata.get_last_modified_date()
    print(f"############## Metadata :")
    print(f"Metadata : {graph_metadata}")
    print(f"graph_name : {get_graph_name}")
    print(f"last_graph_version : {get_last_graph_version}")
    print(f"graph_source_url : {get_graph_source_url}")
    print(f"graph_rotation : {get_graph_rotation}")
    print(f"last_modified_date : {get_last_modified_date}")
