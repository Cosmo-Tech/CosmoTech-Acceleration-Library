# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.
from CosmoTech_Acceleration_Library.Modelops.core.io.model_reader import ModelReader
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil


def get_and_display_relationships_info(relationship_type: str):
    twin_properties = mr.get_relationship_properties_by_type(relationship_type)
    print(f"{relationship_type} properties: {twin_properties}")
    relationships = mr.get_relationships_by_type(relationship_type)
    print(f"Relationships '{relationship_type}':")
    ModelUtil.print_query_result(relationships)


if __name__ == '__main__':
    mr = ModelReader(host='localhost', port=6379, name='SampleGraphImportedFromCSVWithoutSchema', version=1)
    relationship_types = mr.get_relationship_types()
    for relationship_type in relationship_types:
        get_and_display_relationships_info(relationship_type)
