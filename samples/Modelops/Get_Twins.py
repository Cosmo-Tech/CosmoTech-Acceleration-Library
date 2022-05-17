# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
from CosmoTech_Acceleration_Library.Modelops.core.io.model_reader import ModelReader
from CosmoTech_Acceleration_Library.Modelops.core.utils.model_util import ModelUtil


def get_and_display_twins_info(twin_type: str):
    twin_properties = mr.get_twin_properties_by_type(twin_type)
    print(f"{twin_type} properties: {twin_properties}")
    twins = mr.get_twins_by_type(twin_type)
    print(f"Twins '{twin_type}':")
    ModelUtil.print_query_result(twins)


if __name__ == '__main__':
    mr = ModelReader(host='localhost', port=6379, name='SampleGraphImportedFromCSVWithoutSchema', version=1)
    twin_types = mr.get_twin_types()
    for twin_type in twin_types:
        get_and_display_twins_info(twin_type)
