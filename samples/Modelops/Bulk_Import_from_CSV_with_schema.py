# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
from CosmoTech_Acceleration_Library.Modelops.core.io.model_importer import ModelImporter


def run_bulk_import_from_csv_files_with_schema():
    mi = ModelImporter(host='localhost', port=6379, name='SampleGraphImportedFromCSVWithSchema')
    mi.bulk_import(twin_file_paths=['../../data/Modelops/enforce_schema/Bar.csv', '../../data/Modelops/enforce_schema/Customer.csv'],
                   relationship_file_paths=['../../data/Modelops/enforce_schema/arc_to_Customer.csv',
                                            '../../data/Modelops/enforce_schema/contains_Customer.csv'], enforce_schema=True)


if __name__ == '__main__':
    run_bulk_import_from_csv_files_with_schema()
