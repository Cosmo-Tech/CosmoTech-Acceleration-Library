# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.
from CosmoTech_Acceleration_Library.Modelops.core.io.model_importer import ModelImporter


def run_bulk_import_from_csv_files_without_schema():
    mi2 = ModelImporter(host='localhost', port=6379, name='SampleGraphImportedFromCSVWithoutSchema')
    mi2.bulk_import(twin_file_paths=['../../data/Modelops/without_schema/Bar.csv', '../../data/Modelops/without_schema/Customer.csv'],
                    relationship_file_paths=['../../data/Modelops/without_schema/arc_to_Customer.csv',
                                             '../../data/Modelops/without_schema/contains_Customer.csv'])


if __name__ == '__main__':
    run_bulk_import_from_csv_files_without_schema()
