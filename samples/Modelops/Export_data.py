# Copyright (C) - 2023 - 2025 - Cosmo Tech
# Licensed under the MIT license.
import glob
import os

from CosmoTech_Acceleration_Library.Modelops.core.io.model_exporter import ModelExporter

export_directory = "/tmp/export/"


def clean_export_directory():
    print(f"Clean files before export")
    existing_files = glob.glob(f"{export_directory}/*.csv")
    for f in existing_files:
        os.remove(f)


if __name__ == '__main__':
    clean_export_directory()
    me = ModelExporter(host='localhost', port=6379, name='SampleGraphImportedFromCSVWithoutSchema', version=1,
                       export_dir=export_directory)
    me.export_all_data()
    exported_files = glob.glob(f"{export_directory}/*.csv")
    print(exported_files)
