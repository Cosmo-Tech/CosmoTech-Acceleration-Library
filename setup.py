# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import setuptools
from pathlib import Path

VERSION = "0.2.1"

with open('requirements.txt') as f:
    required = f.read().splitlines()


root_directory = Path(__file__).parent
readme_text = (root_directory / "README.md").read_text()

setuptools.setup(
    name='CosmoTech_Acceleration_Library',
    version=VERSION,
    packages=setuptools.find_packages(),
    url='https://github.com/Cosmo-Tech/CosmoTech-Acceleration-Library',
    license='MIT',
    author='afossart',
    author_email='alexis.fossart@cosmotech.com',
    description='Acceleration libraries for CosmoTech cloud based solution development',
    long_description=readme_text,
    long_description_content_type='text/markdown',
    install_requires=required,
)
