# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import setuptools
import cosmotech.acceleration_library
from pathlib import Path


with open('requirements.txt') as f:
    required = f.read().splitlines()


root_directory = Path(__file__).parent
readme_text = (root_directory / "README.md").read_text()

setuptools.setup(
    name='CosmoTech_Acceleration_Library',
    version=cosmotech.acceleration_library.__version__,
    packages=setuptools.find_namespace_packages(include=['cosmotech.*']),
    url='https://github.com/Cosmo-Tech/CosmoTech-Acceleration-Library',
    license='MIT',
    author='afossart',
    author_email='alexis.fossart@cosmotech.com',
    description='Acceleration libraries for CosmoTech cloud based solution development',
    long_description=readme_text,
    long_description_content_type='text/markdown',
    install_requires=required,
    entry_points={
        'console_scripts': [
            'cosmotech_scenario_downloader=cosmotech.acceleration_library.Accelerators.scenario_download.download_command:main'
        ]
    },
)
