# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import setuptools

VERSION = "0.0.8"

with open('requirements.txt') as f:
    required = f.read().splitlines()

setuptools.setup(
    name='CosmoTech_Acceleration_Library',
    version=VERSION,
    packages=setuptools.find_packages(),
    url='https://github.com/Cosmo-Tech/CosmoTech-Acceleration-Library',
    license='MIT',
    author='afossart',
    author_email='alexis.fossart@cosmotech.com',
    description='Acceleration library for CosmoTech cloud based solution development',
    install_requires=required
)
