# Copyright (c) Cosmo Tech corporation.
# Licensed under the MIT license.
import setuptools

setuptools.setup(
    name='CosmoTech_Acceleration_Library',
    version='0.0.1',
    packages=setuptools.find_packages(),
    url='https://github.com/Cosmo-Tech/CosmoTech-Acceleration-Library',
    license='MIT',
    author='afossart',
    author_email='alexis.fossart@cosmotech.com',
    description='  Acceleration library for CosmoTech cloud based solution development',
    install_requires=[
        'azure-identity',
        'cosmotech_api @ git+https://github.com/Cosmo-Tech/cosmotech-api-python-client.git#egg=cosmotech_api',
    ]
)
