import setuptools
from pprint import pprint
import logging

logging.info('setuptools packages', setuptools.find_packages())

setuptools.setup(
    name='censoredplanet-analysis',
    version='0.0.1',
    install_requires=['py-radix==0.10.0'],
    packages=setuptools.find_packages(),
)
