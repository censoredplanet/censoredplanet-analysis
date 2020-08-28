"""Dependency setup for beam workers."""

import setuptools

setuptools.setup(
    name='censoredplanet-analysis',
    version='0.0.1',
    install_requires=['pyasn==1.6.0b1'],
    packages=setuptools.find_packages(),
    url='https://github.com/Jigsaw-Code/censoredplanet-analysis',
    author='Sarah Laplante',
    author_email='laplante@google.com',
)
