# Copyright 2020 Jigsaw Operations LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Dependency setup for beam remote workers."""

import setuptools

setuptools.setup(
    name='censoredplanet-analysis',
    version='0.0.1',
    install_requires=[
        'certifi==2020.6.20',
        'cryptography==36.0.2',
        'geoip2==4.1.0',
        'pyasn==1.6.1'
    ],
    # Add a prefix so absolute import succeeds on workers.
    packages=['pipeline', 'pipeline.metadata'],
    package_dir={
        'pipeline.metadata': './metadata',
        'pipeline': './'
    },
    package_data={'pipeline': ['metadata/data/*']},
    url='https://github.com/censoredplanet/censoredplanet-analysis',
    author='Sarah Laplante',
    author_email='laplante@google.com')
