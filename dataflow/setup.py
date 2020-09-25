
#!/usr/bin/env python

"""
Setup script for stream-decode
"""

import codecs

from pipe_tools.beam.requirements import requirements as DATAFLOW_PINNED_DEPENDENCIES
from setuptools import find_packages
from setuptools import setup

package = __import__('stream_decode')

DEPENDENCIES = [
    "cython",
    "google-cloud-storage==1.22.0",
    "pipe-tools==3.1.2",
    "ais-tools==0.1.2-alpha"
]

with codecs.open('README.md', encoding='utf-8') as f:
    readme = f.read().strip()

with codecs.open('requirements.txt', encoding='utf-8') as f:
    DEPENDENCY_LINKS = [line for line in f]

setup(
    author=package.__author__,
    author_email=package.__email__,
    description=package.__doc__.strip(),
    include_package_data=True,
    license="Apache 2.0",
    long_description=readme,
    name='stream-decode',
    version='v0.0.1',
    url=package.__source__,
    zip_safe=True,
    install_requires=DEPENDENCIES + DATAFLOW_PINNED_DEPENDENCIES,
    packages=find_packages(exclude=['test*.*', 'tests']),
    dependency_links=DEPENDENCY_LINKS,
)
