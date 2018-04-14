#!/usr/bin/env python

from os.path import exists

import versioneer
from setuptools import setup

with open('requirements.txt') as f:
    install_requires = f.read().strip().split('\n')

if exists('README.rst'):
    with open('README.rst') as f:
        long_description = f.read()
else:
    long_description = ''

setup(name='dask-jobqueue',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='Deploy Dask on job queuing systems like PBS or SLURM',
      url='https://github.com/dask/dask-jobqueue',
      license='BSD 3-Clause',
      packages=['dask_jobqueue'],
      install_requires=install_requires,
      tests_require=['pytest >= 2.7.1'],
      long_description=long_description,
      zip_safe=False)
