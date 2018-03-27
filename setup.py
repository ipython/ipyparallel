#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='dask-jobqueue',
      version='0.1.0',
      description='Deploy Dask on job queuing systems like PBS and SLURM',
      url='https://github.com/dask/dask-jobqueue',
      license='BSD 3-Clause',
      packages=['dask_jobqueue'],
      install_requires=open('requirements.txt').read().strip().split('\n'),
      long_description=(open('README.rst').read() if exists('README.rst') else ''),
      zip_safe=False)
