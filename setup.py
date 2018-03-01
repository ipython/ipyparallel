#!/usr/bin/env python

from os.path import exists
from setuptools import setup

setup(name='dask-jobqueue',
      version='0.1.0',
      description='Deploy Dask on job queuing systems like PBS and SLURM',
      url='',
      license='',
      packages=['dask_jobqueue'],
      long_description=(open('README.rst').read() if exists('README.rst') else ''),
      zip_safe=False)
