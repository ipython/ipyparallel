#!/usr/bin/env python

from os.path import exists

import versioneer
from setuptools import setup

with open("requirements.txt") as f:
    install_requires = f.read().strip().split("\n")

if exists("README.rst"):
    with open("README.rst") as f:
        long_description = f.read()
else:
    long_description = ""

setup(
    name="dask-jobqueue",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description="Deploy Dask on job queuing systems like PBS, Slurm, SGE or LSF",
    url="https://jobqueue.dask.org",
    python_requires=">=3.5",
    license="BSD 3-Clause",
    packages=["dask_jobqueue"],
    include_package_data=True,
    install_requires=install_requires,
    tests_require=["pytest >= 2.7.1"],
    long_description=long_description,
    zip_safe=False,
)
