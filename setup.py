#!/usr/bin/env python
# coding: utf-8

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

# the name of the project
name = 'ipyparallel'

#-----------------------------------------------------------------------------
# Minimal Python version sanity check
#-----------------------------------------------------------------------------

import sys

v = sys.version_info
if v[:2] < (2, 7) or (v[0] >= 3 and v[:2] < (3, 4)):
    error = "ERROR: %s requires Python version 2.7 or 3.4 or above." % name
    print(error, file=sys.stderr)
    sys.exit(1)

PY3 = (sys.version_info[0] >= 3)

#-----------------------------------------------------------------------------
# Add test command
#-----------------------------------------------------------------------------

from distutils.cmd import Command

class IPTestCommand(Command):
    description = "Run unit tests using iptest"
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        from IPython.testing import iptest
        old_argv = sys.argv
        sys.argv = ['iptest', 'ipyparallel.tests']
        iptest.run_iptest()
        sys.argv = old_argv

from setuptools.command.bdist_egg import bdist_egg

class bdist_egg_disabled(bdist_egg):
    """Disabled version of bdist_egg

    Prevents setup.py install performing setuptools' default easy_install,
    which it should never ever do.
    """
    def run(self):
        sys.exit("Aborting implicit building of eggs. Use `pip install .` to install from source.")

#-----------------------------------------------------------------------------
# get on with it
#-----------------------------------------------------------------------------

import os
from glob import glob

from setuptools import setup

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))
pkg_root = pjoin(here, name)

packages = []
for d, _, _ in os.walk(pjoin(here, name)):
    if os.path.exists(pjoin(d, '__init__.py')):
        packages.append(d[len(here)+1:].replace(os.path.sep, '.'))

package_data = {'ipyparallel.nbextension': [pjoin('static', '*')]}

data_files = [
    (
        'etc/jupyter/jupyter_notebook_config.d',
        [pjoin('etc', 'ipyparallel-serverextension.json')],
    ),
    (
        'etc/jupyter/nbconfig/tree.d',
        [pjoin('etc', 'ipyparallel-nbextension.json')],
    ),
    (
        'share/jupyter/nbextensions/ipyparallel',
        glob(pjoin('ipyparallel', 'nbextension', 'static', '*')),
    ),
]

version_ns = {}
with open(pjoin(here, name, '_version.py')) as f:
    exec(f.read(), {}, version_ns)


setup_args = dict(
    name=name,
    version=version_ns["__version__"],
    packages=packages,
    package_data=package_data,
    description="Interactive Parallel Computing with IPython",
    long_description="""Use multiple instances of IPython in parallel, interactively.
    
    See https://ipyparallel.readthedocs.io for more info.
    """,
    author="IPython Development Team",
    author_email="ipython-dev@scipy.org",
    url="http://ipython.org",
    license="BSD",
    platforms="Linux, Mac OS X, Windows",
    keywords=["Interactive", "Interpreter", "Shell", "Parallel"],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
    ],
    cmdclass={
        "test": IPTestCommand,
        "bdist_egg": bdist_egg if "bdist_egg" in sys.argv else bdist_egg_disabled,
    },
    data_files=data_files,
    install_requires=[
        "ipython_genutils",
        "decorator",
        "pyzmq>=13",
        "traitlets>=4.3",
        "ipython>=4",
        "jupyter_client",
        "ipykernel>=4.4",
        "tornado>=4",
        "python-dateutil>=2.1",
    ],
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*",
    extras_require={
        ':python_version == "2.7"': ["futures"],
        "nbext": ["notebook"],
        "test": ["pytest", "pytest-cov", "ipython[test]", "testpath", "mock"],
    },
    entry_points={
        "console_scripts": [
            "ipcluster = ipyparallel.apps.ipclusterapp:launch_new_instance",
            "ipcontroller = ipyparallel.apps.ipcontrollerapp:launch_new_instance",
            "ipengine = ipyparallel.apps.ipengineapp:launch_new_instance",
        ]
    },
)


if __name__ == "__main__":
    setup(**setup_args)
