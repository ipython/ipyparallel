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
if v[:2] < (2,7) or (v[0] >= 3 and v[:2] < (3,3)):
    error = "ERROR: %s requires Python version 2.7 or 3.3 or above." % name
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

#-----------------------------------------------------------------------------
# get on with it
#-----------------------------------------------------------------------------

import os
from glob import glob

from distutils.core import setup

pjoin = os.path.join
here = os.path.abspath(os.path.dirname(__file__))
pkg_root = pjoin(here, name)

packages = []
for d, _, _ in os.walk(pjoin(here, name)):
    if os.path.exists(pjoin(d, '__init__.py')):
        packages.append(d[len(here)+1:].replace(os.path.sep, '.'))

package_data = {'ipyparallel.nbextension': [pjoin('static', '*')]}

version_ns = {}
with open(pjoin(here, name, '_version.py')) as f:
    exec(f.read(), {}, version_ns)


setup_args = dict(
    name            = name,
    version         = version_ns['__version__'],
    scripts         = glob(pjoin('scripts', '*')),
    packages        = packages,
    package_data    = package_data,
    description     = "Interactive Parallel Computing with IPython",
    long_description= """Use multiple instances of IPython in parallel, interactively.
    
    See https://ipyparallel.readthedocs.org for more info.
    """,
    author          = 'IPython Development Team',
    author_email    = 'ipython-dev@scipy.org',
    url             = 'http://ipython.org',
    license         = 'BSD',
    platforms       = "Linux, Mac OS X, Windows",
    keywords        = ['Interactive', 'Interpreter', 'Shell', 'Parallel'],
    classifiers     = [
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    cmdclass        = {
        'test': IPTestCommand,
    },
)

if 'develop' in sys.argv or any(bdist in sys.argv for bdist in ['bdist_wheel', 'bdist_egg']):
    import setuptools

setuptools_args = {}

install_requires = setuptools_args['install_requires'] = [
    'ipython_genutils',
    'decorator',
    'pyzmq>=13',
    'ipython>=4',
    'jupyter_client',
    'ipykernel',
    'tornado>=4',
]

extras_require = setuptools_args['extras_require'] = {
    ':python_version == "2.7"': ['futures'],
    'nbext': ["notebook"],
}

tests_require = setuptools_args['tests_require'] = [
    'nose',
    'ipython[test]',
    'mock',
]

if 'setuptools' in sys.modules:
    setup_args.update(setuptools_args)
    setup_args.pop('scripts')
    setup_args['entry_points'] = {
        'console_scripts': [
            'ipcluster = ipyparallel.apps.ipclusterapp:launch_new_instance',
            'ipcontroller = ipyparallel.apps.ipcontrollerapp:launch_new_instance',
            'ipengine = ipyparallel.apps.ipengineapp:launch_new_instance',
        ]
    }

if __name__ == '__main__':
    setup(**setup_args)
