"""IPython parallel backend for joblib

To enable the default view as a backend for joblib::

    import ipyparallel as ipp
    ipp.register_joblib_backend()

Or to enable a particular View you have already set up::

    view.register_joblib_backend()

At this point, you can use it with::

    with parallel_backend('ipyparallel'):
        Parallel(n_jobs=2)(delayed(some_function)(i) for i in range(10))

.. versionadded:: 5.1
"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import absolute_import

from joblib.parallel import register_parallel_backend
from .client._joblib import IPythonParallelBackend

def register(name='ipyparallel', make_default=False):
    """Register the default ipyparallel Client as a joblib backend
    
    See joblib.parallel.register_parallel_backend for details.
    """
    return register_parallel_backend(name, IPythonParallelBackend, make_default=make_default)
