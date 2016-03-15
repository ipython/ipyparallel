"""joblib parallel backend for IPython Parallel"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import absolute_import

import ipyparallel as ipp
from joblib._parallel_backends import ParallelBackendBase, AutoBatchingMixin

class IPythonParallelBackend(AutoBatchingMixin, ParallelBackendBase):

    def __init__(self, view=None):
        if view is None:
            self._owner = True
            rc = ipp.Client()
            view = rc.load_balanced_view()
            # use cloudpickle or dill for closures, if available.
            # joblib tends to create closured default pickle can't handle.
            try:
                import cloudpickle
            except ImportError:
                try:
                    import dill
                except ImportError:
                    pass
                else:
                    view.client[:].use_dill()
            else:
                view.client[:].use_cloudpickle()
        else:
            self._owner = False
        self._view = view

    def effective_n_jobs(self, n_jobs):
        """Determine the number of jobs that can actually run in parallel.
        n_jobs is the is the number of workers requested by the callers.
        Passing n_jobs=-1 means requesting all available workers for instance
        matching the number of CPU cores on the worker host(s).
        This method should return a guesstimate of the number of workers that
        can actually perform work concurrently. The primary use case is to make
        it possible for the caller to know in how many chunks to slice the
        work.
        In general working on larger data chunks is more efficient (less
        scheduling overhead and better use of CPU cache prefetching heuristics)
        as long as all the workers have enough work to do.
        """
        if n_jobs == -1:
            return len(self._view)
        return min(len(self._view), n_jobs)

    def terminate(self):
        """Close the client if we created it"""
        if self._owner:
            self._view.client.close()

    def apply_async(self, func, callback=None):
        """Schedule a func to be run"""
        future = self._view.apply_async(func)
        if callback:
            future.add_done_callback(lambda f: callback(f.result()))
        return future


