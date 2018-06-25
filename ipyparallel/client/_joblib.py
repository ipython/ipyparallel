"""joblib parallel backend for IPython Parallel"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import absolute_import

import ipyparallel as ipp
from joblib.parallel import ParallelBackendBase, AutoBatchingMixin

class IPythonParallelBackend(AutoBatchingMixin, ParallelBackendBase):

    def __init__(self, view=None):
        super(IPythonParallelBackend, self).__init__()
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
        """A View can run len(view) jobs at a time"""
        return len(self._view)

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


