"""joblib parallel backend for IPython Parallel"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import absolute_import

from joblib.parallel import AutoBatchingMixin
from joblib.parallel import ParallelBackendBase

import ipyparallel as ipp


class IPythonParallelBackend(AutoBatchingMixin, ParallelBackendBase):
    def __init__(self, view=None, **kwargs):
        super().__init__(**kwargs)
        self._cluster_owner = False
        self._client_owner = False
        if view is None:
            self._client_owner = True
            try:
                # load the default cluster
                cluster = ipp.Cluster.from_file()
            except FileNotFoundError:
                # other load errors?
                cluster = self._cluster = ipp.Cluster()
                self._cluster_owner = True
                cluster.start_cluster_sync()
            else:
                # cluster running, ensure some engines are, too
                if not cluster.engines:
                    cluster.start_engines_sync()
            rc = cluster.connect_client_sync()
            rc.wait_for_engines(cluster.n or 1)
            view = rc.load_balanced_view()

            # use cloudpickle or dill for closures, if available.
            # joblib tends to create closures default pickle can't handle.
            try:
                import cloudpickle  # noqa
            except ImportError:
                try:
                    import dill  # noqa
                except ImportError:
                    pass
                else:
                    view.client[:].use_dill()
            else:
                view.client[:].use_cloudpickle()
        self._view = view

    def effective_n_jobs(self, n_jobs):
        """A View can run len(view) jobs at a time"""
        return len(self._view)

    def terminate(self):
        """Close the client if we created it"""
        if self._client_owner:
            self._view.client.close()
        if self._cluster_owner:
            self._cluster.stop_cluster_sync()

    def apply_async(self, func, callback=None):
        """Schedule a func to be run"""
        future = self._view.apply_async(func)
        if callback:
            future.add_done_callback(lambda f: callback(f.result()))
        return future
