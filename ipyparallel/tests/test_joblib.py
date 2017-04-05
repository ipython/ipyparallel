try:
    from unittest import mock
except ImportError:
    import mock

import pytest

import ipyparallel as ipp
from .clienttest import ClusterTestCase, add_engines

try:
    import joblib
    from joblib import Parallel, delayed
    from ipyparallel.client._joblib import IPythonParallelBackend
except ImportError:
    have_joblib = False
else:
    have_joblib = True

def neg(x):
    return -1 * x

class TestJobLib(ClusterTestCase):
    def setUp(self):
        if not have_joblib:
            pytest.skip("Requires joblib >= 0.10")
        super(TestJobLib, self).setUp()
        add_engines(1, total=True)

    def test_default_backend(self):
        """ipyparallel.register_joblib_backend() registers default backend"""
        ipp.register_joblib_backend()
        with mock.patch('ipyparallel.Client', lambda : self.client):
            p = Parallel(backend='ipyparallel')
            assert p._backend._view.client is self.client
        
        self.client[:].use_pickle()
    
    def test_register_backend(self):
        view = self.client.load_balanced_view()
        view.register_joblib_backend('view')
        p = Parallel(backend='view')
        self.assertIs(p._backend._view, view)

    def test_joblib_backend(self):
        view = self.client.load_balanced_view()
        view.register_joblib_backend('view')
        p = Parallel(backend='view')
        result = p(delayed(neg)(i) for i in range(10))
        self.assertEqual(result, [neg(i) for i in range(10)])
