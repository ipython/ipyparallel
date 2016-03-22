try:
    from unittest import mock
except ImportError:
    import mock

from nose import SkipTest

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
            raise SkipTest("Requires joblib >= 0.10")
        super(TestJobLib, self).setUp()
        add_engines(1, total=True)

    def test_default_backend(self):
        """ipyparallel.register_joblib_backend() registers default backend"""
        ipp.register_joblib_backend()
        with mock.patch('ipyparallel.Client', lambda : self.client):
            p = Parallel(backend='ipyparallel')
            assert p._backend._view.client is self.client
        
        # FIXME: add View.use_pickle to undo use_cloudpickle
        def _restore_default_pickle():
            import pickle
            from ipyparallel.serialize import canning, serialize
            canning.pickle = serialize.pickle = pickle
            canning.can_map[canning.FunctionType] = canning.CannedFunction
        
        _restore_default_pickle()
        self.client[:].apply_sync(_restore_default_pickle)
    
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
