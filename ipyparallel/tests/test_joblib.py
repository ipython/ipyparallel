
from nose import SkipTest

from ipyparallel.client import client as clientmod
from ipyparallel import error, AsyncHubResult, DirectView, Reference

from .clienttest import ClusterTestCase, wait, add_engines, skip_without

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
    
    def test_import_joblib_registers(self):
        """import ipyparallel.joblib registers backend"""
        import ipyparallel.joblib
        p = Parallel(backend='ipyparallel')
        self.assertIs(p.backend_factory, IPythonParallelBackend)
    
    def test_register_backend(self):
        view = self.client.load_balanced_view()
        view.register_joblib_backend('view')
        p = Parallel(backend='view')
        self.assertIs(p.backend_factory()._view, view)

    def test_joblib_backend(self):
        view = self.client.load_balanced_view()
        view.register_joblib_backend('view')
        p = Parallel(backend='view')
        result = p(delayed(neg)(i) for i in range(10))
        self.assertEqual(result, [neg(i) for i in range(10)])
