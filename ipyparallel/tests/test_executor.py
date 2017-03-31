"""Tests for Executor API"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import time
import types

from tornado.ioloop import IOLoop

from ipyparallel.tests import add_engines
from .clienttest import ClusterTestCase
from ipyparallel.client.view import LoadBalancedView

def wait(n):
    import time
    time.sleep(n)
    return n

def echo(x):
    return x

class AsyncResultTest(ClusterTestCase):
    
    def resolve(self, future):
        return IOLoop().run_sync(lambda : future)
        
    def test_client_executor(self):
        executor = self.client.executor()
        assert isinstance(executor.view, LoadBalancedView)
        f = executor.submit(lambda x: 2 * x, 5)
        r = self.resolve(f)
        self.assertEqual(r, 10)
    
    def test_view_executor(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        self.assertIs(executor.view, view)
    
    def test_executor_submit(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        f = executor.submit(lambda x, y: x*y, 2, 3)
        r = self.resolve(f)
        self.assertEqual(r, 6)
    
    def test_executor_map(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        gen = executor.map(lambda x: x, range(5))
        assert isinstance(gen, types.GeneratorType)
        for i, r in enumerate(gen):
            assert i == r
    
    def test_executor_context(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        with executor:
            f = executor.submit(time.sleep, 0.5)
            assert not f.done()
        # Executor context calls shutdown
        # shutdown doesn't shutdown engines,
        # but it should at least wait for results to finish
        assert f.done()
