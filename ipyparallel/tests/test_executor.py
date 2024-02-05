"""Tests for Executor API"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import time

from ipyparallel.client.view import LazyMapIterator, LoadBalancedView

from .clienttest import ClusterTestCase


def wait(n):
    import time

    time.sleep(n)
    return n


def echo(x):
    return x


class TestExecutor(ClusterTestCase):
    def test_client_executor(self):
        executor = self.client.executor()
        assert isinstance(executor.view, LoadBalancedView)
        f = executor.submit(lambda x: 2 * x, 5)
        r = f.result()
        assert r == 10

    def test_view_executor(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        assert executor.view is view

    def test_executor_submit(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        f = executor.submit(lambda x, y: x * y, 2, 3)
        r = f.result()
        assert r == 6

    def test_executor_map(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        gen = executor.map(lambda x: x, range(5))
        assert isinstance(gen, LazyMapIterator)
        for i, r in enumerate(gen):
            assert i == r

    def test_executor_context(self):
        view = self.client.load_balanced_view()
        executor = view.executor
        with executor:
            f = executor.submit(time.sleep, 0.5)
            assert not f.done()
            m = executor.map(lambda x: x, range(10))
        assert len(view.history) == 11
        # Executor context calls shutdown
        # shutdown doesn't shutdown engines,
        # but it should at least wait for results to finish
        assert f.done()
        tic = time.perf_counter()
        list(m)
        toc = time.perf_counter()
        assert toc - tic < 0.5
