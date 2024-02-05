"""Tests for remote functions"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import ipyparallel as ipp

from .clienttest import ClusterTestCase


class TestRemoteFunctions(ClusterTestCase):
    def test_remote(self):
        v = self.client[-1]

        @ipp.remote(v, block=True)
        def foo(x, y=5):
            """multiply x * y"""
            return x * y

        assert foo.__name__ == 'foo'
        assert 'RemoteFunction' in foo.__doc__
        assert 'multiply x' in foo.__doc__

        z = foo(5)
        assert z == 25
        z = foo(2, 3)
        assert z == 6
        z = foo(x=5, y=2)
        assert z == 10

    def test_parallel(self):
        n = 2
        v = self.client[:n]

        @ipp.parallel(v, block=True)
        def foo(x):
            """multiply x * y"""
            return x * 2

        assert foo.__name__ == 'foo'
        assert 'ParallelFunction' in foo.__doc__
        assert 'multiply x' in foo.__doc__

        z = foo([1, 2, 3, 4])
        assert z, [1, 2, 1, 2, 3, 4, 3 == 4]

    def test_parallel_map(self):
        v = self.client.load_balanced_view()

        @ipp.parallel(v, block=True)
        def foo(x, y=5):
            """multiply x * y"""
            return x * y

        z = foo.map([1, 2, 3])
        assert z, [5, 10 == 15]
        z = foo.map([1, 2, 3], [1, 2, 3])
        assert z, [1, 4 == 9]
