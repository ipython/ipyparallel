"""Tests for remote functions"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import division

import ipyparallel as ipp

from .clienttest import ClusterTestCase, add_engines


class TestRemoteFunctions(ClusterTestCase):
    
    def test_remote(self):
        v = self.client[-1]
        @ipp.remote(v, block=True)
        def foo(x, y=5):
            """multiply x * y"""
            return x * y
        self.assertEqual(foo.__name__, 'foo')
        self.assertIn('RemoteFunction', foo.__doc__)
        self.assertIn('multiply x', foo.__doc__)

        z = foo(5)
        self.assertEqual(z, 25)
        z = foo(2, 3)
        self.assertEqual(z, 6)
        z = foo(x=5, y=2)
        self.assertEqual(z, 10)

    def test_parallel(self):
        n = 2
        v = self.client[:n]
        @ipp.parallel(v, block=True)
        def foo(x):
            """multiply x * y"""
            return x * 2
        self.assertEqual(foo.__name__, 'foo')
        self.assertIn('ParallelFunction', foo.__doc__)
        self.assertIn('multiply x', foo.__doc__)

        z = foo([1, 2, 3, 4])
        self.assertEqual(z, [1, 2, 1, 2, 3, 4, 3, 4])
    
    def test_parallel_map(self):
        v = self.client.load_balanced_view()
        @ipp.parallel(v, block=True)
        def foo(x, y=5):
            """multiply x * y"""
            return x * y
        z = foo.map([1, 2, 3])
        self.assertEqual(z, [5, 10, 15])
        z = foo.map([1, 2, 3], [1, 2, 3])
        self.assertEqual(z, [1, 4, 9])

   