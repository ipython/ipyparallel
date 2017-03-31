"""Tests for dependency.py"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import os

from ipyparallel.serialize import can, uncan

import ipyparallel as ipp
from ipyparallel.util import interactive

from ipyparallel.tests import add_engines
from .clienttest import ClusterTestCase

@ipp.require('time')
def wait(n):
    time.sleep(n)
    return n

@ipp.interactive
def func(x):
    return x*x

mixed = list(map(str, range(10)))
completed = list(map(str, range(0,10,2)))
failed = list(map(str, range(1,10,2)))

class DependencyTest(ClusterTestCase):
    
    def setUp(self):
        ClusterTestCase.setUp(self)
        self.user_ns = {'__builtins__' : __builtins__}
        self.view = self.client.load_balanced_view()
        self.dview = self.client[-1]
        self.succeeded = set(map(str, range(0,25,2)))
        self.failed = set(map(str, range(1,25,2)))
    
    def assertMet(self, dep):
        self.assertTrue(dep.check(self.succeeded, self.failed), "Dependency should be met")
        
    def assertUnmet(self, dep):
        self.assertFalse(dep.check(self.succeeded, self.failed), "Dependency should not be met")
        
    def assertUnreachable(self, dep):
        self.assertTrue(dep.unreachable(self.succeeded, self.failed), "Dependency should be unreachable")
    
    def assertReachable(self, dep):
        self.assertFalse(dep.unreachable(self.succeeded, self.failed), "Dependency should be reachable")
    
    def cancan(self, f):
        """decorator to pass through canning into self.user_ns"""
        return uncan(can(f), self.user_ns)
    
    def test_require_imports(self):
        """test that @require imports names"""
        @self.cancan
        @ipp.require('base64')
        @interactive
        def encode(arg):
            return base64.b64encode(arg)
        # must pass through canning to properly connect namespaces
        self.assertEqual(encode(b'foo'), b'Zm9v')
    
    def test_success_only(self):
        dep = ipp.Dependency(mixed, success=True, failure=False)
        self.assertUnmet(dep)
        self.assertUnreachable(dep)
        dep.all=False
        self.assertMet(dep)
        self.assertReachable(dep)
        dep = ipp.Dependency(completed, success=True, failure=False)
        self.assertMet(dep)
        self.assertReachable(dep)
        dep.all=False
        self.assertMet(dep)
        self.assertReachable(dep)

    def test_failure_only(self):
        dep = ipp.Dependency(mixed, success=False, failure=True)
        self.assertUnmet(dep)
        self.assertUnreachable(dep)
        dep.all=False
        self.assertMet(dep)
        self.assertReachable(dep)
        dep = ipp.Dependency(completed, success=False, failure=True)
        self.assertUnmet(dep)
        self.assertUnreachable(dep)
        dep.all=False
        self.assertUnmet(dep)
        self.assertUnreachable(dep)
    
    def test_require_function(self):
        
        @ipp.interactive
        def bar(a):
            return func(a)

        @ipp.require(func)
        @ipp.interactive
        def bar2(a):
            return func(a)
        
        self.client[:].clear()
        self.assertRaisesRemote(NameError, self.view.apply_sync, bar, 5)
        ar = self.view.apply_async(bar2, 5)
        self.assertEqual(ar.get(5), func(5))

    def test_require_object(self):
        
        @ipp.require(foo=func)
        @ipp.interactive
        def bar(a):
            return foo(a)

        ar = self.view.apply_async(bar, 5)
        self.assertEqual(ar.get(5), func(5))
