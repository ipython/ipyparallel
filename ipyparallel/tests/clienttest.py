"""base class for parallel client tests"""
from __future__ import print_function

import os
import signal
import sys
import time
from contextlib import contextmanager

import pytest
import zmq
from decorator import decorator
from zmq.tests import BaseZMQTestCase

from ipyparallel import Client
from ipyparallel import error
from ipyparallel.tests import add_engines
from ipyparallel.tests import launchers

# simple tasks for use in apply tests


def segfault():
    """this will segfault"""
    import ctypes

    ctypes.memset(-1, 0, 1)


def crash():
    """Ungracefully exit the process"""
    os._exit(1)


def conditional_crash(condition):
    """Ungracefully exit the process"""
    if condition:
        crash()


def wait(n):
    """sleep for a time"""
    import time

    time.sleep(n)
    return n


def raiser(eclass):
    """raise an exception"""
    raise eclass()


def generate_output():
    """function for testing output

    publishes two outputs of each type, and returns
    a rich displayable object.
    """

    from IPython.core.display import display, HTML, Math

    print("stdout")
    print("stderr", file=sys.stderr)

    display(HTML("<b>HTML</b>"))

    print("stdout2")
    print("stderr2", file=sys.stderr)

    display(Math(r"\alpha=\beta"))

    return Math("42")


# test decorator for skipping tests when libraries are unavailable
def skip_without(*names):
    """skip a test if some names are not importable"""

    @decorator
    def skip_without_names(f, *args, **kwargs):
        """decorator to skip tests in the absence of numpy, etc."""
        for name in names:
            try:
                __import__(name)
            except ImportError:
                pytest.skip("Test requires %s" % name)
        return f(*args, **kwargs)

    return skip_without_names


@contextmanager
def raises_remote(etype):
    if isinstance(etype, str):
        # allow Exception or 'Exception'
        expected_ename = etype
    else:
        expected_ename = etype.__name__

    try:
        try:
            yield
        except error.CompositeError as e:
            e.raise_exception()
    except error.RemoteError as e:
        assert (
            expected_ename == e.ename
        ), f"Should have raised {expected_ename}, but raised {e.ename}"

    else:
        pytest.fail("should have raised a RemoteError")


# -------------------------------------------------------------------------------
# Classes
# -------------------------------------------------------------------------------


@pytest.mark.usefixtures("cluster")
class ClusterTestCase(BaseZMQTestCase):
    timeout = 10
    engine_count = 2

    def add_engines(self, n=1, block=True):
        """add multiple engines to our cluster"""
        self.engines.extend(add_engines(n))
        if block:
            self.wait_on_engines()

    def minimum_engines(self, n=1, block=True):
        """add engines until there are at least n connected"""
        self.engines.extend(add_engines(n, total=True))
        if block:
            self.wait_on_engines()

    def wait_on_engines(self, timeout=5):
        """wait for our engines to connect."""
        n = len(self.engines) + self.base_engine_count
        self.client.wait_for_engines(n, timeout=timeout)

        assert not len(self.client.ids) < n, "waiting for engines timed out"

    def client_wait(self, client, jobs=None, timeout=-1):
        """my wait wrapper, sets a default finite timeout to avoid hangs"""
        if timeout is None or timeout < 0:
            timeout = self.timeout
        return Client.wait(client, jobs, timeout)

    def connect_client(self):
        """connect a client with my Context, and track its sockets for cleanup"""
        c = Client(profile='iptest', context=self.context)
        c.wait = lambda *a, **kw: self.client_wait(c, *a, **kw)

        for name in filter(lambda n: n.endswith('socket'), dir(c)):
            s = getattr(c, name)
            s.setsockopt(zmq.LINGER, 0)
            self.sockets.append(s)
        return c

    def assertRaisesRemote(self, etype, f, *args, **kwargs):
        with raises_remote(etype):
            f(*args, **kwargs)

    def _wait_for(self, f, timeout=10):
        """wait for a condition"""
        tic = time.time()
        while time.time() <= tic + timeout:
            if f():
                return
            time.sleep(0.1)
        if not f():
            print("Warning: Awaited condition never arrived")

    test_timeout = 30

    def setUp(self):
        BaseZMQTestCase.setUp(self)
        if hasattr(signal, 'SIGALRM'):
            # use sigalarm for test timeout
            def _sigalarm(sig, frame):
                raise TimeoutError(
                    f"test did not finish in {self.test_timeout} seconds"
                )

            signal.signal(signal.SIGALRM, _sigalarm)
            signal.alarm(self.test_timeout)

        add_engines(self.engine_count, total=True)

        self.client = self.connect_client()
        # start every test with clean engine namespaces:
        self.client.clear(block=True)
        self.base_engine_count = len(self.client.ids)
        self.engines = []

    def tearDown(self):
        self.client[:].use_pickle()

        # self.client.clear(block=True)
        # close fds:
        for e in filter(lambda e: e.poll() is not None, launchers):
            launchers.remove(e)

        # allow flushing of incoming messages to prevent crash on socket close
        self.client.wait(timeout=2)
        self.client.close()
        BaseZMQTestCase.tearDown(self)
        if hasattr(signal, 'SIGALRM'):
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
