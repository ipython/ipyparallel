from binascii import b2a_hex
from functools import partial
import os
import pickle

from ipyparallel.serialize import canning
from ipyparallel.serialize.canning import can, uncan

def interactive(f):
    f.__module__ = '__main__'
    return f

def dumps(obj):
    return pickle.dumps(can(obj))

def loads(obj):
    return uncan(pickle.loads(obj))

def test_no_closure():
    @interactive
    def foo():
        a = 5
        return a
    
    pfoo = dumps(foo)
    bar = loads(pfoo)
    assert foo() == bar()

def test_generator_closure():
    # this only creates a closure on Python 3
    @interactive
    def foo():
        i = 'i'
        r = [ i for j in (1,2) ]
        return r
    
    pfoo = dumps(foo)
    bar = loads(pfoo)
    assert foo() == bar()

def test_nested_closure():
    @interactive
    def foo():
        i = 'i'
        def g():
            return i
        return g()
    
    pfoo = dumps(foo)
    bar = loads(pfoo)
    assert foo() == bar()

def test_closure():
    i = 'i'
    @interactive
    def foo():
        return i
    
    pfoo = dumps(foo)
    bar = loads(pfoo)
    assert foo() == bar()

def test_uncan_bytes_buffer():
    data = b'data'
    canned = can(data)
    canned.buffers = [memoryview(buf) for buf in canned.buffers]
    out = uncan(canned)
    assert out == data

def test_can_partial():
    def foo(x, y, z):
        return x * y * z
    partial_foo = partial(foo, 2, y=5)
    canned = can(partial_foo)
    assert isinstance(canned, canning.CannedPartial)
    dumped = pickle.dumps(canned)
    loaded = pickle.loads(dumped)
    pfoo2 = uncan(loaded)
    assert pfoo2(z=3) == partial_foo(z=3)

def test_can_partial_buffers():
    def foo(arg1, arg2, kwarg1, kwarg2):
        return '%s%s%s%s' % (arg1, arg2[:32], b2a_hex(kwarg1.tobytes()[:32]), kwarg2)

    buf1 = os.urandom(1024 * 1024)
    buf2 = memoryview(os.urandom(1024 * 1024))
    partial_foo = partial(foo, 5, buf1, kwarg1=buf2, kwarg2=10)
    canned = can(partial_foo)
    assert len(canned.buffers) == 2
    assert isinstance(canned, canning.CannedPartial)
    buffers, canned.buffers = canned.buffers, []
    dumped = pickle.dumps(canned)
    loaded = pickle.loads(dumped)
    loaded.buffers = buffers
    pfoo2 = uncan(loaded)
    assert pfoo2() == partial_foo()
