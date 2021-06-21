"""tests for async utilities"""
import pytest

from ipyparallel._async import AsyncFirst


class A(AsyncFirst):
    a = 5

    def sync_method(self):
        return 'sync'

    async def async_method(self):
        return 'async'


def test_async_first_dir():
    a = A()
    attrs = sorted(dir(a))
    real_attrs = [a for a in attrs if not a.startswith("_")]
    assert real_attrs == ['a', 'async_method', 'async_method_sync', 'sync_method']


def test_getattr():
    a = A()
    assert a.a == 5
    with pytest.raises(AttributeError):
        a.a_sync
    with pytest.raises(AttributeError):
        a.some_other_sync
    with pytest.raises(AttributeError):
        a.sync_method_sync


def test_sync_no_asyncio():
    a = A()
    assert a.async_method_sync() == 'async'
    assert a._async_thread is None


async def test_sync_asyncio():
    a = A()
    assert a.async_method_sync() == 'async'
    assert a._async_thread is not None
