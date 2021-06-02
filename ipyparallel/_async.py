"""Async utilities"""
import asyncio
import inspect
from concurrent.futures import ThreadPoolExecutor
from functools import partial


class AsyncFirst:
    """Wrapper class that defines synchronous `_sync` method wrappers

    around async-native methods.

    Every coroutine method automatically gets an `_sync` alias
    that runs it synchronously.
    """

    _async_thread = None

    def _in_thread(self, async_f, *args, **kwargs):
        """Run an async function in a background thread"""
        if self._async_thread is None:
            self._async_thread = ThreadPoolExecutor(1)
        future = self._async_thread.submit(
            lambda: asyncio.run(async_f(*args, **kwargs))
        )
        return future.result()

    def _synchronize(self, async_f, *args, **kwargs):
        """Run a method synchronously

        Uses asyncio.run if asyncio is not running,
        otherwise puts it in a background thread
        """
        if asyncio.get_event_loop().is_running():
            return self._in_thread(async_f, *args, **kwargs)
        else:
            return asyncio.run(async_f(*args, **kwargs))

    def __getattr__(self, name):
        if name.endswith("_sync"):
            # lazily define `_sync` method wrappers for coroutine methods
            async_name = name[:-5]
            async_method = super().__getattribute__(async_name)
            if not inspect.iscoroutinefunction(async_method):
                raise AttributeError(async_name)
            return partial(self._synchronize, async_method)
        return super().__getattribute__(name)

    def __dir__(self):
        attrs = super().__dir__()
        seen = set()
        for cls in self.__class__.mro():
            for name, value in cls.__dict__.items():
                if name in seen:
                    continue
                seen.add(name)
                if inspect.iscoroutinefunction(value):
                    async_name = name + "_sync"
                    attrs.append(async_name)
        return attrs
