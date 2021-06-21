"""Async utilities"""
import asyncio
import concurrent.futures
import inspect
import threading
from functools import partial

from tornado.ioloop import IOLoop


def _asyncio_run(coro):
    """Like asyncio.run, but works when there's no event loop"""
    # for now: using tornado for broader compatibility with FDs,
    # e.g. when using the only partially functional default
    # Proactor on windows
    loop = IOLoop()
    return loop.run_sync(lambda: asyncio.ensure_future(coro))


class AsyncFirst:
    """Wrapper class that defines synchronous `_sync` method wrappers

    around async-native methods.

    Every coroutine method automatically gets an `_sync` alias
    that runs it synchronously.
    """

    _async_thread = None

    def _thread_main(self):
        asyncio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(asyncio_loop)
        loop = self._thread_loop = IOLoop.current()
        loop.add_callback(self._loop_started.set)
        loop.start()

    def _in_thread(self, async_f, *args, **kwargs):
        """Run an async function in a background thread"""
        if self._async_thread is None:
            self._loop_started = threading.Event()
            self._async_thread = threading.Thread(target=self._thread_main, daemon=True)
            self._async_thread.start()
            self._loop_started.wait(timeout=5)

        future = concurrent.futures.Future()

        async def thread_callback():
            try:
                future.set_result(await async_f(*args, **kwargs))
            except Exception as e:
                future.set_exception(e)

        self._thread_loop.add_callback(thread_callback)
        return future.result()

    def _synchronize(self, async_f, *args, **kwargs):
        """Run a method synchronously

        Uses asyncio.run if asyncio is not running,
        otherwise puts it in a background thread
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # sometimes get returns a RuntimeError
            # if there's no current loop under certain policies
            loop = None
        if loop and loop.is_running():
            return self._in_thread(async_f, *args, **kwargs)
        else:
            return _asyncio_run(async_f(*args, **kwargs))

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
