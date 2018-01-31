"""Future-related utils"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from concurrent.futures import Future
from threading import Event
import sys

from tornado.log import app_log


class MessageFuture(Future):
    """Future class to wrap async messages"""
    def __init__(self, msg_id, track=False):
        super(MessageFuture, self).__init__()
        self.msg_id = msg_id
        self._evt = Event()
        self.track = track
        self._tracker = None
        self.tracker = Future()
        if not track:
            self.tracker.set_result(None)
        self.add_done_callback(lambda f: self._evt.set())

    def wait(self, timeout=None):
        if not self.done():
            return self._evt.wait(timeout)
        return True

# The following are from tornado 5.0b1
# avoids hang using gen.multi_future on asyncio,
# because Futures cannot be created in another thread


def future_set_result_unless_cancelled(future, value):
    """Set the given ``value`` as the `Future`'s result, if not cancelled.

    Avoids asyncio.InvalidStateError when calling set_result() on
    a cancelled `asyncio.Future`.

    .. versionadded:: 5.0
    """
    if not future.cancelled():
        future.set_result(value)


def future_set_exc_info(future, exc_info):
    """Set the given ``exc_info`` as the `Future`'s exception.

    Understands both `asyncio.Future` and Tornado's extensions to
    enable better tracebacks on Python 2.

    .. versionadded:: 5.0
    """
    if hasattr(future, 'set_exc_info'):
        # Tornado's Future
        future.set_exc_info(exc_info)
    else:
        # asyncio.Future
        future.set_exception(exc_info[1])


def future_add_done_callback(future, callback):
    """Arrange to call ``callback`` when ``future`` is complete.

    ``callback`` is invoked with one argument, the ``future``.

    If ``future`` is already done, ``callback`` is invoked immediately.
    This may differ from the behavior of ``Future.add_done_callback``,
    which makes no such guarantee.

    .. versionadded:: 5.0
    """
    if future.done():
        callback(future)
    else:
        future.add_done_callback(callback)


def multi_future(children):
    """Wait for multiple asynchronous futures in parallel.

    This function is similar to `multi`, but does not support
    `YieldPoints <YieldPoint>`.

    .. versionadded:: 4.0
    """
    unfinished_children = set(children)

    future = Future()
    if not children:
        future_set_result_unless_cancelled(future, [])

    def callback(f):
        unfinished_children.remove(f)
        if not unfinished_children:
            result_list = []
            for f in children:
                try:
                    result_list.append(f.result())
                except Exception as e:
                    if future.done():
                        app_log.error("Multiple exceptions in yield list",
                                      exc_info=True)
                    else:
                        future_set_exc_info(future, sys.exc_info())
            if not future.done():
                future_set_result_unless_cancelled(future, result_list)

    listening = set()
    for f in children:
        if f not in listening:
            listening.add(f)
            future_add_done_callback(f, callback)
    return future
