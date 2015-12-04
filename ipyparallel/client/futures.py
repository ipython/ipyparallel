"""Future-related utils"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from threading import Event
from tornado.concurrent import Future

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


