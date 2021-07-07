"""Engine subprocess that receives and relays signals

Currently the kernel nanny serves two purposes:

1. to relay signals to the process
2. to notice engine shutdown and promptly notify the Hub,
   rather than waiting for heartbeat failures.

This has overlapping functionality with the Cluster API,
but has key differences, justifying both existing:

- addressing single engines is not feasible with the Cluster API,
  only engine *sets*
- Cluster API can efficiently signal all engines via mpiexec
"""
import asyncio
import logging
import os
import signal
from multiprocessing import Pipe
from multiprocessing import Process
from threading import Thread

import psutil
import tornado.ioloop
import zmq
from jupyter_client.session import Session
from traitlets.config import Config
from zmq.eventloop.zmqstream import ZMQStream

from ipyparallel import error
from ipyparallel.util import local_logger


class KernelNanny:
    """Object for monitoring

    Must be  handling signal messages"""

    def __init__(
        self,
        *,
        pid: int,
        engine_id: int,
        control_url: str,
        registration_url: str,
        identity: bytes,
        config: Config,
        start_pipe: Pipe,
        log_level: int = logging.INFO,
    ):
        self.pid = pid
        self.engine_id = engine_id
        self.parent_process = psutil.Process(self.pid)
        self.control_url = control_url
        self.registration_url = registration_url
        self.identity = identity
        self.start_pipe = start_pipe
        self.config = config
        self.session = Session(config=self.config)

        self.log = local_logger(f"{self.__class__.__name__}.{engine_id}", log_level)
        self.log.propagate = False

        self.control_handlers = {
            "signal_request": self.signal_request,
        }

    def wait_for_parent_thread(self):
        """Wait for my parent to exit, then I'll notify the controller and shut down"""
        self.log.info(f"Nanny watching parent pid {self.pid}.")
        while True:
            try:
                exit_code = self.parent_process.wait(60)
            except psutil.TimeoutExpired:
                continue
            else:
                break
        self.log.critical(f"Parent {self.pid} exited with status {exit_code}.")
        self.loop.add_callback(self.finish)

    def notify_exit(self):
        """Notify the Hub that our parent has exited"""
        self.log.info("Notifying Hub that our parent has shut down")
        s = self.context.socket(zmq.DEALER)
        # finite, nonzero LINGER to prevent hang without dropping message during exit
        s.LINGER = 3000
        s.connect(self.registration_url)
        self.session.send(s, "unregistration_request", content={"id": self.engine_id})
        s.close()

    def finish(self):
        """Prepare to exit and stop our event loop."""
        self.notify_exit()
        self.loop.add_callback(self.loop.stop)

    def dispatch_control(self, stream, raw_msg):
        """Dispatch message from the control scheduler

        If we have a handler registered"""
        try:
            idents, msg_frames = self.session.feed_identities(raw_msg)
        except Exception as e:
            self.log.error(f"Bad control message: {raw_msg}", exc_info=True)
            return

        try:
            msg = self.session.deserialize(msg_frames, content=True)
        except Exception:
            content = error.wrap_exception()
            self.log.error("Bad control message: %r", msg_frames, exc_info=True)
            return

        msg_type = msg['header']['msg_type']
        if msg_type.endswith("_request"):
            reply_type = msg_type[-len("_request") :]
        else:
            reply_type = "error"
        self.log.debug(f"Client {idents[-1]} requested {msg_type}")

        handler = self.control_handlers.get(msg_type, None)
        if handler is None:
            # don't have an intercept handler, relay original message to parent
            self.log.debug("Relaying {msg_type} {msg['header']['msg_id']}")
            self.parent_stream.send_multipart(raw_msg)
            return

        try:
            content = handler(msg['content'])
        except Exception:
            content = error.wrap_exception()
            self.log.error("Error handling request: %r", msg_type, exc_info=True)

        self.session.send(stream, reply_type, ident=idents, content=content, parent=msg)

    def dispatch_parent(self, stream, raw_msg):
        """Relay messages from parent directly to control stream"""
        self.control_stream.send_multipart(raw_msg)

    # intercept message handlers

    def signal_request(self, content):
        """Handle a signal request: send signal to parent process"""
        sig = content['sig']
        if isinstance(sig, str):
            sig = getattr(signal, sig)
        self.log.info(f"Sending signal {sig} to pid {self.pid}")
        # exception will be caught and wrapped by the caller
        self.parent_process.send_signal(sig)
        return {"status": "ok"}

    def start(self):
        self.log.info(
            f"Starting kernel nanny for engine {self.engine_id}, pid={self.pid}, nanny pid={os.getpid()}"
        )
        self._watcher_thread = Thread(
            target=self.wait_for_parent_thread, name="WatchParent", daemon=True
        )
        self._watcher_thread.start()
        # ignore SIGINT sent to parent
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.loop = tornado.ioloop.IOLoop.current()
        self.context = zmq.Context()

        # set up control socket (connection to Scheduler)
        self.control_socket = self.context.socket(zmq.ROUTER)
        self.control_socket.identity = self.identity
        self.control_socket.connect(self.control_url)
        self.control_stream = ZMQStream(self.control_socket)
        self.control_stream.on_recv_stream(self.dispatch_control)

        # set up relay socket (connection to parent's control socket)
        self.parent_socket = self.context.socket(zmq.DEALER)
        port = self.parent_socket.bind_to_random_port("tcp://127.0.0.1")

        # now that we've bound, pass port to parent via AsyncResult
        self.start_pipe.send(f"tcp://127.0.0.1:{port}")
        self.loop.add_timeout(self.loop.time() + 10, self.start_pipe.close)
        self.parent_stream = ZMQStream(self.parent_socket)
        self.parent_stream.on_recv_stream(self.dispatch_parent)
        try:
            self.loop.start()
        finally:
            self.loop.close(all_fds=True)
            self.context.term()

    @classmethod
    def main(cls, *args, **kwargs):
        """Main body function.

        Instantiates and starts a nanny.

        Args and keyword args passed to the constructor.

        Should be called in a subprocess.
        """
        # start a new event loop for the forked process
        asyncio.set_event_loop(asyncio.new_event_loop())
        tornado.ioloop.IOLoop().make_current()
        self = cls(*args, **kwargs)
        self.start()


def start_nanny(**kwargs):
    """Start a nanny subprocess

    Returns
    -------

    nanny_url: str
      The proxied URL the engine's control socket should connect to,
      instead of connecting directly to the control Scheduler.
    """

    pipe_r, pipe_w = Pipe(duplex=False)
    kwargs['start_pipe'] = pipe_w
    kwargs['pid'] = os.getpid()
    p = Process(target=KernelNanny.main, kwargs=kwargs, name="KernelNanny", daemon=True)
    p.start()
    # close our copy of the write pipe
    pipe_w.close()
    nanny_url = pipe_r.recv()
    pipe_r.close()
    return nanny_url
