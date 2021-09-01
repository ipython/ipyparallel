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
import pickle
import signal
import sys
from subprocess import PIPE
from subprocess import Popen
from threading import Thread

import psutil
import zmq
from jupyter_client.session import Session
from tornado.ioloop import IOLoop
from traitlets.config import Config
from zmq.eventloop.zmqstream import ZMQStream

from ipyparallel import error
from ipyparallel.util import local_logger


class KernelNanny:
    """Object for monitoring

    Must be child of engine

    Handles signal messages and watches Engine process for exiting
    """

    def __init__(
        self,
        *,
        pid: int,
        engine_id: int,
        control_url: str,
        registration_url: str,
        identity: bytes,
        config: Config,
        pipe,
        log_level: int = logging.INFO,
    ):
        self.pid = pid
        self.engine_id = engine_id
        self.parent_process = psutil.Process(self.pid)
        self.control_url = control_url
        self.registration_url = registration_url
        self.identity = identity
        self.config = config
        self.pipe = pipe
        self.session = Session(config=self.config)
        log_level = 10

        self.log = local_logger(f"{self.__class__.__name__}.{engine_id}", log_level)
        self.log.propagate = False

        self.control_handlers = {
            "signal_request": self.signal_request,
        }
        self._finish_called = False

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

    def pipe_handler(self, fd, events):
        self.log.debug(f"Pipe event {events}")
        self.loop.remove_handler(fd)
        try:
            fd.close()
        except BrokenPipeError:
            pass
        try:
            status = self.parent_process.wait(0)
        except psutil.TimeoutExpired:
            try:
                status = self.parent_process.status()
            except psutil.NoSuchProcess:
                status = "exited"

        self.log.critical(f"Pipe closed, parent {self.pid} has status: {status}")
        self.finish()

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
        if self._finish_called:
            return
        self._finish_called = True
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
            self.log.debug(f"Relaying {msg_type} {msg['header']['msg_id']}")
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

        self.loop = IOLoop.current()
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
        self.pipe.write(f"tcp://127.0.0.1:{port}\n")
        if not sys.platform.startswith("win"):
            # watch for the stdout pipe to close
            # as a signal that our parent is shutting down
            self.loop.add_handler(
                self.pipe, self.pipe_handler, IOLoop.READ | IOLoop.ERROR
            )
        self.parent_stream = ZMQStream(self.parent_socket)
        self.parent_stream.on_recv_stream(self.dispatch_parent)
        try:
            self.loop.start()
        finally:
            self.loop.close(all_fds=True)
            self.context.term()
            try:
                self.pipe.close()
            except BrokenPipeError:
                pass
            self.log.debug("exiting")

    @classmethod
    def main(cls, *args, **kwargs):
        """Main body function.

        Instantiates and starts a nanny.

        Args and keyword args passed to the constructor.

        Should be called in a subprocess.
        """
        # start a new event loop for the forked process
        asyncio.set_event_loop(asyncio.new_event_loop())
        IOLoop().make_current()
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

    kwargs['pid'] = os.getpid()

    env = os.environ.copy()
    env['PYTHONUNBUFFERED'] = '1'
    p = Popen(
        [sys.executable, '-m', __name__],
        stdin=PIPE,
        stdout=PIPE,
        env=env,
        start_new_session=True,  # don't inherit signals
    )
    p.stdin.write(pickle.dumps(kwargs))
    p.stdin.close()
    out = p.stdout.readline()
    nanny_url = out.decode("utf8").strip()
    if not nanny_url:
        p.terminate()
        raise RuntimeError("nanny failed")
    # return the handle on the process
    # need to keep the pipe open for the nanny
    return nanny_url, p


def main():
    """Entrypoint from the command-line

    Loads kwargs from stdin,
    sets pipe to stdout
    """
    kwargs = pickle.load(os.fdopen(sys.stdin.fileno(), mode='rb'))
    kwargs['pipe'] = sys.stdout
    KernelNanny.main(**kwargs)


if __name__ == "__main__":
    main()
