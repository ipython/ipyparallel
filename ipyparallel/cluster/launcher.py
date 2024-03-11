"""Facilities for launching IPython Parallel processes asynchronously."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import asyncio
import copy
import inspect
import json
import logging
import os
import re
import shlex
import shutil
import signal
import stat
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, partial
from signal import SIGTERM
from subprocess import PIPE, STDOUT, Popen, check_output
from tempfile import TemporaryDirectory
from textwrap import indent

import entrypoints
import psutil
from IPython.utils.path import ensure_dir_exists, get_home_dir
from IPython.utils.text import EvalFormatter
from tornado import ioloop
from traitlets import (
    Any,
    CRegExp,
    Dict,
    Float,
    Instance,
    Integer,
    List,
    Unicode,
    default,
    observe,
)
from traitlets.config.configurable import LoggingConfigurable

from ..util import shlex_join
from ._winhpcjob import IPControllerJob, IPControllerTask, IPEngineSetJob, IPEngineTask

WINDOWS = os.name == 'nt'

SIGKILL = getattr(signal, "SIGKILL", -1)
# -----------------------------------------------------------------------------
# Paths to the kernel apps
# -----------------------------------------------------------------------------

ipcluster_cmd_argv = [sys.executable, "-m", "ipyparallel.cluster"]

ipengine_cmd_argv = [sys.executable, "-m", "ipyparallel.engine"]

ipcontroller_cmd_argv = [sys.executable, "-m", "ipyparallel.controller"]

# -----------------------------------------------------------------------------
# Base launchers and errors
# -----------------------------------------------------------------------------


class LauncherError(Exception):
    pass


class ProcessStateError(LauncherError):
    pass


class UnknownStatus(LauncherError):
    pass


class NotRunning(LauncherError):
    """Raised when a launcher is no longer running"""

    pass


class BaseLauncher(LoggingConfigurable):
    """An abstraction for starting, stopping and signaling a process."""

    stop_timeout = Integer(
        60,
        config=True,
        help="The number of seconds to wait for a process to exit before raising a TimeoutError in stop",
    )

    # In all of the launchers, the work_dir is where child processes will be
    # run. This will usually be the profile_dir, but may not be. any work_dir
    # passed into the __init__ method will override the config value.
    # This should not be used to set the work_dir for the actual engine
    # and controller. Instead, use their own config files or the
    # controller_args, engine_args attributes of the launchers to add
    # the work_dir option.
    work_dir = Unicode('.')

    # used in various places for labeling. often 'ipengine' or 'ipcontroller'
    name = Unicode("process")

    start_data = Any()
    stop_data = Any()

    identifier = Unicode(
        help="Used for lookup in e.g. EngineSetLauncher during notify_stop and default log files"
    )

    @default("identifier")
    def _default_identifier(self):
        identifier = f"{self.name}"
        if self.cluster_id:
            identifier = f"{identifier}-{self.cluster_id}"
        if getattr(self, 'engine_set_id', None):
            identifier = f"{identifier}-{self.engine_set_id}"
        identifier = f"{identifier}-{os.getpid()}"
        return identifier

    loop = Instance(ioloop.IOLoop, allow_none=True)

    def _loop_default(self):
        return ioloop.IOLoop.current()

    profile_dir = Unicode('').tag(to_dict=True)
    cluster_id = Unicode('').tag(to_dict=True)

    state = Unicode("before").tag(to_dict=True)

    stop_callbacks = List()

    def to_dict(self):
        """Serialize a Launcher to a dict, for later restoration"""
        d = {}
        for attr in self.traits(to_dict=True):
            d[attr] = getattr(self, attr)

        return d

    @classmethod
    def from_dict(cls, d, *, config=None, parent=None, **kwargs):
        """Restore a Launcher from a dict

        Subclasses should always call `launcher = super().from_dict(*args, **kwargs)`
        and finish initialization after that.

        After calling from_dict(),
        the launcher should be in the same state as after `.start()`
        (i.e. monitoring for exit, etc.)

        Returns: Launcher
            The instantiated and fully configured Launcher.

        Raises: NotRunning
            e.g. if the process has stopped and is no longer running.
        """
        launcher = cls(config=config, parent=parent, **kwargs)
        for attr in launcher.traits(to_dict=True):
            if attr in d:
                setattr(launcher, attr, d[attr])
        return launcher

    @property
    def cluster_args(self):
        """Common cluster arguments"""
        return []

    @property
    def connection_files(self):
        """Dict of connection file paths"""
        security_dir = os.path.join(self.profile_dir, 'security')
        name_prefix = "ipcontroller"
        if self.cluster_id:
            name_prefix = f"{name_prefix}-{self.cluster_id}"
        return {
            kind: os.path.join(security_dir, f"{name_prefix}-{kind}.json")
            for kind in ("client", "engine")
        }

    @property
    def args(self):
        """A list of cmd and args that will be used to start the process.

        This is what is passed to :func:`spawnProcess` and the first element
        will be the process name.
        """
        return self.find_args()

    def find_args(self):
        """The ``.args`` property calls this to find the args list.

        Subcommand should implement this to construct the cmd and args.
        """
        raise NotImplementedError('find_args must be implemented in a subclass')

    @property
    def arg_str(self):
        """The string form of the program arguments."""
        return ' '.join(self.args)

    @property
    def cluster_env(self):
        """Cluster-related env variables"""
        return {
            "IPP_CLUSTER_ID": self.cluster_id,
            "IPP_PROFILE_DIR": self.profile_dir,
        }

    environment = Dict(
        help="""Set environment variables for the launched process

        .. versionadded:: 8.0
        """,
        config=True,
    )

    def get_env(self):
        """Get the full environment for the process

        merges different sources for environment variables
        """
        env = {}
        env.update(self.cluster_env)
        env.update(self.environment)
        return env

    @property
    def running(self):
        """Am I running."""
        if self.state == 'running':
            return True
        else:
            return False

    async def start(self):
        """Start the process.

        Should be an `async def` coroutine.

        When start completes,
        the process should be requested (it need not be running yet),
        and waiting should begin in the background such that :meth:`.notify_stop`
        will be called when the process finishes.
        """
        raise NotImplementedError('start must be implemented in a subclass')

    async def stop(self):
        """Stop the process and notify observers of stopping.

        This method should be an `async def` coroutine,
        and return only after the process has stopped.

        All resources should be cleaned up by the time this returns.
        """
        raise NotImplementedError('stop must be implemented in a subclass')

    def on_stop(self, f):
        """Register a callback to be called with this Launcher's stop_data
        when the process actually finishes.
        """
        if self.state == 'after':
            return f(self.stop_data)
        else:
            self.stop_callbacks.append(f)

    def notify_start(self, data):
        """Call this to trigger startup actions.

        This logs the process startup and sets the state to 'running'.  It is
        a pass-through so it can be used as a callback.
        """

        self.log.debug(f"{self.__class__.__name__} {self.args[0]} started: {data}")
        self.start_data = data
        self.state = 'running'
        return data

    def notify_stop(self, data):
        """Call this to trigger process stop actions.

        This logs the process stopping and sets the state to 'after'. Call
        this to trigger callbacks registered via :meth:`on_stop`."""
        if self.state == 'after':
            self.log.debug("Already notified stop (data)")
            return data
        self.log.debug(f"{self.__class__.__name__} {self.args[0]} stopped: {data}")

        self.stop_data = data
        self.state = 'after'
        self._log_output(data)
        for f in self.stop_callbacks:
            f(data)
        return data

    def signal(self, sig):
        """Signal the process.

        Parameters
        ----------
        sig : str or int
            'KILL', 'INT', etc., or any signal number
        """
        raise NotImplementedError('signal must be implemented in a subclass')

    async def join(self, timeout=None):
        """Wait for the process to finish"""
        raise NotImplementedError('join must be implemented in a subclass')

    output_limit = Integer(
        100,
        config=True,
        help="""
    When a process exits, display up to this many lines of output
    """,
    )

    def get_output(self, remove=False):
        """Retrieve the output form the Launcher.

        If remove: remove the file, if any, where it was being stored.
        """
        # override in subclasses to retrieve output
        return ""

    def _log_output(self, stop_data=None):
        output = self.get_output(remove=True)
        if self.output_limit:
            output = "".join(output.splitlines(True)[-self.output_limit :])

        log = self.log.debug
        if stop_data and stop_data.get("exit_code", 0) != 0:
            log = self.log.warning
        if output:
            log("Output for %s:\n%s", self.identifier, output)


class ControllerLauncher(BaseLauncher):
    """Base class for launching ipcontroller"""

    name = Unicode("ipcontroller")

    controller_cmd = List(
        list(ipcontroller_cmd_argv),
        config=True,
        help="""Popen command to launch ipcontroller.""",
    )
    # Command line arguments to ipcontroller.
    controller_args = List(
        Unicode(),
        config=True,
        help="""command-line args to pass to ipcontroller""",
    )

    connection_info_timeout = Float(
        60,
        config=True,
        help="""
        Default timeout (in seconds) for get_connection_info
        
        .. versionadded:: 8.7
        """,
    )

    async def get_connection_info(self, timeout=None):
        """Retrieve connection info for the controller

        Default implementation assumes profile_dir and cluster_id are local.

        .. versionchanged:: 8.7
            Accept `timeout=None` (default) to use `.connection_info_timeout` config.
        """
        if timeout is None:
            timeout = self.connection_info_timeout
        connection_files = self.connection_files
        paths = list(connection_files.values())
        start_time = time.monotonic()
        if timeout >= 0:
            deadline = start_time + timeout
        else:
            deadline = None

        if not all(os.path.exists(f) for f in paths):
            self.log.debug(f"Waiting for {paths}")
        while not all(os.path.exists(f) for f in paths):
            if deadline is not None and time.monotonic() > deadline:
                missing_files = [f for f in paths if not os.path.exists(f)]
                raise TimeoutError(
                    f"Connection files {missing_files} did not arrive in {timeout}s"
                )
            await asyncio.sleep(0.1)
            status = self.poll()
            if inspect.isawaitable(status):
                status = await status
            if status is not None:
                raise RuntimeError(
                    f"Controller stopped with {status} while waiting for {paths}"
                )
        self.log.debug(f"Loading {paths}")
        connection_info = {}
        for key, path in connection_files.items():
            try:
                with open(path) as f:
                    connection_info[key] = json.load(f)
            except ValueError:
                # possible race while controller is still writing the file
                # give it half a second before trying again
                time.sleep(0.5)
                with open(path) as f:
                    connection_info[key] = json.load(f)

        return connection_info


class EngineLauncher(BaseLauncher):
    """Base class for launching one engine"""

    name = Unicode("ipengine")

    engine_cmd = List(
        ipengine_cmd_argv, config=True, help="""command to launch the Engine."""
    )
    # Command line arguments for ipengine.
    engine_args = List(
        Unicode(),
        config=True,
        help="command-line arguments to pass to ipengine",
    )

    n = Integer(1).tag(to_dict=True)

    engine_set_id = Unicode()


# -----------------------------------------------------------------------------
# Local process launchers
# -----------------------------------------------------------------------------


class LocalProcessLauncher(BaseLauncher):
    """Start and stop an external process in an asynchronous manner.

    This will launch the external process with a working directory of
    ``self.work_dir``.
    """

    # This is used to to construct self.args, which is passed to
    # spawnProcess.
    cmd_and_args = List(Unicode())

    poll_seconds = Integer(
        30,
        config=True,
        help="""Interval on which to poll processes (.

        Note: process exit should be noticed immediately,
        due to use of Process.wait(),
        but this interval should ensure we aren't leaving threads running forever,
        as other signals/events are checked on this interval
        """,
    )

    pid = Integer(-1).tag(to_dict=True)

    output_file = Unicode().tag(to_dict=True)

    @default("output_file")
    def _default_output_file(self):
        log_dir = os.path.join(self.profile_dir, "log")
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f'{self.identifier}.log')

    stop_seconds_until_kill = Integer(
        5,
        config=True,
        help="""The number of seconds to wait for a process to exit after sending SIGTERM before sending SIGKILL""",
    )

    stdout = None
    stderr = None
    process = None
    _wait_thread = None
    _popen_process = None

    def find_args(self):
        return self.cmd_and_args

    @classmethod
    def from_dict(cls, d, **kwargs):
        self = super().from_dict(d, **kwargs)
        self._reconstruct_process(d)
        return self

    def _reconstruct_process(self, d):
        """Reconstruct our process"""
        if 'pid' in d and d['pid'] > 0:
            try:
                self.process = psutil.Process(d['pid'])
            except psutil.NoSuchProcess as e:
                raise NotRunning(f"Process {d['pid']}")
            self._start_waiting()

    def _wait(self):
        """Background thread waiting for a process to exit"""
        exit_code = None
        while not self._stop_waiting.is_set() and self.state == 'running':
            try:
                # use a timeout so we can check the _stop_waiting event
                exit_code = self.process.wait(timeout=self.poll_seconds)
            except psutil.TimeoutExpired:
                continue
            else:
                break
        stop_data = dict(exit_code=exit_code, pid=self.pid, identifier=self.identifier)
        self.loop.add_callback(lambda: self.notify_stop(stop_data))
        if self._popen_process:
            # wait avoids ResourceWarning if the process has exited
            self._popen_process.wait(0)

    def _start_waiting(self):
        """Start background thread waiting on the process to exit"""
        # ensure self.loop is accessed on the main thread before waiting
        self.loop
        self._stop_waiting = threading.Event()
        self._wait_thread = threading.Thread(
            target=self._wait, daemon=True, name=f"wait(pid={self.pid})"
        )
        self._wait_thread.start()

    def start(self):
        self.log.debug("Starting %s: %r", self.__class__.__name__, self.args)
        if self.state != 'before':
            raise ProcessStateError(
                'The process was already started and has state: {self.state}'
            )
        self.log.debug(f"Sending output for {self.identifier} to {self.output_file}")

        env = os.environ.copy()
        env.update(self.get_env())
        self.log.debug(f"Setting environment: {','.join(self.get_env())}")

        with open(self.output_file, "ab") as f, open(os.devnull, "rb") as stdin:
            proc = self._popen_process = Popen(
                self.args,
                stdout=f.fileno(),
                stderr=STDOUT,
                stdin=stdin,
                env=env,
                cwd=self.work_dir,
                start_new_session=True,  # don't forward signals
            )
        self.pid = proc.pid
        # use psutil API for self.process
        self.process = psutil.Process(proc.pid)

        self.notify_start(self.process.pid)
        self._start_waiting()
        if 1 <= self.log.getEffectiveLevel() <= logging.DEBUG:
            self._start_streaming()

    async def join(self, timeout=None):
        """Wait for the process to exit"""
        if self._wait_thread is not None:
            self._wait_thread.join(timeout=timeout)

    def _stream_file(self, path):
        """Stream one file"""
        with open(path) as f:
            while self.state == 'running' and not self._stop_waiting.is_set():
                line = f.readline()
                # log prefix?
                # or stream directly to sys.stderr
                if line:
                    sys.stderr.write(line)
                else:
                    # pause while we are at the end of the file
                    time.sleep(0.1)

    def _start_streaming(self):
        self._stream_thread = t = threading.Thread(
            target=partial(self._stream_file, self.output_file),
            name=f"Stream Output {self.identifier}",
            daemon=True,
        )
        t.start()

    _output = None

    def get_output(self, remove=False):
        if self._output is None:
            if self.output_file:
                try:
                    with open(self.output_file) as f:
                        self._output = f.read()
                except FileNotFoundError:
                    self.log.debug(f"Missing output file: {self.output_file}")
                    self._output = ""
            else:
                self._output = ""

        if remove and os.path.isfile(self.output_file):
            self.log.debug(f"Removing {self.output_file}")
            try:
                os.remove(self.output_file)
            except Exception as e:
                # don't crash on failure to remove a file,
                # e.g. due to another processing having it open
                self.log.error(f"Failed to remove {self.output_file}: {e}")

        return self._output

    async def stop(self):
        try:
            self.signal(SIGTERM)
        except Exception as e:
            self.log.debug(f"TERM failed: {e!r}")

        try:
            await self.join(timeout=self.stop_seconds_until_kill)
        except TimeoutError:
            self.log.warning(
                f"Process {self.pid} did not exit in {self.stop_seconds_until_kill} seconds after TERM"
            )
        else:
            return

        try:
            self.signal(SIGKILL)
        except Exception as e:
            self.log.debug(f"KILL failed: {e!r}")

        await self.join(timeout=self.stop_timeout)

    def signal(self, sig):
        if self.state == 'running':
            if WINDOWS and sig in {SIGTERM, SIGKILL}:
                # use Windows tree-kill for better child cleanup
                cmd = ['taskkill', '/pid', str(self.process.pid), '/t', '/F']
                check_output(cmd)
            else:
                self.process.send_signal(sig)

    # callbacks, etc:

    def handle_stdout(self, fd, events):
        if WINDOWS:
            line = self.stdout.recv().decode('utf8', 'replace')
        else:
            line = self.stdout.readline().decode('utf8', 'replace')
        # a stopped process will be readable but return empty strings
        if line:
            self.log.debug(line.rstrip())

    def handle_stderr(self, fd, events):
        if WINDOWS:
            line = self.stderr.recv().decode('utf8', 'replace')
        else:
            line = self.stderr.readline().decode('utf8', 'replace')
        # a stopped process will be readable but return empty strings
        if line:
            self.log.debug(line.rstrip())
        else:
            self.poll()

    def poll(self):
        if self.process.is_running():
            return None

        status = self.process.wait(0)
        if status is None:
            # return code cannot always be retrieved.
            # but we need to not return None if it's still running
            status = 'unknown'
        self.notify_stop(
            dict(exit_code=status, pid=self.process.pid, identifier=self.identifier)
        )
        return status


class LocalControllerLauncher(LocalProcessLauncher, ControllerLauncher):
    """Launch a controller as a regular external process."""

    def find_args(self):
        return self.controller_cmd + self.cluster_args + self.controller_args

    def start(self):
        """Start the controller by profile_dir."""
        return super().start()


class LocalEngineLauncher(LocalProcessLauncher, EngineLauncher):
    """Launch a single engine as a regular external process."""

    def find_args(self):
        return self.engine_cmd + self.cluster_args + self.engine_args


class LocalEngineSetLauncher(LocalEngineLauncher):
    """Launch a set of engines as regular external processes."""

    delay = Float(
        0.1,
        config=True,
        help="""delay (in seconds) between starting each engine after the first.
        This can help force the engines to get their ids in order, or limit
        process flood when starting many engines.""",
    )

    # launcher class
    launcher_class = LocalEngineLauncher

    launchers = Dict()
    stop_data = Dict()
    outputs = Dict()
    output_file = ""  # no output file for me

    def __init__(self, work_dir='.', config=None, **kwargs):
        super().__init__(work_dir=work_dir, config=config, **kwargs)

    def to_dict(self):
        d = super().to_dict()
        d['engines'] = {i: launcher.to_dict() for i, launcher in self.launchers.items()}
        return d

    @classmethod
    def from_dict(cls, d, **kwargs):
        self = super().from_dict(d, **kwargs)
        n = 0
        for i, engine_dict in d['engines'].items():
            try:
                self.launchers[i] = el = self.launcher_class.from_dict(
                    engine_dict, identifier=i, parent=self
                )
            except NotRunning as e:
                self.log.error(f"Engine {i} not running: {e}")
            else:
                n += 1
                el.on_stop(self._notice_engine_stopped)
        if n == 0:
            raise NotRunning("No engines left")
        else:
            self.n = n
        return self

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        self.n = n
        dlist = []
        for i in range(n):
            identifier = str(i)
            if i > 0:
                time.sleep(self.delay)
            el = self.launchers[identifier] = self.launcher_class(
                work_dir=self.work_dir,
                parent=self,
                log=self.log,
                profile_dir=self.profile_dir,
                cluster_id=self.cluster_id,
                environment=self.environment,
                identifier=identifier,
                output_file=os.path.join(
                    self.profile_dir,
                    "log",
                    f"ipengine-{self.cluster_id}-{self.engine_set_id}-{i}.log",
                ),
            )

            # Copy the engine args over to each engine launcher.
            el.engine_cmd = copy.deepcopy(self.engine_cmd)
            el.engine_args = copy.deepcopy(self.engine_args)
            el.on_stop(self._notice_engine_stopped)
            d = el.start()
            dlist.append(d)
        self.notify_start(dlist)
        return dlist

    def find_args(self):
        return ['engine set']

    def signal(self, sig):
        for el in list(self.launchers.values()):
            el.signal(sig)

    async def stop(self):
        futures = []
        for el in list(self.launchers.values()):
            f = el.stop()
            if inspect.isawaitable(f):
                futures.append(asyncio.ensure_future(f))

        if futures:
            await asyncio.gather(*futures)

    def _notice_engine_stopped(self, data):
        identifier = data['identifier']
        launcher = self.launchers.pop(identifier)
        engines = self.stop_data.setdefault("engines", {})
        if launcher is not None:
            self.outputs[identifier] = launcher.get_output()
        engines[identifier] = data
        if not self.launchers:
            # get exit code from engine exit codes
            # set error code if any engine has an error
            self.stop_data["exit_code"] = None
            for engine in engines.values():
                if 'exit_code' in engine:
                    if self.stop_data['exit_code'] is None:
                        self.stop_data['exit_code'] = engine['exit_code']
                    if engine['exit_code']:
                        # save the first nonzero exit code
                        self.stop_data['exit_code'] = engine['exit_code']
                        break

            self.notify_stop(self.stop_data)

    def _log_output(self, stop_data=None):
        # avoid double-logging output, already logged by each engine
        # that will be a lot if all 100 engines fail!
        pass

    def get_output(self, remove=False):
        """Get the output of all my child Launchers"""
        for identifier, launcher in self.launchers.items():
            # remaining launchers
            self.outputs[identifier] = launcher.get_output(remove=remove)

        joined_output = []
        for identifier, engine_output in self.outputs.items():
            if engine_output:
                joined_output.append(f"Output for engine {identifier}")
                if self.output_limit:
                    engine_output = "".join(
                        engine_output.splitlines(True)[-self.output_limit :]
                    )
                joined_output.append(indent(engine_output, '  '))
        return '\n'.join(joined_output)


# -----------------------------------------------------------------------------
# MPI launchers
# -----------------------------------------------------------------------------


class MPILauncher(LocalProcessLauncher):
    """Launch an external process using mpiexec."""

    mpi_cmd = List(
        ['mpiexec'],
        config=True,
        help="The mpiexec command to use in starting the process.",
    )
    mpi_args = List(
        [], config=True, help="The command line arguments to pass to mpiexec."
    )
    program = List(['date'], help="The program to start via mpiexec.")
    program_args = List([], help="The command line argument to the program.")

    def __init__(self, *args, **kwargs):
        # deprecation for old MPIExec names:
        config = kwargs.get('config') or {}
        for oldname in (
            'MPIExecLauncher',
            'MPIExecControllerLauncher',
            'MPIExecEngineSetLauncher',
        ):
            deprecated = config.get(oldname)
            if deprecated:
                newname = oldname.replace('MPIExec', 'MPI')
                config[newname].update(deprecated)
                self.log.warning(
                    "WARNING: %s name has been deprecated, use %s", oldname, newname
                )

        super().__init__(*args, **kwargs)

    def find_args(self):
        """Build self.args using all the fields."""
        return (
            self.mpi_cmd
            + ['-n', str(self.n)]
            + self.mpi_args
            + self.program
            + self.program_args
        )

    def start(self, n=1):
        """Start n instances of the program using mpiexec."""
        self.n = n
        return super().start()

    def _log_output(self, stop_data):
        """Try to log mpiexec error output, if any, at warning level"""
        super()._log_output(stop_data)

        if stop_data and self.stop_data.get("exit_code", 0) != 0:
            # if this is True, super()._log_output would have already logged the full output
            # no need to extract from MPI
            return

        output = self.get_output(remove=False)
        mpiexec_lines = []

        in_mpi = False
        after_mpi = False
        mpi_tail = 0
        for line in output.splitlines(True):
            if line.startswith("======="):
                # mpich output looks like one block,
                # with a few lines trailing after
                # =========
                # = message
                # =
                # =========
                # YOUR APPLICATION TERMINATED WITH...
                if in_mpi:
                    after_mpi = True
                    mpi_tail = 2
                    in_mpi = False
                else:
                    in_mpi = True
            elif not in_mpi and line.startswith("-----"):
                # openmpi has less clear boundaries;
                # potentially several blocks that start and end with `----`
                # and error messages can show up after one or more blocks
                # once we see one of these lines, capture everything after it
                # toggle on each such line
                if not in_mpi:
                    in_mpi = True
                # this would let us only capture messages inside blocks
                # but doing so would exclude most useful error output
                # else:
                #     # show the trailing delimiter line
                #     mpiexec_lines.append(line)
                #     in_mpi = False
                #     continue

            if in_mpi:
                mpiexec_lines.append(line)
            elif after_mpi:
                if mpi_tail <= 0:
                    break
                else:
                    mpi_tail -= 1
                    mpiexec_lines.append(line)

        if mpiexec_lines:
            self.log.warning("mpiexec error output:\n" + "".join(mpiexec_lines))


class MPIControllerLauncher(MPILauncher, ControllerLauncher):
    """Launch a controller using mpiexec."""

    # alias back to *non-configurable* program[_args] for use in find_args()
    # this way all Controller/EngineSetLaunchers have the same form, rather
    # than *some* having `program_args` and others `controller_args`
    @property
    def program(self):
        return self.controller_cmd

    @property
    def program_args(self):
        return self.cluster_args + self.controller_args


class MPIEngineSetLauncher(MPILauncher, EngineLauncher):
    """Launch engines using mpiexec"""

    # alias back to *non-configurable* program[_args] for use in find_args()
    # this way all Controller/EngineSetLaunchers have the same form, rather
    # than *some* having `program_args` and others `controller_args`
    @property
    def program(self):
        return self.engine_cmd + ['--mpi']

    @property
    def program_args(self):
        return self.cluster_args + self.engine_args

    def start(self, n):
        """Start n engines by profile or profile_dir."""
        self.n = n
        return super().start(n)


# deprecated MPIExec names
class DeprecatedMPILauncher:
    def warn(self):
        oldname = self.__class__.__name__
        newname = oldname.replace('MPIExec', 'MPI')
        self.log.warning("WARNING: %s name is deprecated, use %s", oldname, newname)


class MPIExecLauncher(MPILauncher, DeprecatedMPILauncher):
    """Deprecated, use MPILauncher"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.warn()


class MPIExecControllerLauncher(MPIControllerLauncher, DeprecatedMPILauncher):
    """Deprecated, use MPIControllerLauncher"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.warn()


class MPIExecEngineSetLauncher(MPIEngineSetLauncher, DeprecatedMPILauncher):
    """Deprecated, use MPIEngineSetLauncher"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.warn()


# -----------------------------------------------------------------------------
# SSH launchers
# -----------------------------------------------------------------------------

ssh_output_pattern = re.compile(r"__([a-z][a-z0-9_]+)=([a-z0-9\-\.]+)__", re.IGNORECASE)


def _ssh_outputs(out):
    """Extract ssh output variables from process output"""
    return dict(ssh_output_pattern.findall(out))


def sshx(ssh_cmd, cmd, env, remote_output_file, log=None):
    """Launch a remote process, returning its remote pid

    Uses nohup and pipes to put it in the background
    """
    remote_cmd = shlex_join(cmd)

    nohup_start = f"nohup {remote_cmd} > {remote_output_file} 2>&1 </dev/null & echo __remote_pid=$!__"
    full_cmd = ssh_cmd + ["--", "sh -"]

    input_script = "\n".join(
        [
            "set -eu",
        ]
        + [f"export {key}={shlex.quote(value)}" for key, value in env.items()]
        + ["", f"exec {nohup_start}"]
    )
    if log:
        log.info(f"Running `{remote_cmd}`")
        log.debug("Running script via ssh:\n%s", input_script)
    out = check_output(full_cmd, input=input_script.encode("utf8")).decode(
        "utf8", "replace"
    )
    values = _ssh_outputs(out)
    if 'remote_pid' in values:
        return int(values['remote_pid'])
    else:
        raise RuntimeError("Failed to get pid for {full_cmd}: {out}")


def ssh_waitpid(pid, timeout=None):
    """To be called on a remote host, waiting on a pid"""
    try:
        p = psutil.Process(pid)
        exit_code = p.wait(timeout)
    except psutil.NoSuchProcess:
        print("__process_running=0__")
        print("__exit_code=-1__")
    except psutil.TimeoutExpired:
        print("__process_running=1__")
    else:
        print("__process_running=0__")
        print("__exit_code=-1__")


class SSHLauncher(LocalProcessLauncher):
    """A minimal launcher for ssh.

    To be useful this will probably have to be extended to use the ``sshx``
    idea for environment variables.  There could be other things this needs
    as well.
    """

    ssh_cmd = List(['ssh'], config=True, help="command for starting ssh").tag(
        to_dict=True
    )
    ssh_args = List([], config=True, help="args to pass to ssh").tag(to_dict=True)
    scp_cmd = List(['scp'], config=True, help="command for sending files").tag(
        to_dict=True
    )
    scp_args = List([], config=True, help="args to pass to scp").tag(to_dict=True)
    program = List([], help="Program to launch via ssh")
    program_args = List([], help="args to pass to remote program")
    hostname = Unicode(
        '', config=True, help="hostname on which to launch the program"
    ).tag(to_dict=True)
    user = Unicode('', config=True, help="username for ssh").tag(to_dict=True)
    location = Unicode(
        '', config=True, help="user@hostname location for ssh in one setting"
    )
    to_fetch = List(
        [], config=True, help="List of (remote, local) files to fetch after starting"
    )
    to_send = List(
        [], config=True, help="List of (local, remote) files to send before starting"
    )

    @default("poll_seconds")
    def _default_poll_seconds(self):
        # slower poll for ssh
        return 60

    @observe('hostname')
    def _hostname_changed(self, change):
        if self.user:
            self.location = '{}@{}'.format(self.user, change['new'])
        else:
            self.location = change['new']

    @observe('user')
    def _user_changed(self, change):
        self.location = '{}@{}'.format(change['new'], self.hostname)

    def find_args(self):
        # not really used except in logging
        return list(self.ssh_cmd)

    remote_output_file = Unicode(
        help="""The remote file to store output""",
    ).tag(to_dict=True)

    @default("remote_output_file")
    def _default_remote_output_file(self):
        full_program = ' '.join(self.program)
        if 'engine' in full_program:
            name = 'ipengine'
        elif 'controller' in full_program:
            name = 'ipcontroller'
        else:
            name = self.program[0]
        return os.path.join(
            self.remote_profile_dir,
            'log',
            os.path.basename(name) + f"-{time.time():.4f}.out",
        )

    remote_profile_dir = Unicode(
        '',
        config=True,
        help="""The remote profile_dir to use.

        If not specified, use calling profile, stripping out possible leading homedir.
        """,
    ).tag(to_dict=True)

    @observe('profile_dir')
    def _profile_dir_changed(self, change):
        if not self.remote_profile_dir:
            # trigger remote_profile_dir_default logic again,
            # in case it was already triggered before profile_dir was set
            self.remote_profile_dir = self._strip_home(change['new'])

    remote_python = Unicode(
        "python3", config=True, help="""Remote path to Python interpreter, if needed"""
    ).tag(to_dict=True)

    @staticmethod
    def _strip_home(path):
        """turns /home/you/.ipython/profile_foo into .ipython/profile_foo"""
        home = get_home_dir()
        if not home.endswith('/'):
            home = home + '/'

        if path.startswith(home):
            return path[len(home) :]
        else:
            return path

    @default("remote_profile_dir")
    def _remote_profile_dir_default(self):
        return self._strip_home(self.profile_dir)

    @property
    def cluster_env(self):
        # use remote profile dir in env
        env = super().cluster_env
        env['IPP_PROFILE_DIR'] = self.remote_profile_dir
        return env

    _output = None

    def _reconstruct_process(self, d):
        # called in from_dict
        # override from LocalProcessLauncher which invokes psutil.Process
        if 'pid' in d and d['pid'] > 0:
            self._start_waiting()

    def poll(self):
        """Override poll"""
        if self.state == 'running':
            return None
        else:
            return 0

    def get_output(self, remove=False):
        """Retrieve engine output from the remote file"""
        if self._output is None:
            with TemporaryDirectory() as td:
                output_file = os.path.join(
                    td, os.path.basename(self.remote_output_file)
                )
                try:
                    self._fetch_file(self.remote_output_file, output_file)
                except Exception as e:
                    self.log.error(
                        f"Failed to get output file {self.remote_output_file}: {e}"
                    )
                    self._output = ''
                else:
                    if remove:
                        # remove the file after we retrieve it
                        self.log.info(
                            f"Removing {self.location}:{self.remote_output_file}"
                        )
                        check_output(
                            self.ssh_cmd
                            + self.ssh_args
                            + [
                                self.location,
                                "--",
                                shlex_join(["rm", "-f", self.remote_output_file]),
                            ],
                            input=None,
                        )
                    with open(output_file) as f:
                        self._output = f.read()
        return self._output

    def _send_file(self, local, remote, wait=True):
        """send a single file"""
        full_remote = f"{self.location}:{remote}"
        for i in range(10 if wait else 0):
            if not os.path.exists(local):
                self.log.debug("waiting for %s" % local)
                time.sleep(1)
            else:
                break
        remote_dir = os.path.dirname(remote)
        self.log.info("ensuring remote %s:%s/ exists", self.location, remote_dir)
        check_output(
            self.ssh_cmd
            + self.ssh_args
            + [self.location, '--', 'mkdir', '-p', remote_dir],
            input=None,
        )
        self.log.info("sending %s to %s", local, full_remote)
        check_output(self.scp_cmd + self.scp_args + [local, full_remote], input=None)

    def send_files(self):
        """send our files (called before start)"""
        if not self.to_send:
            return
        for local_file, remote_file in self.to_send:
            self._send_file(local_file, remote_file)

    def _fetch_file(self, remote, local, wait=True):
        """fetch a single file"""
        full_remote = f"{self.location}:{remote}"
        self.log.info("fetching %s from %s", local, full_remote)
        for i in range(10 if wait else 0):
            # wait up to 10s for remote file to exist
            check = check_output(
                self.ssh_cmd
                + self.ssh_args
                + [self.location, 'test -e', remote, "&& echo 'yes' || echo 'no'"],
                input=None,
            )
            check = check.decode("utf8", 'replace').strip()
            if check == 'no':
                time.sleep(1)
            elif check == 'yes':
                break
        local_dir = os.path.dirname(local)
        ensure_dir_exists(local_dir, 700)
        check_output(self.scp_cmd + self.scp_args + [full_remote, local])

    def fetch_files(self):
        """fetch remote files (called after start)"""
        if not self.to_fetch:
            return
        for remote_file, local_file in self.to_fetch:
            self._fetch_file(remote_file, local_file)

    def start(self, hostname=None, user=None, port=None):
        if hostname is not None:
            self.hostname = hostname
        if user is not None:
            self.user = user
        if port is not None:
            if '-p' not in self.ssh_args:
                self.ssh_args.append('-p')
                self.ssh_args.append(str(port))
            if '-P' not in self.scp_args:
                self.scp_args.append('-P')
                self.scp_args.append(str(port))

        # create remote profile dir
        check_output(
            self.ssh_cmd
            + self.ssh_args
            + [
                self.location,
                shlex_join(
                    [
                        self.remote_python,
                        "-m",
                        "IPython",
                        "profile",
                        "create",
                        "--profile-dir",
                        self.remote_profile_dir,
                    ]
                ),
            ],
            input=None,
        )
        self.send_files()
        self.pid = sshx(
            self.ssh_cmd + self.ssh_args + [self.location],
            self.program + self.program_args,
            env=self.get_env(),
            remote_output_file=self.remote_output_file,
            log=self.log,
        )
        self.notify_start({'host': self.location, 'pid': self.pid})
        self._start_waiting()
        self.fetch_files()

    def _wait(self):
        """Background thread waiting for a process to exit"""
        exit_code = None
        while not self._stop_waiting.is_set() and self.state == 'running':
            try:
                # use a timeout so we can check the _stop_waiting event
                exit_code = self.wait_one(timeout=self.poll_seconds)
            except TimeoutError:
                continue
            else:
                break
        stop_data = dict(exit_code=exit_code, pid=self.pid, identifier=self.identifier)
        self.loop.add_callback(lambda: self.notify_stop(stop_data))

    def _start_waiting(self):
        """Start background thread waiting on the process to exit"""
        # ensure self.loop is accessed on the main thread before waiting
        self.loop
        self._stop_waiting = threading.Event()
        self._wait_thread = threading.Thread(
            target=self._wait,
            daemon=True,
            name=f"wait(host={self.location}, pid={self.pid})",
        )
        self._wait_thread.start()

    def wait_one(self, timeout):
        python_code = f"from ipyparallel.cluster.launcher import ssh_waitpid; ssh_waitpid({self.pid}, timeout={timeout})"
        full_cmd = (
            self.ssh_cmd
            + self.ssh_args
            # double-quote for ssh
            + [self.location, "--", self.remote_python, "-c", f"'{python_code}'"]
        )
        out = check_output(full_cmd, input=None, start_new_session=True).decode(
            "utf8", "replace"
        )
        values = _ssh_outputs(out)
        if 'process_running' not in values:
            raise RuntimeError(out)
        running = int(values.get("process_running", 0))
        if running:
            raise TimeoutError("still running")
        return int(values.get("exit_code", -1))

    async def join(self, timeout=None):
        with ThreadPoolExecutor(1) as pool:
            wait = partial(self.wait_one, timeout=timeout)
            try:
                future = pool.submit(wait)
            except RuntimeError:
                # e.g. called during process shutdown,
                # which raises
                # RuntimeError: cannot schedule new futures after interpreter shutdown
                # Instead, do the blocking call
                wait()
            else:
                await asyncio.wrap_future(future)
        if getattr(self, '_stop_waiting', None) and self._wait_thread:
            self._stop_waiting.set()
            # got here, should be done
            # wait for wait_thread to cleanup
            self._wait_thread.join()

    def signal(self, sig):
        if self.state == 'running':
            check_output(
                self.ssh_cmd
                + self.ssh_args
                + [
                    self.location,
                    '--',
                    'kill',
                    f'-{sig}',
                    str(self.pid),
                ],
                input=None,
            )

    @property
    def remote_connection_files(self):
        """Return remote paths for connection files"""
        return {
            key: self.remote_profile_dir + local_path[len(self.profile_dir) :]
            for key, local_path in self.connection_files.items()
        }


class SSHControllerLauncher(SSHLauncher, ControllerLauncher):
    # alias back to *non-configurable* program[_args] for use in find_args()
    # this way all Controller/EngineSetLaunchers have the same form, rather
    # than *some* having `program_args` and others `controller_args`

    def _controller_cmd_default(self):
        return [self.remote_python, "-m", 'ipyparallel.controller']

    @property
    def program(self):
        return self.controller_cmd

    @property
    def program_args(self):
        return self.cluster_args + self.controller_args

    @default("to_fetch")
    def _to_fetch_default(self):
        to_fetch = []
        return [
            (self.remote_connection_files[key], local_path)
            for key, local_path in self.connection_files.items()
        ]


class SSHEngineLauncher(SSHLauncher, EngineLauncher):
    # alias back to *non-configurable* program[_args] for use in find_args()
    # this way all Controller/EngineSetLaunchers have the same form, rather
    # than *some* having `program_args` and others `controller_args`

    def _engine_cmd_default(self):
        return [self.remote_python, "-m", "ipyparallel.engine"]

    @property
    def program(self):
        return self.engine_cmd

    @property
    def program_args(self):
        return self.cluster_args + self.engine_args

    @default("to_send")
    def _to_send_default(self):
        return [
            (local_path, self.remote_connection_files[key])
            for key, local_path in self.connection_files.items()
        ]


class SSHEngineSetLauncher(LocalEngineSetLauncher, SSHLauncher):
    launcher_class = SSHEngineLauncher
    engines = Dict(
        config=True,
        help="""dict of engines to launch.  This is a dict by hostname of ints,
        corresponding to the number of engines to start on that host.""",
    ).tag(to_dict=True)

    def _engine_cmd_default(self):
        return [self.remote_python, "-m", "ipyparallel.engine"]

    # unset some traits we inherit but don't use
    remote_output_file = ""

    def start(self, n):
        """Start engines by profile or profile_dir.
        `n` is an *upper limit* of engines.
        The `engines` config property is used to assign slots to hosts.
        """

        dlist = []
        # traits to inherit:
        # + all common config traits
        # - traits set per-engine via engines dict
        # + some non-configurable traits such as cluster_id
        engine_traits = self.launcher_class.class_traits(config=True)
        my_traits = self.traits(config=True)
        shared_traits = set(my_traits).intersection(engine_traits)
        # in addition to shared traits, pass some derived traits
        # and exclude some composite traits
        inherited_traits = shared_traits.difference(
            {"location", "user", "hostname", "to_send", "to_fetch"}
        ).union({"profile_dir", "cluster_id"})

        requested_n = n
        started_n = 0
        for host, n_or_config in self.engines.items():
            if isinstance(n_or_config, dict):
                overrides = n_or_config
                n = overrides.pop("n", 1)
            else:
                overrides = {}
                n = n_or_config

            full_host = host

            if '@' in host:
                user, host = host.split('@', 1)
            else:
                user = None
            if ':' in host:
                host, port = host.split(':', 1)
            else:
                port = None

            for i in range(min(n, requested_n - started_n)):
                if i > 0:
                    time.sleep(self.delay)
                # pass all common traits to the launcher
                kwargs = {attr: getattr(self, attr) for attr in inherited_traits}
                # overrides from engine config
                kwargs.update(overrides)
                # explicit per-engine values
                kwargs['parent'] = self
                kwargs['identifier'] = key = f"{full_host}/{i}"
                el = self.launchers[key] = self.launcher_class(**kwargs)
                if i > 0:
                    # only send files for the first engine on each host
                    el.to_send = []

                el.on_stop(self._notice_engine_stopped)
                d = el.start(user=user, hostname=host, port=port)
                dlist.append(key)
                started_n += 1
                if started_n >= requested_n:
                    break
        self.notify_start(dlist)
        self.n = started_n
        return dlist


class SSHProxyEngineSetLauncher(SSHLauncher, EngineLauncher):
    """Launcher for calling
    `ipcluster engines` on a remote machine.

    Requires that remote profile is already configured.
    """

    n = Integer().tag(to_dict=True)
    ipcluster_cmd = List(Unicode(), config=True)

    @default("ipcluster_cmd")
    def _default_ipcluster_cmd(self):
        return [self.remote_python, "-m", "ipyparallel.cluster"]

    ipcluster_args = List(
        Unicode(),
        config=True,
        help="""Extra CLI arguments to pass to ipcluster engines""",
    )

    @property
    def program(self):
        return self.ipcluster_cmd + ['engines']

    @property
    def program_args(self):
        return [
            '-n',
            str(self.n),
            '--profile-dir',
            self.remote_profile_dir,
            '--cluster-id',
            self.cluster_id,
        ] + self.ipcluster_args

    @default("to_send")
    def _to_send_default(self):
        return [
            (local_path, self.remote_connection_files[key])
            for key, local_path in self.connection_files.items()
        ]

    def start(self, n):
        self.n = n
        super().start()


# -----------------------------------------------------------------------------
# Windows HPC Server 2008 scheduler launchers
# -----------------------------------------------------------------------------


class WindowsHPCLauncher(BaseLauncher):
    job_id_regexp = CRegExp(
        r'\d+',
        config=True,
        help="""A regular expression used to get the job id from the output of the
        submit_command. """,
    )
    job_file_name = Unicode(
        'ipython_job.xml',
        config=True,
        help="The filename of the instantiated job script.",
    )
    scheduler = Unicode(
        '', config=True, help="The hostname of the scheduler to submit the job to."
    )
    job_cmd = Unicode(config=True, help="The command for submitting jobs.")

    @default("job_cmd")
    def _default_job(self):
        return shutil.which("job") or "job"

    @property
    def job_file(self):
        return os.path.join(self.work_dir, self.job_file_name)

    def write_job_file(self, n):
        raise NotImplementedError("Implement write_job_file in a subclass.")

    def find_args(self):
        return ['job.exe']

    def parse_job_id(self, output):
        """Take the output of the submit command and return the job id."""
        m = self.job_id_regexp.search(output)
        if m is not None:
            job_id = m.group()
        else:
            raise LauncherError("Job id couldn't be determined: %s" % output)
        self.job_id = job_id
        self.log.info('Job started with id: %r', job_id)
        return job_id

    def start(self, n):
        """Start n copies of the process using the Win HPC job scheduler."""
        self.write_job_file(n)
        args = [
            'submit',
            '/jobfile:%s' % self.job_file,
            '/scheduler:%s' % self.scheduler,
        ]
        self.log.debug(
            "Starting Win HPC Job: {}".format(self.job_cmd + ' ' + ' '.join(args))
        )

        output = check_output(
            [self.job_cmd] + args, env=os.environ, cwd=self.work_dir, stderr=STDOUT
        )
        output = output.decode("utf8", 'replace')
        job_id = self.parse_job_id(output)
        self.notify_start(job_id)
        return job_id

    def stop(self):
        args = ['cancel', self.job_id, '/scheduler:%s' % self.scheduler]
        self.log.info(
            "Stopping Win HPC Job: {}".format(self.job_cmd + ' ' + ' '.join(args))
        )
        try:
            output = check_output(
                [self.job_cmd] + args, env=os.environ, cwd=self.work_dir, stderr=STDOUT
            )
            output = output.decode("utf8", 'replace')
        except Exception:
            output = 'The job already appears to be stopped: %r' % self.job_id
        self.notify_stop(
            dict(job_id=self.job_id, output=output)
        )  # Pass the output of the kill cmd
        return output


class WindowsHPCControllerLauncher(WindowsHPCLauncher):
    job_file_name = Unicode(
        'ipcontroller_job.xml', config=True, help="WinHPC xml job file."
    )
    controller_args = List([], config=False, help="extra args to pass to ipcontroller")

    def write_job_file(self, n):
        job = IPControllerJob(parent=self)

        t = IPControllerTask(parent=self)
        # The tasks work directory is *not* the actual work directory of
        # the controller. It is used as the base path for the stdout/stderr
        # files that the scheduler redirects to.
        t.work_directory = self.profile_dir
        # Add the profile_dir and from self.start().
        t.controller_args.extend(self.cluster_args)
        t.controller_args.extend(self.controller_args)
        job.add_task(t)

        self.log.debug("Writing job description file: %s", self.job_file)
        job.write(self.job_file)


class WindowsHPCEngineSetLauncher(WindowsHPCLauncher):
    job_file_name = Unicode(
        'ipengineset_job.xml', config=True, help="jobfile for ipengines job"
    )
    engine_args = List(Unicode(), config=False, help="extra args to pas to ipengine")

    def write_job_file(self, n):
        job = IPEngineSetJob(parent=self)

        for i in range(n):
            t = IPEngineTask(parent=self)
            # The tasks work directory is *not* the actual work directory of
            # the engine. It is used as the base path for the stdout/stderr
            # files that the scheduler redirects to.
            t.work_directory = self.profile_dir
            # Add the profile_dir and from self.start().
            t.engine_args.extend(self.cluster_args)
            t.engine_args.extend(self.engine_args)
            job.add_task(t)

        self.log.debug("Writing job description file: %s", self.job_file)
        job.write(self.job_file)

    def start(self, n):
        """Start the controller by profile_dir."""
        return super().start(n)


# -----------------------------------------------------------------------------
# Batch (PBS) system launchers
# -----------------------------------------------------------------------------


class BatchSystemLauncher(BaseLauncher):
    """Launch an external process using a batch system.

    This class is designed to work with UNIX batch systems like PBS, LSF,
    GridEngine, etc.  The overall model is that there are different commands
    like qsub, qdel, etc. that handle the starting and stopping of the process.

    This class also has the notion of a batch script. The ``batch_template``
    attribute can be set to a string that is a template for the batch script.
    This template is instantiated using string formatting. Thus the template can
    use {n} for the number of instances. Subclasses can add additional variables
    to the template dict.
    """

    # load cluster args into context instead of cli

    output_file = Unicode(
        config=True, help="File in which to store stdout/err of processes"
    ).tag(to_dict=True)

    @default("output_file")
    def _default_output_file(self):
        log_dir = os.path.join(self.profile_dir, "log")
        os.makedirs(log_dir, exist_ok=True)
        return os.path.join(log_dir, f'{self.identifier}.log')

    # Subclasses must fill these in.  See PBSEngineSet
    submit_command = List(
        [''],
        config=True,
        help="The name of the command line program used to submit jobs.",
    )
    delete_command = List(
        [''],
        config=True,
        help="The name of the command line program used to delete jobs.",
    )

    signal_command = List(
        [''],
        config=True,
        help="The name of the command line program used to send signals to jobs.",
    )

    job_id = Unicode().tag(to_dict=True)

    job_id_regexp = CRegExp(
        '',
        config=True,
        help="""A regular expression used to get the job id from the output of the
        submit_command.""",
    )
    job_id_regexp_group = Integer(
        0,
        config=True,
        help="""The group we wish to match in job_id_regexp (0 to match all)""",
    )
    batch_template = Unicode(
        '', config=True, help="The string that is the batch script template itself."
    ).tag(to_dict=True)
    batch_template_file = Unicode(
        '', config=True, help="The file that contains the batch template."
    )
    batch_file_name = Unicode(
        'batch_script',
        config=True,
        help="The filename of the instantiated batch script.",
    ).tag(to_dict=True)
    queue = Unicode('', config=True, help="The batch queue.").tag(to_dict=True)

    n = Integer(1).tag(to_dict=True)

    @observe('queue', 'n', 'cluster_id', 'profile_dir', 'output_file')
    def _context_field_changed(self, change):
        self._update_context(change)

    # not configurable, override in subclasses
    # Job Array regex
    job_array_regexp = CRegExp('')
    job_array_template = Unicode('')

    # Queue regex
    queue_regexp = CRegExp('')
    queue_template = Unicode('')

    # Output file
    output_regexp = CRegExp('')
    output_template = Unicode('')

    # The default batch template, override in subclasses
    default_template = Unicode('')
    # The full path to the instantiated batch script.
    batch_file = Unicode('')
    # the format dict used with batch_template:
    context = Dict()

    namespace = Dict(
        config=True,
        help="""Extra variables to pass to the template.

        This lets you parameterize additional options,
        such as wall_time with a custom template.
        """,
    ).tag(to_dict=True)

    @default("context")
    def _context_default(self):
        """load the default context with the default values for the basic keys

        because the _trait_changed methods only load the context if they
        are set to something other than the default value.
        """
        return dict(
            n=self.n,
            queue=self.queue,
            profile_dir=self.profile_dir,
            cluster_id=self.cluster_id,
            output_file=self.output_file,
        )

    program = List(Unicode())
    program_args = List(Unicode())

    @observe("program", "program_args")
    def _program_changed(self, change=None):
        self.context['program'] = shlex_join(self.program)
        self.context['program_args'] = shlex_join(self.program_args)
        self.context['program_and_args'] = shlex_join(self.program + self.program_args)

    @observe("n", "queue")
    def _update_context(self, change):
        self.context[change['name']] = change['new']

    # the Formatter instance for rendering the templates:
    formatter = Instance(EvalFormatter, (), {})

    def find_args(self):
        return self.submit_command + [self.batch_file]

    def __init__(self, work_dir='.', config=None, **kwargs):
        super().__init__(work_dir=work_dir, config=config, **kwargs)
        self.batch_file = os.path.join(self.work_dir, self.batch_file_name)
        # trigger program_changed to populate default context arguments
        self._program_changed()

    def parse_job_id(self, output):
        """Take the output of the submit command and return the job id."""
        m = self.job_id_regexp.search(output)
        if m is not None:
            job_id = m.group(self.job_id_regexp_group)
        else:
            raise LauncherError("Job id couldn't be determined: %s" % output)
        self.job_id = job_id
        self.log.info('Job submitted with job id: %r', job_id)
        return job_id

    def write_batch_script(self, n=1):
        """Instantiate and write the batch script to the work_dir."""
        self.n = n
        self.context['environment_json'] = json.dumps(self.get_env())

        # first priority is batch_template if set
        if self.batch_template_file and not self.batch_template:
            # second priority is batch_template_file
            with open(self.batch_template_file) as f:
                self.batch_template = f.read()
        if not self.batch_template:
            # third (last) priority is default_template
            self.batch_template = self.default_template
            # add jobarray or queue lines to user-specified template
            # note that this is *only* when user did not specify a template.
            self._insert_options_in_script()
            self._insert_job_array_in_script()
        ns = {}
        # internally generated
        ns.update(self.context)
        # from user config
        ns.update(self.namespace)
        script_as_string = self.formatter.format(self.batch_template, **ns)
        self.log.debug(f'Writing batch script: {self.batch_file}\n{script_as_string}')
        with open(self.batch_file, 'w') as f:
            f.write(script_as_string)
        os.chmod(self.batch_file, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

    def _insert_options_in_script(self):
        """Inserts a queue if required into the batch script."""
        inserts = []
        if self.queue and not self.queue_regexp.search(self.batch_template):
            self.log.debug(f"Adding queue={self.queue} to {self.batch_file}")
            inserts.append(self.queue_template)

        if (
            self.output_file
            and self.output_template
            and not self.output_regexp.search(self.batch_template)
        ):
            self.log.debug(f"Adding output={self.output_file} to {self.batch_file}")
            inserts.append(self.output_template)

        if inserts:
            firstline, rest = self.batch_template.split('\n', 1)
            self.batch_template = '\n'.join([firstline] + inserts + [rest])

    def _insert_job_array_in_script(self):
        """Inserts a job array if required into the batch script."""
        if not self.job_array_regexp.search(self.batch_template):
            self.log.debug("adding job array settings to batch script")
            firstline, rest = self.batch_template.split('\n', 1)
            self.batch_template = '\n'.join([firstline, self.job_array_template, rest])

    def start(self, n=1):
        """Start n copies of the process using a batch system."""
        self.log.debug("Starting %s: %r", self.__class__.__name__, self.args)
        # Here we save profile_dir in the context so they
        # can be used in the batch script template as {profile_dir}
        self.write_batch_script(n)

        env = os.environ.copy()
        env.update(self.get_env())
        output = check_output(self.args, env=env)
        output = output.decode("utf8", 'replace')
        self.log.debug(f"Submitted {shlex_join(self.args)}. Output: {output}")

        job_id = self.parse_job_id(output)
        self.notify_start(job_id)
        return job_id

    def stop(self):
        try:
            output = check_output(
                self.delete_command + [self.job_id],
                stdin=None,
            ).decode("utf8", 'replace')
        except Exception:
            self.log.exception(
                "Problem stopping cluster with command: %s"
                % (self.delete_command + [self.job_id])
            )
            output = ""

        self.notify_stop(
            dict(job_id=self.job_id, output=output)
        )  # Pass the output of the kill cmd
        return output

    def signal(self, sig):
        cmd = self.signal_command + [str(sig), self.job_id]
        try:
            output = check_output(
                cmd,
                stdin=None,
            ).decode("utf8", 'replace')
        except Exception:
            self.log.exception("Problem sending signal with: {shlex_join(cmd)}")
            output = ""

    # same local-file implementation as LocalProcess
    # should this be on the base class?
    _output = None

    def get_output(self, remove=True):
        return LocalProcessLauncher.get_output(self, remove=remove)

    def poll(self):
        """Poll not implemented

        Need to use `squeue` and friends to check job status
        """
        return None


class BatchControllerLauncher(BatchSystemLauncher, ControllerLauncher):
    @default("program")
    def _default_program(self):
        return self.controller_cmd

    @observe("controller_cmd")
    def _controller_cmd_changed(self, change):
        self.program = self._default_program()

    @default("program_args")
    def _default_program_args(self):
        return self.cluster_args + self.controller_args

    @observe("controller_args")
    def _controller_args_changed(self, change):
        self.program_args = self._default_program_args()

    def start(self):
        return super().start(n=1)


class BatchEngineSetLauncher(BatchSystemLauncher, EngineLauncher):
    @default("program")
    def _default_program(self):
        return self.engine_cmd

    @observe("engine_cmd")
    def _engine_cmd_changed(self, change):
        self.program = self._default_program()

    @default("program_args")
    def _default_program_args(self):
        return self.cluster_args + self.engine_args

    @observe("engine_args")
    def _engine_args_changed(self, change):
        self.program_args = self._default_program_args()


class PBSLauncher(BatchSystemLauncher):
    """A BatchSystemLauncher subclass for PBS."""

    submit_command = List(['qsub'], config=True, help="The PBS submit command ['qsub']")
    delete_command = List(['qdel'], config=True, help="The PBS delete command ['qdel']")
    signal_command = List(
        ['qsig', '-s'], config=True, help="The PBS signal command ['qsig']"
    )
    job_id_regexp = CRegExp(
        r'\d+',
        config=True,
        help=r"Regular expresion for identifying the job ID [r'\d+']",
    )

    batch_file = Unicode('')
    job_array_regexp = CRegExp(r'#PBS\W+-t\W+[\w\d\-\$]+')
    job_array_template = Unicode('#PBS -t 1-{n}')
    queue_regexp = CRegExp(r'#PBS\W+-q\W+\$?\w+')
    queue_template = Unicode('#PBS -q {queue}')
    output_regexp = CRegExp(r'#PBS\W+(?:-o)\W+\$?\w+')
    output_template = Unicode('#PBS -j oe\n#PBS -o {output_file}')


class PBSControllerLauncher(PBSLauncher, BatchControllerLauncher):
    """Launch a controller using PBS."""

    batch_file_name = Unicode(
        'pbs_controller', config=True, help="batch file name for the controller job."
    )
    default_template = Unicode(
        """#!/bin/sh
#PBS -V
#PBS -N ipcontroller
{program_and_args}
"""
    )


class PBSEngineSetLauncher(PBSLauncher, BatchEngineSetLauncher):
    """Launch Engines using PBS"""

    batch_file_name = Unicode(
        'pbs_engines', config=True, help="batch file name for the engine(s) job."
    )
    default_template = Unicode(
        """#!/bin/sh
#PBS -V
#PBS -N ipengine
{program_and_args}
"""
    )


# Slurm is very similar to PBS


class SlurmLauncher(BatchSystemLauncher):
    """A BatchSystemLauncher subclass for slurm."""

    submit_command = List(
        ['sbatch'], config=True, help="The slurm submit command ['sbatch']"
    )
    delete_command = List(
        ['scancel'], config=True, help="The slurm delete command ['scancel']"
    )
    signal_command = List(
        ['scancel', '-s'],
        config=True,
        help="The slurm signal command ['scancel', '-s']",
    )
    job_id_regexp = CRegExp(
        r'\d+',
        config=True,
        help=r"Regular expresion for identifying the job ID [r'\d+']",
    )

    account = Unicode("", config=True, help="Slurm account to be used")

    qos = Unicode("", config=True, help="Slurm QoS to be used")

    # Note: from the man page:
    #'Acceptable time formats include "minutes", "minutes:seconds",
    # "hours:minutes:seconds", "days-hours", "days-hours:minutes"
    # and "days-hours:minutes:seconds".
    timelimit = Any("", config=True, help="Slurm timelimit to be used")

    options = Unicode("", config=True, help="Extra Slurm options")

    @observe('account')
    def _account_changed(self, change):
        self._update_context(change)

    @observe('qos')
    def _qos_changed(self, change):
        self._update_context(change)

    @observe('timelimit')
    def _timelimit_changed(self, change):
        self._update_context(change)

    @observe('options')
    def _options_changed(self, change):
        self._update_context(change)

    batch_file = Unicode('')

    job_array_regexp = CRegExp(r'#SBATCH\W+(?:--ntasks|-n)[\w\d\-\$]+')
    job_array_template = Unicode('''#SBATCH --ntasks={n}''')

    queue_regexp = CRegExp(r'#SBATCH\W+(?:--partition|-p)\W+\$?\w+')
    queue_template = Unicode('#SBATCH --partition={queue}')

    account_regexp = CRegExp(r'#SBATCH\W+(?:--account|-A)\W+\$?\w+')
    account_template = Unicode('#SBATCH --account={account}')

    qos_regexp = CRegExp(r'#SBATCH\W+--qos\W+\$?\w+')
    qos_template = Unicode('#SBATCH --qos={qos}')

    timelimit_regexp = CRegExp(r'#SBATCH\W+(?:--time|-t)\W+\$?\w+')
    timelimit_template = Unicode('#SBATCH --time={timelimit}')

    output_regexp = CRegExp(r'#SBATCH\W+(?:--output)\W+\$?\w+')
    output_template = Unicode('#SBATCH --output={output_file}')

    def _insert_options_in_script(self):
        """Insert 'partition' (slurm name for queue), 'account', 'time' and other options if necessary"""
        super()._insert_options_in_script()
        inserts = []
        if self.account and not self.account_regexp.search(self.batch_template):
            self.log.debug("adding slurm account settings to batch script")
            inserts.append(self.account_template)

        if self.qos and not self.qos_regexp.search(self.batch_template):
            self.log.debug("adding Slurm qos settings to batch script")
            firstline, rest = self.batch_template.split('\n', 1)
            inserts.append(self.qos_template)

        if self.timelimit and not self.timelimit_regexp.search(self.batch_template):
            self.log.debug("adding slurm time limit settings to batch script")
            inserts.append(self.timelimit_template)

        if inserts:
            firstline, rest = self.batch_template.split('\n', 1)
            self.batch_template = '\n'.join([firstline] + inserts + [rest])


class SlurmControllerLauncher(SlurmLauncher, BatchControllerLauncher):
    """Launch a controller using Slurm."""

    batch_file_name = Unicode(
        'slurm_controller.sbatch',
        config=True,
        help="batch file name for the controller job.",
    )
    default_template = Unicode(
        """#!/bin/sh
#SBATCH --export=ALL
#SBATCH --job-name=ipcontroller-{cluster_id}
#SBATCH --ntasks=1
{program_and_args}
"""
    )


class SlurmEngineSetLauncher(SlurmLauncher, BatchEngineSetLauncher):
    """Launch Engines using Slurm"""

    batch_file_name = Unicode(
        'slurm_engine.sbatch',
        config=True,
        help="batch file name for the engine(s) job.",
    )
    default_template = Unicode(
        """#!/bin/sh
#SBATCH --export=ALL
#SBATCH --job-name=ipengine-{cluster_id}
srun {program_and_args}
"""
    )


# SGE is very similar to PBS


class SGELauncher(PBSLauncher):
    """Sun GridEngine is a PBS clone with slightly different syntax"""

    job_array_regexp = CRegExp(r'#\$\W+\-t')
    job_array_template = Unicode('#$ -t 1-{n}')
    queue_regexp = CRegExp(r'#\$\W+-q\W+\$?\w+')
    queue_template = Unicode('#$ -q {queue}')


class SGEControllerLauncher(SGELauncher, BatchControllerLauncher):
    """Launch a controller using SGE."""

    batch_file_name = Unicode(
        'sge_controller', config=True, help="batch file name for the ipontroller job."
    )
    default_template = Unicode(
        """#$ -V
#$ -S /bin/sh
#$ -N ipcontroller
{program_and_args}
"""
    )


class SGEEngineSetLauncher(SGELauncher, BatchEngineSetLauncher):
    """Launch Engines with SGE"""

    batch_file_name = Unicode(
        'sge_engines', config=True, help="batch file name for the engine(s) job."
    )
    default_template = Unicode(
        """#$ -V
#$ -S /bin/sh
#$ -N ipengine
{program_and_args}
"""
    )


# LSF launchers


class LSFLauncher(BatchSystemLauncher):
    """A BatchSystemLauncher subclass for LSF."""

    submit_command = List(['bsub'], config=True, help="The LSF submit command ['bsub']")
    delete_command = List(
        ['bkill'], config=True, help="The LSF delete command ['bkill']"
    )
    signal_command = List(
        ['bkill', '-s'], config=True, help="The LSF signal command ['bkill', '-s']"
    )
    job_id_regexp = CRegExp(
        r'\d+',
        config=True,
        help=r"Regular expresion for identifying the job ID [r'\d+']",
    )

    batch_file = Unicode('')
    job_array_regexp = CRegExp(r'#BSUB\s+-J+\w+\[\d+-\d+\]')
    job_array_template = Unicode('#BSUB -J ipengine[1-{n}]')
    queue_regexp = CRegExp(r'#BSUB\s+-q\s+\w+')
    queue_template = Unicode('#BSUB -q {queue}')
    output_regexp = CRegExp(r'#BSUB\s+-oo?\s+\w+')
    output_template = Unicode('#BSUB -o {output_file}\n#BSUB -e {output_file}\n')

    def start(self, n=1):
        """Start n copies of the process using LSF batch system.
        This cant inherit from the base class because bsub expects
        to be piped a shell script in order to honor the #BSUB directives :
        bsub < script
        """
        # Here we save profile_dir in the context so they
        # can be used in the batch script template as {profile_dir}
        self.write_batch_script(n)
        piped_cmd = self.args[0] + '<"' + self.args[1] + '"'
        self.log.debug("Starting %s: %s", self.__class__.__name__, piped_cmd)
        p = Popen(piped_cmd, shell=True, env=os.environ, stdout=PIPE)
        output, err = p.communicate()
        output = output.decode("utf8", 'replace')
        job_id = self.parse_job_id(output)
        self.notify_start(job_id)
        return job_id


class LSFControllerLauncher(LSFLauncher, BatchControllerLauncher):
    """Launch a controller using LSF."""

    batch_file_name = Unicode(
        'lsf_controller', config=True, help="batch file name for the controller job."
    )
    default_template = Unicode(
        """#!/bin/sh
    #BSUB -env all
    #BSUB -J ipcontroller-{cluster_id}
    {program_and_args}
    """
    )


class LSFEngineSetLauncher(LSFLauncher, BatchEngineSetLauncher):
    """Launch Engines using LSF"""

    batch_file_name = Unicode(
        'lsf_engines', config=True, help="batch file name for the engine(s) job."
    )
    default_template = Unicode(
        """#!/bin/sh
    #BSUB -J ipengine-{cluster_id}
    #BSUB -env all
    {program_and_args}
    """
    )

    def get_env(self):
        # write directly to output files
        # otherwise, will copy and clobber merged stdout/err
        env = {"LSB_STDOUT_DIRECT": "Y"}
        env.update(super().get_env())
        return env


class HTCondorLauncher(BatchSystemLauncher):
    """A BatchSystemLauncher subclass for HTCondor.

    HTCondor requires that we launch the ipengine/ipcontroller scripts rather
    that the python instance but otherwise is very similar to PBS.  This is because
    HTCondor destroys sys.executable when launching remote processes - a launched
    python process depends on sys.executable to effectively evaluate its
    module search paths. Without it, regardless of which python interpreter you launch
    you will get the to built in module search paths.

    We use the ip{cluster, engine, controller} scripts as our executable to circumvent
    this - the mechanism of shebanged scripts means that the python binary will be
    launched with argv[0] set to the *location of the ip{cluster, engine, controller}
    scripts on the remote node*. This means you need to take care that:

    a. Your remote nodes have their paths configured correctly, with the ipengine and ipcontroller
       of the python environment you wish to execute code in having top precedence.
    b. This functionality is untested on Windows.

    If you need different behavior, consider making you own template.
    """

    submit_command = List(
        ['condor_submit'],
        config=True,
        help="The HTCondor submit command ['condor_submit']",
    )
    delete_command = List(
        ['condor_rm'], config=True, help="The HTCondor delete command ['condor_rm']"
    )
    job_id_regexp = CRegExp(
        r'(\d+)\.$',
        config=True,
        help=r"Regular expression for identifying the job ID [r'(\d+)\.$']",
    )
    job_id_regexp_group = Integer(
        1, config=True, help="""The group we wish to match in job_id_regexp [1]"""
    )

    job_array_regexp = CRegExp(r'queue\W+\$')
    job_array_template = Unicode('queue {n}')

    def _insert_job_array_in_script(self):
        """Inserts a job array if required into the batch script."""
        if not self.job_array_regexp.search(self.batch_template):
            self.log.debug("adding job array settings to batch script")
            # HTCondor requires that the job array goes at the bottom of the script
            self.batch_template = '\n'.join(
                [self.batch_template, self.job_array_template]
            )

    def _insert_options_in_script(self):
        """AFAIK, HTCondor doesn't have a concept of multiple queues that can be
        specified in the script.
        """
        super()._insert_options_in_script()


class HTCondorControllerLauncher(HTCondorLauncher, BatchControllerLauncher):
    """Launch a controller using HTCondor."""

    batch_file_name = Unicode(
        'htcondor_controller',
        config=True,
        help="batch file name for the controller job.",
    )
    default_template = Unicode(
        r"""
universe        = vanilla
executable      = ipcontroller
# by default we expect a shared file system
transfer_executable = False
arguments       = {program_args}
"""
    )


class HTCondorEngineSetLauncher(HTCondorLauncher, BatchEngineSetLauncher):
    """Launch Engines using HTCondor"""

    batch_file_name = Unicode(
        'htcondor_engines', config=True, help="batch file name for the engine(s) job."
    )
    default_template = Unicode(
        """
universe        = vanilla
executable      = ipengine
# by default we expect a shared file system
transfer_executable = False
arguments       = "{program_args}"
"""
    )


# -----------------------------------------------------------------------------
# Collections of launchers
# -----------------------------------------------------------------------------

local_launchers = [
    LocalControllerLauncher,
    LocalEngineLauncher,
    LocalEngineSetLauncher,
]
mpi_launchers = [
    MPILauncher,
    MPIControllerLauncher,
    MPIEngineSetLauncher,
]
ssh_launchers = [
    SSHLauncher,
    SSHControllerLauncher,
    SSHEngineLauncher,
    SSHEngineSetLauncher,
    SSHProxyEngineSetLauncher,
]
winhpc_launchers = [
    WindowsHPCLauncher,
    WindowsHPCControllerLauncher,
    WindowsHPCEngineSetLauncher,
]
pbs_launchers = [
    PBSLauncher,
    PBSControllerLauncher,
    PBSEngineSetLauncher,
]
slurm_launchers = [
    SlurmLauncher,
    SlurmControllerLauncher,
    SlurmEngineSetLauncher,
]
sge_launchers = [
    SGELauncher,
    SGEControllerLauncher,
    SGEEngineSetLauncher,
]
lsf_launchers = [
    LSFLauncher,
    LSFControllerLauncher,
    LSFEngineSetLauncher,
]
htcondor_launchers = [
    HTCondorLauncher,
    HTCondorControllerLauncher,
    HTCondorEngineSetLauncher,
]
all_launchers = (
    local_launchers
    + mpi_launchers
    + ssh_launchers
    + winhpc_launchers
    + pbs_launchers
    + slurm_launchers
    + sge_launchers
    + lsf_launchers
    + htcondor_launchers
)


def find_launcher_class(name, kind):
    """Return a launcher class for a given name and kind.

    Parameters
    ----------
    name : str
        The full name of the launcher class, either with or without the
        module path, or an abbreviation (MPI, SSH, SGE, PBS, LSF, HTCondor
        Slurm, WindowsHPC).
    kind : str
        Either 'EngineSet' or 'Controller'.
    """
    if kind == 'engine':
        group_name = 'ipyparallel.engine_launchers'
    elif kind == 'controller':
        group_name = 'ipyparallel.controller_launchers'
    else:
        raise ValueError(f"kind must be 'engine' or 'controller', not {kind!r}")
    group = entrypoints.get_group_named(group_name)
    # make it case-insensitive
    registry = {key.lower(): value for key, value in group.items()}
    return registry[name.lower()].load()


@lru_cache
def abbreviate_launcher_class(cls):
    """Abbreviate a launcher class back to its entrypoint name"""
    cls_key = f"{cls.__module__}.{cls.__name__}"
    # allow entrypoint_name attribute in case the definition module
    # is not the same as the 'import' module
    if getattr(cls, 'entrypoint_name', None):
        return getattr(cls, 'entrypoint_name')

    for kind in ('controller', 'engine'):
        group_name = f'ipyparallel.{kind}_launchers'
        group = entrypoints.get_group_named(group_name)
        for key, value in group.items():
            if f"{value.module_name}.{value.object_name}" == cls_key:
                return key.lower()
    return cls_key
