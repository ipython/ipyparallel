"""Cluster class

defines the basic interface to a single IPython Parallel cluster

starts/stops/polls controllers, engines, etc.
"""
import asyncio
import inspect
import logging
import random
import socket
import string
import sys
import time
from functools import partial
from multiprocessing import cpu_count

import IPython
import traitlets.log
from IPython.core.profiledir import ProfileDir
from traitlets import Any
from traitlets import default
from traitlets import Dict
from traitlets import Integer
from traitlets import Type
from traitlets import Unicode
from traitlets.config import LoggingConfigurable

from . import launcher
from .._async import AsyncFirst

_suffix_chars = string.ascii_lowercase + string.digits


class Cluster(AsyncFirst, LoggingConfigurable):
    """Class representing an IPP cluster

    i.e. one controller and a groups of engines

    Can start/stop/monitor/poll cluster resources
    """

    controller_launcher_class = Type(
        default_value=launcher.LocalControllerLauncher,
        klass=launcher.BaseLauncher,
        help="""Launcher class for controllers""",
    )
    engine_launcher_class = Type(
        default_value=launcher.LocalEngineSetLauncher,
        klass=launcher.BaseLauncher,
        help="""Launcher class for sets of engines""",
    )

    cluster_id = Unicode(help="The id of the cluster (default: random string)")

    @default("cluster_id")
    def _default_cluster_id(self):
        return f"{socket.gethostname()}-{int(time.time())}-{''.join(random.choice(_suffix_chars) for i in range(4))}"

    profile_dir = Unicode(
        help="""The profile directory.

    Default priority:

    - specified explicitly
    - current IPython session
    - use profile name (default: 'default')

    """
    )

    @default("profile_dir")
    def _default_profile_dir(self):
        if not self.profile:
            ip = IPython.get_ipython()
            if ip is not None:
                return ip.profile_dir.location
        return ProfileDir.find_profile_dir_by_name(
            IPython.paths.get_ipython_dir(), name=self.profile or 'default'
        ).location

    profile = Unicode(
        "",
        help="""The profile name,
             a shortcut for specifying profile_dir within $IPYTHONDIR.""",
    )

    n = Integer(None, allow_none=True, help="The number of engines to start")

    @default("parent")
    def _default_parent(self):
        """Default to inheriting config from current IPython session"""
        return IPython.get_ipython()

    log_level = Integer(logging.INFO)

    @default("log")
    def _default_log(self):
        if self.parent and self.parent is IPython.get_ipython():
            # log to stdout in an IPython session
            log = logging.getLogger(f"{__name__}.{self.cluster_id}")
            log.setLevel(self.log_level)

            handler = logging.StreamHandler(sys.stdout)
            log.handlers = [handler]
            return log
        else:
            return traitlets.log.get_logger()

    # private state
    _controller = Any()
    _engine_sets = Dict()

    @classmethod
    def from_json(self, json_dict):
        """Construct a Cluster from serialized state"""
        raise NotImplementedError()

    def to_json(self):
        """Serialize a Cluster object for later reconstruction"""
        raise NotImplementedError()

    async def start_controller(self):
        """Start the controller"""
        # start controller
        # retrieve connection info
        # webhook?
        if self._controller is not None:
            raise RuntimeError(
                "controller is already running. Call stop_controller() first."
            )
        self._controller = self.controller_launcher_class(
            work_dir=u'.',
            parent=self,
            log=self.log,
            profile_dir=self.profile_dir,
            cluster_id=self.cluster_id,
        )
        self._controller.on_stop(self._controller_stopped)
        r = self._controller.start()
        if inspect.isawaitable(r):
            await r
        # TODO: retrieve connection info

    def _controller_stopped(self, stop_data=None):
        """Callback when a controller stops"""
        self.log.info(f"Controller stopped: {stop_data}")

    async def start_engines(self, n=None, engine_set_id=None):
        """Start an engine set

        Returns an engine set id which can be used in stop_engines
        """
        # TODO: send engines connection info
        if engine_set_id is None:
            engine_set_id = f"{int(time.time())}-{''.join(random.choice(_suffix_chars) for i in range(4))}"
        engine_set = self._engine_sets[engine_set_id] = self.engine_launcher_class(
            work_dir=u'.',
            parent=self,
            log=self.log,
            profile_dir=self.profile_dir,
            cluster_id=self.cluster_id,
        )
        if n is None:
            n = self.n
        n = getattr(engine_set, 'engine_count', n)
        if n is None:
            n = cpu_count()
        self.log.info(f"Starting {n or ''} engines with {self.engine_launcher_class}")
        r = engine_set.start(n)
        engine_set.on_stop(self._engines_stopped)
        if inspect.isawaitable(r):
            await r
        return engine_set_id

    def _engines_stopped(self, engine_set_id, stop_data=None):
        self.log.warning(f"engine set stopped {engine_set_id}: {stop_data}")

    async def start_cluster(self, n=None):
        """Start a cluster

        starts one controller and n engines (default: self.n)
        """
        await self.start_controller()
        await self.start_engines(n)

    async def stop_engines(self, engine_set_id=None):
        """Stop an engine set

        If engine_set_id is not given,
        all engines are stopped"""
        if engine_set_id is None:
            for engine_set_id in list(self._engine_sets):
                await self.stop_engines(engine_set_id)
            return

        engine_set = self._engine_sets[engine_set_id]
        r = engine_set.stop()
        if inspect.isawaitable(r):
            await r
        self._engine_sets.pop(engine_set_id)

    async def stop_engine(self, engine_id):
        """Stop one engine

        *May* stop all engines in a set,
        depending on EngineSet features (e.g. mpiexec)
        """
        raise NotImplementedError("How do we find an engine by id?")

    async def restart_engine_set(self, engine_set_id):
        """Restart an engine set"""
        engine_set = self._engine_sets[engine_set_id]
        n = engine_set.n
        await self.stop_engines(engine_set_id)
        await self.start_engines(n, engine_set_id)

    async def restart_engine(self, engine_id):
        """Restart one engine

        *May* stop all engines in a set,
        depending on EngineSet features (e.g. mpiexec)
        """
        raise NotImplementedError("How do we find an engine by id?")

    async def signal_engine(self, engine_id, signum):
        """Signal one engine

        *May* signal all engines in a set,
        depending on EngineSet features (e.g. mpiexec)
        """
        raise NotImplementedError("How do we find an engine by id?")

    async def signal_engines(self, engine_set_id, signum):
        """Signal all engines in a set"""
        engine_set = self._engine_sets[engine_set_id]
        engine_set.signal(signum)

    async def stop_controller(self):
        """Stop the controller"""
        if self._controller and self._controller.running:
            r = self._controller.stop()
            if inspect.isawaitable(r):
                await r

        self._controller = None

    async def stop_cluster(self):
        """Stop the controller and all engines"""
        await self.stop_engines()
        await self.stop_controller()

    def connect_client(self):
        """Return a client connected to the cluster"""
        # TODO: get connect info directly from controller
        # this assumes local files exist
        from ipyparallel import Client

        return Client(
            parent=self, profile_dir=self.profile_dir, cluster_id=self.cluster_id
        )

    # context managers (both async and sync)
    _context_client = None

    async def __aenter__(self):
        await self.start_controller()
        await self.start_engines()
        client = self._context_client = self.connect_client()
        if self.n:
            # wait for engine registration
            # TODO: timeout
            while len(client) < self.n:
                await asyncio.sleep(0.1)
        return client

    async def __aexit__(self, *args):
        if self._context_client is not None:
            self._context_client.close()
            self._context_client = None
        await self.stop_engines()
        await self.stop_controller()

    def __enter__(self):
        self.start_controller_sync()
        self.start_engines_sync()
        client = self._context_client = self.connect_client()
        if self.n:
            # wait for engine registration
            while len(client) < self.n:
                time.sleep(0.1)
        return client

    def __exit__(self, *args):
        if self._context_client:
            self._context_client.close()
            self._context_client = None
        self.stop_engines_sync()
        self.stop_controller_sync()


class ClusterManager(LoggingConfigurable):
    """A manager of clusters

    Wraps Cluster, adding"""

    _clusters = Dict(help="My cluster objects")

    def load_clusters(self):
        """Load serialized cluster state"""
        raise NotImplementedError()

    def list_clusters(self):
        """List current clusters"""

    def new_cluster(self, cluster_cls):
        """Create a new cluster"""

    def _cluster_method(self, method_name, cluster_id, *args, **kwargs):
        """Wrapper around single-cluster methods

        Defines ClusterManager.method(cluster_id, ...)

        which returns ClusterManager.clusters[cluster_id].method(...)
        """
        cluster = self._clusters[cluster_id]
        method = getattr(cluster, method_name)
        return method(*args, **kwargs)

    def __getattr__(self, key):
        if key in Cluster.__dict__:
            return partial(self._cluster_method, key)
        return super().__getattr__(self, key)
