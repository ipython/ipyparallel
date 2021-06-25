"""Cluster class

defines the basic interface to a single IPython Parallel cluster

starts/stops/polls controllers, engines, etc.
"""
import asyncio
import atexit
import inspect
import logging
import os
import random
import socket
import string
import sys
import time
from functools import partial
from multiprocessing import cpu_count
from weakref import WeakSet

import IPython
import traitlets.log
from IPython.core.profiledir import ProfileDir
from IPython.core.profiledir import ProfileDirError
from traitlets import Any
from traitlets import Bool
from traitlets import default
from traitlets import Dict
from traitlets import Float
from traitlets import Integer
from traitlets import List
from traitlets import Unicode
from traitlets.config import LoggingConfigurable

from . import launcher
from .._async import AsyncFirst
from ..traitlets import Launcher

_suffix_chars = string.ascii_lowercase + string.digits

# weak set of clusters to be cleaned up at exit
_atexit_clusters = WeakSet()


def _atexit_cleanup_clusters(*args):
    """Cleanup clusters during process shutdown"""
    for cluster in _atexit_clusters:
        if not cluster.shutdown_atexit:
            # overridden after register
            continue
        if cluster._controller or cluster._engine_sets:
            print(f"Stopping cluster {cluster}", file=sys.stderr)
            cluster.stop_cluster_sync()


_atexit_cleanup_clusters.registered = False


class Cluster(AsyncFirst, LoggingConfigurable):
    """Class representing an IPP cluster

    i.e. one controller and a groups of engines

    Can start/stop/monitor/poll cluster resources

    All async methods can be called synchronously with a `_sync` suffix,
    e.g. `cluster.start_cluster_sync()`
    """

    # general configuration

    shutdown_atexit = Bool(
        True,
        help="""
        Shutdown the cluster at process exit.

        Set to False if you want to launch a cluster and leave it running
        after the launching process exits.
        """,
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
        ipython_dir = IPython.paths.get_ipython_dir()
        profile_name = self.profile or 'default'
        try:
            pd = ProfileDir.find_profile_dir_by_name(ipython_dir, name=profile_name)
        except ProfileDirError:
            pd = ProfileDir.create_profile_dir_by_name(ipython_dir, name=profile_name)
        return pd.location

    profile = Unicode(
        "",
        help="""The profile name,
             a shortcut for specifying profile_dir within $IPYTHONDIR.""",
    )

    engine_timeout = Integer(
        60,
        help="""Timeout to use when waiting for engines to register

        before giving up.
        """,
        config=True,
    )

    controller_launcher_class = Launcher(
        default_value=launcher.LocalControllerLauncher,
        kind='Controller',
        help="""The class for launching a Controller. Change this value if you want
        your controller to also be launched by a batch system, such as PBS,SGE,MPI,etc.

        Each launcher class has its own set of configuration options, for making sure
        it will work in your environment.

        Note that using a batch launcher for the controller *does not* put it
        in the same batch job as the engines, so they will still start separately.

        IPython's bundled examples include:

            Local : start engines locally as subprocesses
            MPI : use mpiexec to launch the controller in an MPI universe
            PBS : use PBS (qsub) to submit the controller to a batch queue
            SGE : use SGE (qsub) to submit the controller to a batch queue
            LSF : use LSF (bsub) to submit the controller to a batch queue
            HTCondor : use HTCondor to submit the controller to a batch queue
            Slurm : use Slurm to submit engines to a batch queue
            SSH : use SSH to start the controller
            WindowsHPC : use Windows HPC

        If you are using one of IPython's builtin launchers, you can specify just the
        prefix, e.g:

            c.Cluster.controller_launcher_class = 'SSH'

        or:

            ipcluster start --controller=MPI

        """,
        config=True,
    )

    engine_launcher_class = Launcher(
        default_value=launcher.LocalEngineSetLauncher,
        kind='EngineSet',
        help="""The class for launching a set of Engines. Change this value
        to use various batch systems to launch your engines, such as PBS,SGE,MPI,etc.
        Each launcher class has its own set of configuration options, for making sure
        it will work in your environment.

        You can also write your own launcher, and specify it's absolute import path,
        as in 'mymodule.launcher.FTLEnginesLauncher`.

        IPython's bundled examples include:

            Local : start engines locally as subprocesses [default]
            MPI : use mpiexec to launch engines in an MPI environment
            PBS : use PBS (qsub) to submit engines to a batch queue
            SGE : use SGE (qsub) to submit engines to a batch queue
            LSF : use LSF (bsub) to submit engines to a batch queue
            SSH : use SSH to start the controller
                        Note that SSH does *not* move the connection files
                        around, so you will likely have to do this manually
                        unless the machines are on a shared file system.
            HTCondor : use HTCondor to submit engines to a batch queue
            Slurm : use Slurm to submit engines to a batch queue
            WindowsHPC : use Windows HPC

        If you are using one of IPython's builtin launchers, you can specify just the
        prefix, e.g:

            c.Cluster.engine_launcher_class = 'SSH'

        or:

            ipcluster start --engines=MPI

        """,
        config=True,
    )

    # controller configuration

    controller_args = List(
        Unicode(),
        config=True,
        help="Additional CLI args to pass to the controller.",
    )
    controller_ip = Unicode(config=True, help="Set the IP address of the controller.")
    controller_location = Unicode(
        config=True,
        help="""Set the location (hostname or ip) of the controller.

        This is used by engines and clients to locate the controller
        when the controller listens on all interfaces
        """,
    )

    # engine configuration

    delay = Float(
        1.0,
        config=True,
        help="delay (in s) between starting the controller and the engines",
    )

    n = Integer(
        None, allow_none=True, config=True, help="The number of engines to start"
    )

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

    def __del__(self):
        if not self.shutdown_atexit:
            return
        if self._controller or self._engine_sets:
            self.stop_cluster_sync()

    def __repr__(self):

        fields = {
            "cluster_id": repr(self.cluster_id),
        }
        profile_dir = self.profile_dir
        profile_prefix = os.path.join(IPython.paths.get_ipython_dir(), "profile_")
        if profile_dir.startswith(profile_prefix):
            fields["profile"] = repr(profile_dir[len(profile_prefix) :])
        else:
            home_dir = os.path.expanduser("~")

            if profile_dir.startswith(home_dir + os.path.sep):
                # truncate $HOME/. -> ~/...
                profile_dir = "~" + profile_dir[len(home_dir) :]
            fields["profile_dir"] = repr(profile_dir)

        if self._controller:
            fields["controller"] = "<running>"
        if self._engine_sets:
            fields["engine_sets"] = list(self._engine_sets)

        fields_str = ', '.join(f"{key}={value}" for key, value in fields.items())

        return f"<{self.__class__.__name__}({fields_str})>"

    @classmethod
    def from_json(self, json_dict):
        """Construct a Cluster from serialized state"""
        raise NotImplementedError()

    def to_json(self):
        """Serialize a Cluster object for later reconstruction"""
        raise NotImplementedError()

    async def start_controller(self, **kwargs):
        """Start the controller

        Keyword arguments are passed to the controller launcher constructor
        """
        # start controller
        # retrieve connection info
        # webhook?
        if self._controller is not None:
            raise RuntimeError(
                "controller is already running. Call stop_controller() first."
            )

        if self.shutdown_atexit:
            _atexit_clusters.add(self)
            if not _atexit_cleanup_clusters.registered:
                atexit.register(_atexit_cleanup_clusters)

        self._controller = controller = self.controller_launcher_class(
            work_dir=u'.',
            parent=self,
            log=self.log,
            profile_dir=self.profile_dir,
            cluster_id=self.cluster_id,
            **kwargs,
        )

        controller_args = getattr(controller, 'controller_args', None)
        if controller_args is None:

            def add_args(args):
                # only some Launchers support modifying controller args
                self.log.warning(
                    "Not adding controller args %s. "
                    "controller_args passthrough is not supported by %s",
                    args,
                    self.controller_launcher_class.__name__,
                )

        else:
            add_args = controller_args.extend
        if self.controller_ip:
            add_args(['--ip=%s' % self.controller_ip])
        if self.controller_location:
            add_args(['--location=%s' % self.controller_location])
        if self.controller_args:
            add_args(self.controller_args)

        self._controller.on_stop(self._controller_stopped)
        r = self._controller.start()
        if inspect.isawaitable(r):
            await r
        # TODO: retrieve connection info

    def _controller_stopped(self, stop_data=None):
        """Callback when a controller stops"""
        self.log.info(f"Controller stopped: {stop_data}")

    async def start_engines(self, n=None, engine_set_id=None, **kwargs):
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
            **kwargs,
        )
        if n is None:
            n = self.n
        n = getattr(engine_set, 'engine_count', n)
        if n is None:
            n = cpu_count()
        self.log.info(f"Starting {n or ''} engines with {self.engine_launcher_class}")
        r = engine_set.start(n)
        engine_set.on_stop(partial(self._engines_stopped, engine_set_id))
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
        if self.delay:
            await asyncio.sleep(self.delay)
        await self.start_engines(n)

    async def stop_engines(self, engine_set_id=None):
        """Stop an engine set

        If engine_set_id is not given,
        all engines are stopped.
        """
        if engine_set_id is None:
            for engine_set_id in list(self._engine_sets):
                await self.stop_engines(engine_set_id)
            return
        self.log.info(f"Stopping engine(s): {engine_set_id}")
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

    async def restart_engines(self, engine_set_id=None):
        """Restart an engine set"""
        if engine_set_id is None:
            for engine_set_id in list(self._engine_sets):
                await self.restart_engines(engine_set_id)
            return
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

    async def signal_engine(self, signum, engine_id):
        """Signal one engine

        *May* signal all engines in a set,
        depending on EngineSet features (e.g. mpiexec)
        """
        raise NotImplementedError("How do we find an engine by id?")

    async def signal_engines(self, signum, engine_set_id=None):
        """Signal all engines in a set

        If no engine set is specified, signal all engine sets.
        """
        if engine_set_id is None:
            for engine_set_id in list(self._engine_sets):
                await self.signal_engines(signum, engine_set_id)
            return
        self.log.info(f"Sending signal {signum} to engine(s) {engine_set_id}")
        engine_set = self._engine_sets[engine_set_id]
        r = engine_set.signal(signum)
        if inspect.isawaitable(r):
            await r

    async def stop_controller(self):
        """Stop the controller"""
        if self._controller and self._controller.running:
            self.log.info("Stopping controller")
            r = self._controller.stop()
            if inspect.isawaitable(r):
                await r

        self._controller = None

    async def stop_cluster(self):
        """Stop the controller and all engines"""
        await self.stop_engines()
        await self.stop_controller()

    async def connect_client(self, **client_kwargs):
        """Return a client connected to the cluster"""
        # TODO: get connect info directly from controller
        # this assumes local files exist
        from ipyparallel import Client

        connection_info = self._controller.get_connection_info()
        if inspect.isawaitable(connection_info):
            connection_info = await connection_info

        return Client(
            connection_info['client'],
            cluster=self,
            profile_dir=self.profile_dir,
            cluster_id=self.cluster_id,
            **client_kwargs,
        )

    # context managers (both async and sync)
    _context_client = None

    async def __aenter__(self):
        await self.start_controller()
        await self.start_engines()
        client = self._context_client = await self.connect_client()
        if self.n:
            # wait for engine registration
            await asyncio.wrap_future(
                client.wait_for_engines(
                    self.n, block=False, timeout=self.engine_timeout
                )
            )
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
        client = self._context_client = self.connect_client_sync()
        if self.n:
            # wait for engine registration
            client.wait_for_engines(self.n, block=True, timeout=self.engine_timeout)
        return client

    def __exit__(self, *args):
        if self._context_client:
            self._context_client.close()
            self._context_client = None
        self.stop_engines_sync()
        self.stop_controller_sync()


class ClusterManager(LoggingConfigurable):
    """A manager of clusters

    Wraps Cluster, adding lookup/list by cluster id
    """

    _clusters = Dict(help="My cluster objects")

    def from_dict(self, serialized_state):
        """Load serialized cluster state"""
        raise NotImplementedError("Serializing clusters not implemented")

    def list_clusters(self):
        """List current clusters"""
        # TODO: what should we return?
        # just cluster ids or the full dict?
        # just cluster ids for now
        return sorted(self._clusters)

    def new_cluster(self, **kwargs):
        """Create a new cluster"""
        cluster = Cluster(parent=self, **kwargs)
        if cluster.cluster_id in self._clusters:
            raise KeyError(f"Cluster {cluster.cluster_id} already exists!")
        self._clusters[cluster.cluster_id] = cluster
        return cluster

    def get_cluster(self, cluster_id):
        """Get a Cluster object by id"""
        return self._clusters[cluster_id]

    def remove_cluster(self, cluster_id):
        """Delete a cluster by id"""
        # TODO: check running?
        del self._clusters[cluster_id]
