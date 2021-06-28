"""Cluster class

defines the basic interface to a single IPython Parallel cluster

starts/stops/polls controllers, engines, etc.
"""
import asyncio
import atexit
import glob
import inspect
import json
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
from traitlets import Any
from traitlets import Bool
from traitlets import default
from traitlets import Dict
from traitlets import Float
from traitlets import import_item
from traitlets import Integer
from traitlets import List
from traitlets import Unicode
from traitlets.config import LoggingConfigurable

from . import launcher
from .._async import AsyncFirst
from ..traitlets import Launcher
from ..util import _all_profile_dirs
from ..util import _default_profile_dir
from ..util import _locate_profiles
from ..util import abbreviate_profile_dir

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

    cluster_id = Unicode(help="The id of the cluster (default: random string)").tag(
        to_dict=True
    )

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
    ).tag(to_dict=True)

    @default("profile_dir")
    def _default_profile_dir(self):
        return _default_profile_dir(profile=self.profile)

    profile = Unicode(
        "",
        help="""The profile name,
             a shortcut for specifying profile_dir within $IPYTHONDIR.""",
    )

    cluster_file = Unicode(
        help="The path to the cluster file for saving this cluster to disk"
    )

    @default("cluster_file")
    def _default_cluster_file(self):
        return os.path.join(
            self.profile_dir, "security", f"cluster-{self.cluster_id}.json"
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
    ).tag(to_dict=True)
    controller_ip = Unicode(
        config=True, help="Set the IP address of the controller."
    ).tag(to_dict=True)
    controller_location = Unicode(
        config=True,
        help="""Set the location (hostname or ip) of the controller.

        This is used by engines and clients to locate the controller
        when the controller listens on all interfaces
        """,
    ).tag(to_dict=True)

    # engine configuration

    delay = Float(
        1.0,
        config=True,
        help="delay (in s) between starting the controller and the engines",
    ).tag(to_dict=True)

    n = Integer(
        None, allow_none=True, config=True, help="The number of engines to start"
    ).tag(to_dict=True)

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

    def to_dict(self):
        """Serialize a Cluster object for later reconstruction"""
        cluster_info = {}
        d = {"cluster": cluster_info}
        for attr in self.traits(to_dict=True):
            cluster_info[attr] = getattr(self, attr)

        def _cls_str(cls):
            return f"{cls.__module__}.{cls.__name__}"

        cluster_info["class"] = _cls_str(self.__class__)

        if self._controller:
            d["controller"] = {
                "class": _cls_str(self.controller_launcher_class),
                "state": None,
            }
            if self._controller:
                d["controller"]["state"] = self._controller.to_dict()

        d["engines"] = {
            "class": _cls_str(self.engine_launcher_class),
            "sets": {},
        }
        sets = d["engines"]["sets"]
        for engine_set_id, engine_launcher in self._engine_sets.items():
            sets[engine_set_id] = engine_launcher.to_dict()
        return d

    @classmethod
    def from_dict(cls, d, **kwargs):
        """Construct a Cluster from serialized state"""
        cluster_info = d["cluster"]
        if cluster_info.get("class"):
            specified_cls = import_item(cluster_info["class"])
            if specified_cls is not cls:
                # specified a custom Cluster class,
                # dispatch to from_dict from that class
                return specified_cls.from_dict(d, **kwargs)

        kwargs.setdefault("shutdown_atexit", False)
        self = cls(**kwargs)
        for attr in self.traits(to_dict=True):
            if attr in cluster_info:
                setattr(self, attr, cluster_info[attr])

        for attr in self.traits(to_dict=True):
            if attr in d:
                setattr(self, attr, d[attr])

        if d.get("controller"):
            controller_info = d["controller"]
            cls = self.controller_launcher_class = import_item(controller_info["class"])
            if controller_info["state"]:
                self._controller = cls.from_dict(controller_info["state"], parent=self)

        engine_info = d.get("engines")
        if engine_info:
            cls = self.engine_launcher_class = import_item(engine_info["class"])
            for engine_set_id, engine_state in engine_info.get("sets", {}).items():
                self._engine_sets[engine_set_id] = cls.from_dict(
                    engine_state, parent=self
                )

        return self

    @classmethod
    def from_file(
        cls,
        cluster_file=None,
        *,
        profile=None,
        profile_dir=None,
        cluster_id='',
        **kwargs,
    ):
        """Load a Cluster object from a file

        Can specify a full path,
        or combination of profile, profile_dir, and/or cluster_id.

        With no arguments given, it will connect to a cluster created
        with `ipcluster start`.
        """

        if cluster_file is None:
            # determine cluster_file from profile/profile_dir

            kwargs['cluster_id'] = cluster_id
            if profile is not None:
                kwargs['profile'] = profile
            if profile_dir is not None:
                kwargs['profile_dir'] = profile_dir
            cluster_file = Cluster(**kwargs).cluster_file

        # ensure from_file preserves cluster_file, even if it moved
        kwargs.setdefault("cluster_file", cluster_file)
        with open(cluster_file) as f:
            return cls.from_dict(json.load(f), **kwargs)

    def write_cluster_file(self):
        """Write cluster info to disk for later loading"""
        os.makedirs(os.path.dirname(self.cluster_file), exist_ok=True)
        with open(self.cluster_file, "w") as f:
            json.dump(self.to_dict(), f)

    def remove_cluster_file(self):
        """Remove my cluster file."""
        try:
            os.remove(self.cluster_file)
        except FileNotFoundError:
            pass
        else:
            self.log.debug(f"Removed cluster file: {self.cluster_file}")

    def update_cluster_file(self):
        """Update my cluster file

        If cluster_file is disabled, do nothing
        If cluster is fully stopped, remove the file
        """
        if not self.cluster_file:
            # setting cluster_file='' disables saving to disk
            return

        if not self._controller and not self._engine_sets:
            self.remove_cluster_file()
        else:
            self.write_cluster_file()

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

        if controller_args is not None:
            # ensure we trigger trait observers after we are done
            self._controller.controller_args = list(controller_args)

        self._controller.on_stop(self._controller_stopped)
        r = self._controller.start()
        if inspect.isawaitable(r):
            await r

        self.update_cluster_file()

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
        self.update_cluster_file()
        return engine_set_id

    def _engines_stopped(self, engine_set_id, stop_data=None):
        self.log.warning(f"engine set stopped {engine_set_id}: {stop_data}")
        self.update_cluster_file()

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
        self.update_cluster_file()

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
        self.update_cluster_file()

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

    def _cluster_key(self, cluster):
        """Return a unique cluster key for a cluster

        Default is {profile}:{cluster_id}
        """
        return f"{abbreviate_profile_dir(cluster.profile_dir)}:{cluster.cluster_id}"

    @staticmethod
    def _cluster_files_in_profile_dir(profile_dir):
        """List clusters in a profile directory

        Returns list of cluster *files*
        """
        return glob.glob(os.path.join(profile_dir, "security", "cluster-*.json"))

    def load_clusters(
        self,
        *,
        profile_dirs=None,
        profile_dir=None,
        profiles=None,
        profile=None,
        **kwargs,
    ):
        """Populate a ClusterManager from cluster files on disk

        Load all cluster objects from the given profile directory(ies).

        Default is to find clusters in all IPython profiles,
        but profile directories or profile names can be specified explicitly.

        Priority:

        - profile_dirs list
        - single profile_dir
        - profiles list by name
        - single profile by name
        - all IPython profiles, if nothing else specified
        """
        if profile_dirs is None:
            if profile_dir is not None:
                profile_dirs = [profile_dir]
            else:
                if profiles is None:
                    if profile is not None:
                        profiles = [profile]

                if profiles is not None:
                    profile_dirs = _locate_profiles(profiles)

            if profile_dirs is None:
                # totally unspecified, default to all
                profile_dirs = _all_profile_dirs()

        for profile_dir in profile_dirs:
            for cluster_file in self._cluster_files_in_profile_dir(profile_dir):
                try:
                    cluster = Cluster.from_file(cluster_file, parent=self)
                except Exception as e:
                    self.log.warning(f"Failed to load cluster from {cluster_file}: {e}")
                    continue
                else:
                    cluster_key = self._cluster_key(cluster)
                    self._clusters[cluster_key] = cluster

        return self._clusters

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
