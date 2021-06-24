import asyncio
import logging
import os
import shutil
import signal
import sys
import time

import pytest
from traitlets.config import Config

from .clienttest import raises_remote
from ipyparallel import cluster
from ipyparallel.cluster.launcher import find_launcher_class

_engine_launcher_classes = ["Local"]
if shutil.which("mpiexec"):
    _engine_launcher_classes.append("MPI")

_timeout = 30


@pytest.fixture
def Cluster(request, io_loop):
    """Fixture for instantiating Clusters"""

    def ClusterConstructor(**kwargs):
        log = logging.getLogger(__file__)
        log.setLevel(logging.DEBUG)
        log.handlers = [logging.StreamHandler(sys.stdout)]
        kwargs['log'] = log
        engine_launcher_class = kwargs.get("engine_launcher_class")

        if (
            isinstance(engine_launcher_class, str)
            and "MPI" in engine_launcher_class
            and shutil.which("mpiexec") is None
        ):
            pytest.skip("requires mpiexec")

        cfg = kwargs.setdefault("config", Config())
        cfg.EngineLauncher.engine_args = ['--log-level=10']
        cfg.ControllerLauncher.controller_args = ['--log-level=10']
        kwargs.setdefault("controller_args", ['--ping=250'])

        c = cluster.Cluster(**kwargs)
        assert c.config is cfg
        request.addfinalizer(c.stop_cluster_sync)
        return c

    yield ClusterConstructor


async def test_cluster_id(Cluster):
    cluster_ids = set()
    for i in range(3):
        cluster = Cluster()
        cluster_ids.add(cluster.cluster_id)
    assert len(cluster_ids) == 3
    cluster = Cluster(cluster_id='abc')
    assert cluster.cluster_id == 'abc'


async def test_ipython_log(ipython):
    c = cluster.Cluster(parent=ipython)
    assert c.log.name == f"{cluster.Cluster.__module__}.{c.cluster_id}"
    assert len(c.log.handlers) == 1
    assert c.log.handlers[0].stream is sys.stdout


async def test_start_stop_controller(Cluster):
    cluster = Cluster()
    await cluster.start_controller()
    with pytest.raises(RuntimeError):
        await cluster.start_controller()
    assert cluster.config is not None
    assert cluster._controller.config is cluster.config
    assert cluster._controller is not None
    proc = cluster._controller.process
    assert proc.poll() is None
    with await cluster.connect_client() as rc:
        assert rc.queue_status() == {'unassigned': 0}

    await cluster.stop_controller()
    assert not proc.poll() is not None
    assert cluster._controller is None
    # stop is idempotent
    await cluster.stop_controller()
    # TODO: test file cleanup


@pytest.mark.parametrize("engine_launcher_class", _engine_launcher_classes)
async def test_start_stop_engines(Cluster, engine_launcher_class):
    cluster = Cluster(engine_launcher_class=engine_launcher_class)
    await cluster.start_controller()

    n = 3
    engine_set_id = await cluster.start_engines(n)
    assert engine_set_id in cluster._engine_sets
    engine_set = cluster._engine_sets[engine_set_id]
    launcher_class = find_launcher_class(engine_launcher_class, "EngineSet")
    assert isinstance(engine_set, launcher_class)

    with await cluster.connect_client() as rc:
        rc.wait_for_engines(n, timeout=_timeout)

    await cluster.stop_engines(engine_set_id)
    assert cluster._engine_sets == {}
    with pytest.raises(KeyError):
        await cluster.stop_engines(engine_set_id)

    await cluster.stop_controller()


@pytest.mark.parametrize("engine_launcher_class", _engine_launcher_classes)
async def test_start_stop_cluster(Cluster, engine_launcher_class):
    n = 2
    cluster = Cluster(engine_launcher_class=engine_launcher_class, n=n)
    await cluster.start_cluster()
    controller = cluster._controller
    assert controller is not None
    assert len(cluster._engine_sets) == 1

    with await cluster.connect_client() as rc:
        rc.wait_for_engines(n, timeout=_timeout)
    await cluster.stop_cluster()
    assert cluster._controller is None
    assert cluster._engine_sets == {}


@pytest.mark.parametrize("engine_launcher_class", _engine_launcher_classes)
async def test_signal_engines(request, Cluster, engine_launcher_class):
    cluster = Cluster(engine_launcher_class=engine_launcher_class)
    await cluster.start_controller()
    engine_set_id = await cluster.start_engines(n=3)
    rc = await cluster.connect_client()
    request.addfinalizer(rc.close)
    while len(rc) < 3:
        await asyncio.sleep(0.1)
    # seems to be a problem if we start too soon...
    await asyncio.sleep(1)
    # ensure responsive
    rc[:].apply_async(lambda: None).get(timeout=_timeout)
    # submit request to be interrupted
    ar = rc[:].apply_async(time.sleep, 3)
    # wait for it to be running
    await asyncio.sleep(0.5)
    # send signal
    await cluster.signal_engines(signal.SIGINT, engine_set_id)

    # wait for result, which should raise KeyboardInterrupt
    with raises_remote(KeyboardInterrupt) as e:
        ar.get(timeout=_timeout)
    rc.close()

    await cluster.stop_engines()
    await cluster.stop_controller()


@pytest.mark.parametrize("engine_launcher_class", _engine_launcher_classes)
async def test_restart_engines(Cluster, engine_launcher_class):
    n = 3
    async with Cluster(engine_launcher_class=engine_launcher_class, n=n) as rc:
        cluster = rc.cluster
        engine_set_id = next(iter(cluster._engine_sets))
        engine_set = cluster._engine_sets[engine_set_id]
        assert rc.ids == list(range(n))
        before_pids = rc[:].apply_sync(os.getpid)
        await cluster.restart_engines()
        # wait for unregister
        while any(eid in rc.ids for eid in range(n)):
            await asyncio.sleep(0.1)
        # wait for register
        rc.wait_for_engines(n, timeout=_timeout)
        after_pids = rc[:].apply_sync(os.getpid)
        assert set(after_pids).intersection(before_pids) == set()


async def test_async_with(Cluster):
    async with Cluster(n=5) as rc:
        assert sorted(rc.ids) == list(range(5))
        rc[:]['a'] = 5
        assert rc[:]['a'] == [5] * 5


def test_sync_with(Cluster):
    with Cluster(log_level=10, n=5) as rc:
        assert sorted(rc.ids) == list(range(5))
        rc[:]['a'] = 5
        assert rc[:]['a'] == [5] * 5


@pytest.mark.parametrize(
    "classname, expected_class",
    [
        ("MPI", cluster.launcher.MPIEngineSetLauncher),
        ("SGE", cluster.launcher.SGEEngineSetLauncher),
        (
            "ipyparallel.cluster.launcher.LocalEngineSetLauncher",
            cluster.launcher.LocalEngineSetLauncher,
        ),
    ],
)
def test_cluster_abbreviations(classname, expected_class):
    c = cluster.Cluster(engine_launcher_class=classname)
    assert c.engine_launcher_class is expected_class


async def test_cluster_repr(Cluster):
    c = Cluster(cluster_id="test", profile_dir='/tmp')
    assert repr(c) == "<Cluster(cluster_id='test', profile_dir='/tmp')>"
    await c.start_controller()
    assert (
        repr(c)
        == "<Cluster(cluster_id='test', profile_dir='/tmp', controller=<running>)>"
    )
    await c.start_engines(1, 'engineid')
    assert (
        repr(c)
        == "<Cluster(cluster_id='test', profile_dir='/tmp', controller=<running>, engine_sets=['engineid'])>"
    )


async def test_cluster_manager():
    m = cluster.ClusterManager()
    assert m.list_clusters() == []
    c = m.new_cluster(profile_dir="/tmp")
    assert c.profile_dir == "/tmp"
    assert m.get_cluster(c.cluster_id) is c
    with pytest.raises(KeyError):
        m.get_cluster("nosuchcluster")

    with pytest.raises(KeyError):
        m.new_cluster(cluster_id=c.cluster_id)

    assert m.list_clusters() == [c.cluster_id]
    m.remove_cluster(c.cluster_id)
    assert m.list_clusters() == []
    with pytest.raises(KeyError):
        m.remove_cluster("nosuchcluster")
