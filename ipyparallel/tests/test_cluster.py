import asyncio
import shutil
import signal
import time

import pytest

from .clienttest import raises_remote
from ipyparallel import cluster
from ipyparallel.apps.ipclusterapp import find_launcher_class


@pytest.fixture
def Cluster(request):
    """Fixture for instantiating Clusters"""

    def ClusterConstructor(**kwargs):
        kwargs.setdefault('log_level', 10)
        launcher_prefix = kwargs.pop("launcher_prefix")

        if launcher_prefix == "MPI" and shutil.which("mpiexec") is None:
            pytest.skip("requires mpiexec")
        launcher_class = find_launcher_class(launcher_prefix, "EngineSet")
        kwargs['engine_launcher_class'] = launcher_class

        c = cluster.Cluster(**kwargs)
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


async def test_start_stop_controller(Cluster):
    cluster = Cluster()
    await cluster.start_controller()
    with pytest.raises(RuntimeError):
        await cluster.start_controller()
    assert cluster._controller is not None
    proc = cluster._controller.process
    assert proc.poll() is None
    # TODO: wait for connection
    client = cluster.connect_client()
    assert client.queue_status() == {'unassigned': 0}
    await cluster.stop_controller()
    assert not proc.poll() is not None
    assert cluster._controller is None
    # stop is idempotent
    await cluster.stop_controller()
    # TODO: test file cleanup


@pytest.mark.parametrize("launcher_prefix", ["Local", "MPI"])
async def test_start_stop_engines(Cluster, launcher_prefix):
    cluster = Cluster(launcher_prefix=launcher_prefix)
    await cluster.start_controller()
    engine_set_id = await cluster.start_engines(n=3)
    assert engine_set_id in cluster._engine_sets
    engine_set = cluster._engine_sets[engine_set_id]
    launcher_class = launcher_class = find_launcher_class(launcher_prefix, "EngineSet")
    assert isinstance(engine_set, launcher_class)
    await cluster.stop_engines(engine_set_id)
    assert cluster._engine_sets == {}
    with pytest.raises(KeyError):
        await cluster.stop_engines(engine_set_id)

    await cluster.stop_controller()


@pytest.mark.parametrize("launcher_prefix", ["Local", "MPI"])
async def test_signal_engines(Cluster, launcher_prefix):
    cluster = Cluster(launcher_prefix=launcher_prefix)
    await cluster.start_controller()
    engine_set_id = await cluster.start_engines(n=3)
    rc = cluster.connect_client()
    while len(rc) < 3:
        await asyncio.sleep(0.1)
    ar = rc[:].apply_async(time.sleep, 3)
    await asyncio.sleep(0.5)
    await cluster.signal_engines(engine_set_id, signal.SIGINT)

    with raises_remote(KeyboardInterrupt) as e:
        ar.get()

    await cluster.stop_engines()
    await cluster.stop_controller()


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
