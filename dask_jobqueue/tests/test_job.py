import asyncio
from time import time

from dask_jobqueue import (
    PBSCluster,
    SGECluster,
    SLURMCluster,
    LSFCluster,
    HTCondorCluster,
    MoabCluster,
    OARCluster,
)
from dask_jobqueue.local import LocalJob, LocalCluster
from dask_jobqueue.pbs import PBSJob
from dask_jobqueue.sge import SGEJob
from dask_jobqueue.slurm import SLURMJob
from dask_jobqueue.lsf import LSFJob
from dask_jobqueue.moab import MoabJob
from dask_jobqueue.htcondor import HTCondorJob
from dask_jobqueue.oar import OARJob

from dask_jobqueue.core import JobQueueCluster
from dask.distributed import Scheduler, Client

import pytest


def test_basic():
    job = PBSJob(scheduler="127.0.0.1:12345", cores=1, memory="1 GB")
    assert "127.0.0.1:12345" in job.job_script()


job_protected = [
    pytest.param(SGEJob, marks=[pytest.mark.env("sge")]),
    pytest.param(PBSJob, marks=[pytest.mark.env("pbs")]),
    pytest.param(SLURMJob, marks=[pytest.mark.env("slurm")]),
    pytest.param(LSFJob, marks=[pytest.mark.env("lsf")]),
    LocalJob,
]


all_jobs = [SGEJob, PBSJob, SLURMJob, LSFJob, HTCondorJob, MoabJob, OARJob]
all_clusters = [
    SGECluster,
    PBSCluster,
    SLURMCluster,
    LSFCluster,
    HTCondorCluster,
    MoabCluster,
    OARCluster,
    HTCondorCluster,
]


@pytest.mark.parametrize("job_cls", job_protected)
@pytest.mark.asyncio
async def test_job(job_cls):
    async with Scheduler(port=0) as s:
        job = job_cls(scheduler=s.address, name="foo", cores=1, memory="1GB")
        job = await job
        async with Client(s.address, asynchronous=True) as client:
            await client.wait_for_workers(1)
            assert list(s.workers.values())[0].name == "foo"

        await job.close()

        start = time()
        while len(s.workers):
            await asyncio.sleep(0.1)
            assert time() < start + 10


@pytest.mark.parametrize("job_cls", job_protected)
@pytest.mark.asyncio
async def test_cluster(job_cls):
    async with JobQueueCluster(
        1, cores=1, memory="1GB", job_cls=job_cls, asynchronous=True, name="foo"
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            assert len(cluster.workers) == 1
            cluster.scale(jobs=2)
            await cluster
            assert len(cluster.workers) == 2
            assert all(isinstance(w, job_cls) for w in cluster.workers.values())
            assert all(w.status == "running" for w in cluster.workers.values())
            await client.wait_for_workers(2)

            cluster.scale(1)
            start = time()
            await cluster
            while len(cluster.scheduler.workers) > 1:
                await asyncio.sleep(0.1)
                assert time() < start + 10


@pytest.mark.parametrize("job_cls", job_protected)
@pytest.mark.asyncio
async def test_adapt(job_cls):
    async with JobQueueCluster(
        1, cores=1, memory="1GB", job_cls=job_cls, asynchronous=True, name="foo"
    ) as cluster:
        async with Client(cluster, asynchronous=True) as client:
            await client.wait_for_workers(1)
            cluster.adapt(minimum=0, maximum=4, interval="10ms")

            start = time()
            while len(cluster.scheduler.workers) or cluster.workers:
                await asyncio.sleep(0.050)
                assert time() < start + 10
            assert not cluster.worker_spec
            assert not cluster.workers

            future = client.submit(lambda: 0)
            await client.wait_for_workers(1)

            del future

            start = time()
            while len(cluster.scheduler.workers) or cluster.workers:
                await asyncio.sleep(0.050)
                assert time() < start + 10
            assert not cluster.worker_spec
            assert not cluster.workers


@pytest.mark.parametrize("job_cls", job_protected)
@pytest.mark.asyncio
async def test_adapt_parameters(job_cls):
    async with JobQueueCluster(
        cores=2, memory="1GB", processes=2, job_cls=job_cls, asynchronous=True
    ) as cluster:
        adapt = cluster.adapt(minimum=2, maximum=4, interval="10ms")
        await adapt.adapt()
        await cluster
        assert len(cluster.workers) == 1  # 2 workers, 4 jobs

        adapt = cluster.adapt(minimum_jobs=2, maximum_jobs=4, interval="10ms")
        await adapt.adapt()
        await cluster
        assert len(cluster.workers) == 2  # 2 workers, 4 jobs


def test_header_lines_skip():
    job = PBSJob(cores=1, memory="1GB", job_name="foobar")
    assert "foobar" in job.job_script()

    job = PBSJob(cores=1, memory="1GB", job_name="foobar", header_skip=["-N"])
    assert "foobar" not in job.job_script()


@pytest.mark.asyncio
async def test_nprocs_scale():
    async with LocalCluster(
        cores=2, memory="4GB", processes=2, asynchronous=True
    ) as cluster:
        s = cluster.scheduler
        async with Client(cluster, asynchronous=True) as client:
            cluster.scale(cores=2)
            await cluster
            await client.wait_for_workers(2)
            assert len(cluster.workers) == 1  # two workers, one job
            assert len(s.workers) == 2
            assert cluster.plan == {ws.name for ws in s.workers.values()}

            cluster.scale(cores=1)
            await cluster
            await asyncio.sleep(0.2)
            assert len(cluster.scheduler.workers) == 2  # they're still one group

            cluster.scale(jobs=2)
            assert len(cluster.worker_spec) == 2
            cluster.scale(5)
            assert len(cluster.worker_spec) == 3
            cluster.scale(1)
            assert len(cluster.worker_spec) == 1


@pytest.mark.parametrize("Cluster", all_clusters)
def test_docstring_cluster(Cluster):
    assert "cores :" in Cluster.__doc__
    assert Cluster.__name__[: -len("Cluster")] in Cluster.__doc__
