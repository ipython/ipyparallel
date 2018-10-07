from __future__ import absolute_import, division, print_function

from time import sleep, time

import pytest
from distributed import Client
from distributed.utils_test import loop  # noqa: F401

from dask_jobqueue import SGECluster

from . import QUEUE_WAIT


@pytest.mark.env("sge")  # noqa: F811
def test_basic(loop):  # noqa: F811
    with SGECluster(walltime='00:02:00', cores=8, processes=4, memory='2GB', loop=loop) as cluster:
        print(cluster.job_script())
        with Client(cluster, loop=loop) as client:

            cluster.scale(2)

            start = time()
            while not(cluster.pending_jobs or cluster.running_jobs):
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert cluster.running_jobs

            workers = list(client.scheduler_info()['workers'].values())
            w = workers[0]
            assert w['memory_limit'] == 2e9 / 4
            assert w['ncores'] == 2

            cluster.scale(0)

            start = time()
            while cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT
