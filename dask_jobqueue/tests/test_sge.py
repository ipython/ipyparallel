from __future__ import absolute_import, division, print_function

from time import sleep, time

import pytest
from distributed import Client
from distributed.utils_test import loop  # noqa: F401

from dask_jobqueue import SGECluster
import dask

from . import QUEUE_WAIT


@pytest.mark.env("sge")  # noqa: F811
def test_basic(loop):  # noqa: F811
    with SGECluster(
        walltime="00:02:00", cores=8, processes=4, memory="2GB", loop=loop
    ) as cluster:
        print(cluster.job_script())
        with Client(cluster, loop=loop) as client:

            cluster.scale(2)

            start = time()
            while not (cluster.pending_jobs or cluster.running_jobs):
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert cluster.running_jobs

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2e9 / 4
            assert w["ncores"] == 2

            cluster.scale(0)

            start = time()
            while cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


def test_config_name_sge_takes_custom_config():
    conf = {
        "queue": "myqueue",
        "project": "myproject",
        "ncpus": 1,
        "cores": 1,
        "memory": "2 GB",
        "walltime": "00:02",
        "job-extra": [],
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "local-directory": "/foo",
        "extra": [],
        "env-extra": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
        "job-cpu": None,
        "job-mem": None,
        "resource-spec": None,
    }

    with dask.config.set({"jobqueue.sge-config-name": conf}):
        with SGECluster(config_name="sge-config-name") as cluster:
            assert cluster.name == "myname"
