import os
from shutil import rmtree
import sys
from textwrap import dedent
import tempfile
from time import sleep, time

import dask
import pytest
from dask.distributed import Client
from distributed.utils import parse_bytes

from dask_jobqueue import LSFCluster, lsf

from . import QUEUE_WAIT


def test_header():
    with LSFCluster(walltime="00:02", processes=4, cores=8, memory="8GB") as cluster:

        assert "#BSUB" in cluster.job_header
        assert "#BSUB -J dask-worker" in cluster.job_header
        assert "#BSUB -n 8" in cluster.job_header
        assert "#BSUB -M 8000" in cluster.job_header
        assert "#BSUB -W 00:02" in cluster.job_header
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header
        assert "--name dask-worker--${JOB_ID}--" in cluster.job_script()

    with LSFCluster(
        queue="general",
        project="DaskOnLSF",
        processes=4,
        cores=8,
        memory="28GB",
        ncpus=24,
        mem=100000000000,
    ) as cluster:

        assert "#BSUB -q general" in cluster.job_header
        assert "#BSUB -J dask-worker" in cluster.job_header
        assert "#BSUB -n 24" in cluster.job_header
        assert "#BSUB -n 8" not in cluster.job_header
        assert "#BSUB -M 100000" in cluster.job_header
        assert "#BSUB -M 28000" not in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -P DaskOnLSF" in cluster.job_header

    with LSFCluster(cores=4, memory="8GB") as cluster:

        assert "#BSUB -n" in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -M" in cluster.job_header
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header

    with LSFCluster(
        cores=4, memory="8GB", job_extra=["-u email@domain.com"]
    ) as cluster:

        assert "#BSUB -u email@domain.com" in cluster.job_header
        assert "#BSUB -n" in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -M" in cluster.job_header
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header


def test_job_script():
    with LSFCluster(walltime="00:02", processes=4, cores=8, memory="28GB") as cluster:

        job_script = cluster.job_script()
        assert "#BSUB" in job_script
        assert "#BSUB -J dask-worker" in job_script
        assert "#BSUB -n 8" in job_script
        assert "#BSUB -M 28000" in job_script
        assert "#BSUB -W 00:02" in job_script
        assert "#BSUB -q" not in cluster.job_header
        assert "#BSUB -P" not in cluster.job_header

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        assert "--nthreads 2 --nprocs 4 --memory-limit 7.00GB" in job_script

    with LSFCluster(
        queue="general",
        project="DaskOnLSF",
        processes=4,
        cores=8,
        memory="28GB",
        ncpus=24,
        mem=100000000000,
    ) as cluster:

        job_script = cluster.job_script()
        assert "#BSUB -q general" in cluster.job_header
        assert "#BSUB -J dask-worker" in cluster.job_header
        assert "#BSUB -n 24" in cluster.job_header
        assert "#BSUB -n 8" not in cluster.job_header
        assert "#BSUB -M 100000" in cluster.job_header
        assert "#BSUB -M 28000" not in cluster.job_header
        assert "#BSUB -W" in cluster.job_header
        assert "#BSUB -P DaskOnLSF" in cluster.job_header

        assert (
            "{} -m distributed.cli.dask_worker tcp://".format(sys.executable)
            in job_script
        )
        assert "--nthreads 2 --nprocs 4 --memory-limit 7.00GB" in job_script


@pytest.mark.env("lsf")
def test_basic(loop):
    with LSFCluster(
        walltime="00:02",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        loop=loop,
    ) as cluster:
        with Client(cluster) as client:
            cluster.start_workers(2)
            assert cluster.pending_jobs or cluster.running_jobs
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11
            assert cluster.running_jobs

            workers = list(client.scheduler_info()["workers"].values())
            w = workers[0]
            assert w["memory_limit"] == 2e9
            assert w["nthreads"] == 2

            cluster.stop_workers(workers)

            start = time()
            while client.scheduler_info()["workers"]:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            assert not cluster.running_jobs


@pytest.mark.env("lsf")
def test_adaptive(loop):
    with LSFCluster(
        walltime="00:02",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        loop=loop,
    ) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)

            start = time()
            while not (cluster.pending_jobs or cluster.running_jobs):
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            assert future.result(QUEUE_WAIT) == 11

            start = time()
            processes = cluster.worker_processes
            while len(client.scheduler_info()["workers"]) != processes:
                sleep(0.1)
                assert time() < start + QUEUE_WAIT

            del future

            start = time()
            while len(client.scheduler_info()["workers"]) > 0:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            start = time()
            while cluster.pending_jobs or cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT
            assert cluster.finished_jobs


@pytest.mark.env("lsf")
def test_adaptive_grouped(loop):
    with LSFCluster(
        walltime="00:02",
        processes=1,
        cores=2,
        memory="2GB",
        local_directory="/tmp",
        loop=loop,
    ) as cluster:
        cluster.adapt(minimum=1)  # at least 1 worker
        with Client(cluster) as client:
            start = time()
            while not (cluster.pending_jobs or cluster.running_jobs):
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            future = client.submit(lambda x: x + 1, 10)
            assert future.result(QUEUE_WAIT) == 11

            start = time()
            while not cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            start = time()
            processes = cluster.worker_processes
            while len(client.scheduler_info()["workers"]) != processes:
                sleep(0.1)
                assert time() < start + QUEUE_WAIT


def test_config(loop):
    with dask.config.set(
        {"jobqueue.lsf.walltime": "00:02", "jobqueue.lsf.local-directory": "/foo"}
    ):
        with LSFCluster(loop=loop, cores=1, memory="2GB") as cluster:
            assert "00:02" in cluster.job_script()
            assert "--local-directory /foo" in cluster.job_script()


def test_config_name_lsf_takes_custom_config():
    conf = {
        "queue": "myqueue",
        "project": "myproject",
        "ncpus": 1,
        "cores": 1,
        "mem": 2,
        "memory": "2 GB",
        "walltime": "00:02",
        "job-extra": [],
        "lsf-units": "TB",
        "name": "myname",
        "processes": 1,
        "interface": None,
        "death-timeout": None,
        "local-directory": "/foo",
        "extra": [],
        "env-extra": [],
        "log-directory": None,
        "shebang": "#!/usr/bin/env bash",
    }

    with dask.config.set({"jobqueue.lsf-config-name": conf}):
        with LSFCluster(config_name="lsf-config-name") as cluster:
            assert cluster.name == "myname"


def test_informative_errors():
    with pytest.raises(ValueError) as info:
        LSFCluster(memory=None, cores=4)
    assert "memory" in str(info.value)

    with pytest.raises(ValueError) as info:
        LSFCluster(memory="1GB", cores=None)
    assert "cores" in str(info.value)


def lsf_unit_detection_helper(expected_unit, conf_text=None):
    temp_dir = tempfile.mkdtemp()
    current_lsf_envdir = os.environ.get("LSF_ENVDIR", None)
    os.environ["LSF_ENVDIR"] = temp_dir
    if conf_text is not None:
        with open(os.path.join(temp_dir, "lsf.conf"), "w") as conf_file:
            conf_file.write(conf_text)
    memory_string = "13GB"
    memory_base = parse_bytes(memory_string)
    correct_memory = lsf.lsf_format_bytes_ceil(memory_base, lsf_units=expected_unit)
    with LSFCluster(memory=memory_string, cores=1) as cluster:
        assert "#BSUB -M %s" % correct_memory in cluster.job_header
    rmtree(temp_dir)
    if current_lsf_envdir is None:
        del os.environ["LSF_ENVDIR"]
    else:
        os.environ["LSF_ENVDIR"] = current_lsf_envdir


@pytest.mark.parametrize(
    "lsf_units_string,expected_unit",
    [
        ("LSF_UNIT_FOR_LIMITS=MB", "mb"),
        ("LSF_UNIT_FOR_LIMITS=G  # And a comment", "gb"),
        ("#LSF_UNIT_FOR_LIMITS=NotDetected", "kb"),
    ],
)
def test_lsf_unit_detection(lsf_units_string, expected_unit):
    conf_text = dedent(
        """
        LSB_JOB_MEMLIMIT=Y
        LSB_MOD_ALL_JOBS=N
        LSF_PIM_SLEEPTIME_UPDATE=Y
        LSF_PIM_LINUX_ENHANCE=Y
        %s
        LSB_DISABLE_LIMLOCK_EXCL=Y
        LSB_SUBK_SHOW_EXEC_HOST=Y
        """
        % lsf_units_string
    )
    lsf_unit_detection_helper(expected_unit, conf_text)


def test_lsf_unit_detection_without_file():
    lsf_unit_detection_helper("kb", conf_text=None)
