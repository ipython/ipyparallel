from __future__ import absolute_import, division, print_function

import sys
from time import sleep, time

import pytest
from distributed import Client
from distributed.utils_test import loop  # noqa: F401

from dask_jobqueue import SLURMCluster

from . import QUEUE_WAIT


def test_header():
    with SLURMCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB') as cluster:

        assert '#SBATCH' in cluster.job_header
        assert '#SBATCH -J dask-worker' in cluster.job_header
        assert '#SBATCH -n 1' in cluster.job_header
        assert '#SBATCH --cpus-per-task=8' in cluster.job_header
        assert '#SBATCH --mem=27G' in cluster.job_header
        assert '#SBATCH -t 00:02:00' in cluster.job_header
        assert '#SBATCH -p' not in cluster.job_header
        assert '#SBATCH -A' not in cluster.job_header

    with SLURMCluster(queue='regular', project='DaskOnSlurm', processes=4, cores=8, memory='28GB',
                      job_cpu=16, job_mem='100G') as cluster:

        assert '#SBATCH --cpus-per-task=16' in cluster.job_header
        assert '#SBATCH --cpus-per-task=8' not in cluster.job_header
        assert '#SBATCH --mem=100G' in cluster.job_header
        assert '#SBATCH -t ' in cluster.job_header
        assert '#SBATCH -A DaskOnSlurm' in cluster.job_header
        assert '#SBATCH -p regular' in cluster.job_header

    with SLURMCluster(cores=4, memory='8GB') as cluster:

        assert '#SBATCH' in cluster.job_header
        assert '#SBATCH -J ' in cluster.job_header
        assert '#SBATCH -n 1' in cluster.job_header
        assert '#SBATCH -t ' in cluster.job_header
        assert '#SBATCH -p' not in cluster.job_header
        assert '#SBATCH -A' not in cluster.job_header


def test_job_script():
    with SLURMCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB') as cluster:

        job_script = cluster.job_script()
        assert '#SBATCH' in job_script
        assert '#SBATCH -J dask-worker' in job_script
        assert '--memory-limit 7.00GB ' in job_script
        assert '#SBATCH -n 1' in job_script
        assert '#SBATCH --cpus-per-task=8' in job_script
        assert '#SBATCH --mem=27G' in job_script
        assert '#SBATCH -t 00:02:00' in job_script
        assert '#SBATCH -p' not in job_script
        assert '#SBATCH -A' not in job_script

        assert 'export ' not in job_script

        assert '{} -m distributed.cli.dask_worker tcp://'.format(sys.executable) in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7.00GB' in job_script

    with SLURMCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                      env_extra=['export LANG="en_US.utf8"',
                                 'export LANGUAGE="en_US.utf8"',
                                 'export LC_ALL="en_US.utf8"']
                      ) as cluster:
        job_script = cluster.job_script()
        assert '#SBATCH' in job_script
        assert '#SBATCH -J dask-worker' in job_script
        assert '#SBATCH -n 1' in job_script
        assert '#SBATCH --cpus-per-task=8' in job_script
        assert '#SBATCH --mem=27G' in job_script
        assert '#SBATCH -t 00:02:00' in job_script
        assert '#SBATCH -p' not in job_script
        assert '#SBATCH -A' not in job_script

        assert 'export LANG="en_US.utf8"' in job_script
        assert 'export LANGUAGE="en_US.utf8"' in job_script
        assert 'export LC_ALL="en_US.utf8"' in job_script

        assert '{} -m distributed.cli.dask_worker tcp://'.format(sys.executable) in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7.00GB' in job_script


@pytest.mark.env("slurm")  # noqa: F811
def test_basic(loop):
    with SLURMCluster(walltime='00:02:00', cores=2, processes=1, memory='2GB',
                      job_extra=['-D /'], loop=loop) as cluster:
        with Client(cluster) as client:

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
            assert w['memory_limit'] == 2e9
            assert w['ncores'] == 2

            cluster.scale(0)

            start = time()
            while cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT


@pytest.mark.env("slurm")  # noqa: F811
def test_adaptive(loop):
    with SLURMCluster(walltime='00:02:00', cores=2, processes=1, memory='2GB',
                      job_extra=['-D /'], loop=loop) as cluster:
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
            while len(client.scheduler_info()['workers']) != processes:
                sleep(0.1)
                assert time() < start + QUEUE_WAIT

            del future

            start = time()
            while cluster.running_jobs:
                sleep(0.100)
                assert time() < start + QUEUE_WAIT

            assert cluster.finished_jobs
