from time import sleep, time

import pytest
import sys
from distributed import Client
from distributed.utils_test import loop  # noqa: F401

from dask_jobqueue import SLURMCluster


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

    with SLURMCluster(queue='regular', project='DaskOnPBS', processes=4,
                      cores=8, memory='28GB', job_cpu=16, job_mem='100G') as cluster:

        assert '#SBATCH --cpus-per-task=16' in cluster.job_header
        assert '#SBATCH --cpus-per-task=8' not in cluster.job_header
        assert '#SBATCH --mem=100G' in cluster.job_header
        assert '#SBATCH -t ' in cluster.job_header
        assert '#SBATCH -A DaskOnPBS' in cluster.job_header
        assert '#SBATCH -p regular' in cluster.job_header

    with SLURMCluster(cores=4, memory='8GB') as cluster:

        assert '#SBATCH' in cluster.job_header
        assert '#SBATCH -J ' in cluster.job_header
        assert '#SBATCH -n 1' in cluster.job_header
        # assert '#SBATCH --cpus-per-task=' in cluster.job_header
        # assert '#SBATCH --mem=' in cluster.job_header
        assert '#SBATCH -t ' in cluster.job_header
        assert '#SBATCH -p' not in cluster.job_header
        assert '#SBATCH -A' not in cluster.job_header


def test_job_script():
    with SLURMCluster(walltime='00:02:00', processes=4, cores=8,
                      memory='28GB') as cluster:

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
                      env_extra=['export LANG="en_US.utf8"', 'export LANGUAGE="en_US.utf8"',
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
    with SLURMCluster(walltime='00:02:00', cores=2, processes=1, memory='4GB',
                      job_extra=['-D /'], loop=loop) as cluster:
        with Client(cluster) as client:
            workers = cluster.start_workers(2)
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(60) == 11
            assert cluster.jobs

            info = client.scheduler_info()
            w = list(info['workers'].values())[0]
            assert w['memory_limit'] == 4e9
            assert w['ncores'] == 2

            cluster.stop_workers(workers)

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + 10

            assert not cluster.jobs


@pytest.mark.env("slurm")  # noqa: F811
def test_adaptive(loop):
    with SLURMCluster(walltime='00:02:00', cores=2, processes=1, memory='4GB',
                      job_extra=['-D /'], loop=loop) as cluster:
        cluster.adapt()
        with Client(cluster) as client:
            future = client.submit(lambda x: x + 1, 10)
            assert future.result(60) == 11

            assert cluster.jobs

            start = time()
            processes = cluster.worker_processes
            while (len(client.scheduler_info()['workers']) != processes):
                sleep(0.1)
                assert time() < start + 10

            del future

            start = time()
            while len(client.scheduler_info()['workers']) > 0:
                sleep(0.100)
                assert time() < start + 10

            # There is probably a bug to fix in the adaptive methods of the JobQueueCluster
            # Currently cluster.jobs is not cleaned up.
            # start = time()
            # while cluster.jobs:
            #     sleep(0.100)
            #     assert time() < start + 10
