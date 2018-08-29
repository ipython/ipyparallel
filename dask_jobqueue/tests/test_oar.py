from __future__ import absolute_import, division, print_function

import sys

from dask_jobqueue import OARCluster


def test_header():
    with OARCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB') as cluster:
        assert '#OAR -n dask-worker' in cluster.job_header
        assert '#OAR -l /nodes=1/core=8,walltime=00:02:00' in cluster.job_header
        assert '#OAR --project' not in cluster.job_header
        assert '#OAR -q' not in cluster.job_header

    with OARCluster(queue='regular', project='DaskOnOar', processes=4, cores=8, memory='28GB',
                    job_cpu=16, job_mem='100G', job_extra=['-t besteffort']) as cluster:
        assert 'walltime=' in cluster.job_header
        assert '#OAR --project DaskOnOar' in cluster.job_header
        assert '#OAR -q regular' in cluster.job_header
        assert '#OAR -t besteffort' in cluster.job_header

    with OARCluster(cores=4, memory='8GB') as cluster:
        assert '#OAR -n dask-worker' in cluster.job_header
        assert 'walltime=' in cluster.job_header
        assert '#OAR --project' not in cluster.job_header
        assert '#OAR -q' not in cluster.job_header


def test_job_script():
    with OARCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB') as cluster:
        job_script = cluster.job_script()
        assert '#OAR' in job_script
        assert '#OAR -n dask-worker' in job_script
        assert '--memory-limit 7.00GB ' in job_script
        assert '#OAR -l /nodes=1/core=8,walltime=00:02:00' in job_script
        assert '#OAR --project' not in job_script
        assert '#OAR -q' not in job_script

        assert 'export ' not in job_script

        assert '{} -m distributed.cli.dask_worker tcp://'.format(sys.executable) in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7.00GB' in job_script

    with OARCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                    env_extra=['export LANG="en_US.utf8"',
                               'export LANGUAGE="en_US.utf8"',
                               'export LC_ALL="en_US.utf8"']) as cluster:
        job_script = cluster.job_script()
        assert '#OAR' in job_script
        assert '#OAR -n dask-worker' in job_script
        assert '--memory-limit 7.00GB ' in job_script
        assert '#OAR -l /nodes=1/core=8,walltime=00:02:00' in job_script
        assert '#OAR --project' not in job_script
        assert '#OAR -q' not in job_script

        assert 'export LANG="en_US.utf8"' in job_script
        assert 'export LANGUAGE="en_US.utf8"' in job_script
        assert 'export LC_ALL="en_US.utf8"' in job_script

        assert '{} -m distributed.cli.dask_worker tcp://'.format(sys.executable) in job_script
        assert '--nthreads 2 --nprocs 4 --memory-limit 7.00GB' in job_script
