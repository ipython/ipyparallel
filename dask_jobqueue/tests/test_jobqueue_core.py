from __future__ import absolute_import, division, print_function

import os
import shutil
import socket
import sys
import re

import pytest

from dask_jobqueue import (JobQueueCluster, PBSCluster, MoabCluster,
                           SLURMCluster, SGECluster, LSFCluster)


def test_errors():
    with pytest.raises(NotImplementedError) as info:
        JobQueueCluster(cores=4)

    assert 'abstract class' in str(info.value)


def test_threads_deprecation():
    with pytest.raises(ValueError) as info:
        JobQueueCluster(threads=4)

    assert all(word in str(info.value)
               for word in ['threads', 'core', 'processes'])


def test_command_template():
    with PBSCluster(cores=2, memory='4GB') as cluster:
        assert '%s -m distributed.cli.dask_worker' % (sys.executable) \
               in cluster._command_template
        assert ' --nthreads 2' in cluster._command_template
        assert ' --memory-limit ' in cluster._command_template
        assert ' --name ' in cluster._command_template

    with PBSCluster(cores=2, memory='4GB', death_timeout=60, local_directory='/scratch',
                    extra=['--preload', 'mymodule']) as cluster:
        assert ' --death-timeout 60' in cluster._command_template
        assert ' --local-directory /scratch' in cluster._command_template
        assert ' --preload mymodule' in cluster._command_template


@pytest.mark.parametrize('Cluster', [PBSCluster, MoabCluster, SLURMCluster,
                                     SGECluster, LSFCluster])
def test_repr(Cluster):
    with Cluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                 name='dask-worker') as cluster:
        cluster_repr = repr(cluster)
        assert cluster.__class__.__name__ in cluster_repr
        assert 'cores=0' in cluster_repr
        assert 'memory=0 B' in cluster_repr
        assert 'workers=0' in cluster_repr


def test_forward_ip():
    ip = '127.0.0.1'
    with PBSCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                    name='dask-worker', ip=ip) as cluster:
        assert cluster.local_cluster.scheduler.ip == ip

    default_ip = socket.gethostbyname('')
    with PBSCluster(walltime='00:02:00', processes=4, cores=8, memory='28GB',
                    name='dask-worker') as cluster:
        assert cluster.local_cluster.scheduler.ip == default_ip


@pytest.mark.parametrize('Cluster', [PBSCluster, MoabCluster, SLURMCluster,
                                     SGECluster, LSFCluster])
@pytest.mark.parametrize(
    'qsub_return_string',
    ['{job_id}.admin01',
     'Request {job_id}.asdf was sumbitted to queue: standard.',
     'sbatch: Submitted batch job {job_id}',
     '{job_id};cluster',
     'Job <{job_id}> is submitted to default queue <normal>.',
     '{job_id}'])
def test_job_id_from_qsub(Cluster, qsub_return_string):
    original_job_id = '654321'
    qsub_return_string = qsub_return_string.format(job_id=original_job_id)
    with Cluster(cores=1, memory='1GB') as cluster:
        assert (original_job_id
                == cluster._job_id_from_submit_output(qsub_return_string))


@pytest.mark.parametrize('Cluster', [PBSCluster, MoabCluster, SLURMCluster,
                                     SGECluster, LSFCluster])
def test_job_id_error_handling(Cluster):
    # non-matching regexp
    with Cluster(cores=1, memory='1GB') as cluster:
        with pytest.raises(ValueError, match="Could not parse job id"):
            return_string = "there is no number here"
            cluster._job_id_from_submit_output(return_string)

    # no job_id named group in the regexp
    with Cluster(cores=1, memory='1GB') as cluster:
        with pytest.raises(ValueError, match="You need to use a 'job_id' named group"):
            return_string = 'Job <12345> submited to <normal>.'
            cluster.job_id_regexp = r'(\d+)'
            cluster._job_id_from_submit_output(return_string)


def test_log_directory(tmpdir):
    shutil.rmtree(tmpdir.strpath, ignore_errors=True)
    with PBSCluster(cores=1, memory='1GB'):
        assert not os.path.exists(tmpdir.strpath)

    with PBSCluster(cores=1, memory='1GB',
                    log_directory=tmpdir.strpath):
        assert os.path.exists(tmpdir.strpath)


def test_jobqueue_cluster_call(tmpdir):
    cluster = PBSCluster(cores=1, memory='1GB')

    path = tmpdir.join('test.py')
    path.write('print("this is the stdout")')

    out = cluster._call([sys.executable, path.strpath])
    assert out == 'this is the stdout\n'

    path_with_error = tmpdir.join('non-zero-exit-code.py')
    path_with_error.write('print("this is the stdout")\n1/0')

    match = ('Command exited with non-zero exit code.+'
             'Exit code: 1.+'
             'stdout:\nthis is the stdout.+'
             'stderr:.+ZeroDivisionError')

    match = re.compile(match, re.DOTALL)
    with pytest.raises(RuntimeError, match=match):
        cluster._call([sys.executable, path_with_error.strpath])
