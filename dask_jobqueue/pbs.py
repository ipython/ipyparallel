import logging
import os
import sys

from distributed import LocalCluster
from distributed.utils import get_ip_interface

from .core import JobQueueCluster

logger = logging.getLogger(__name__)

dirname = os.path.dirname(sys.executable)


class PBSCluster(JobQueueCluster):
    """ Launch Dask on a PBS cluster

    Parameters
    ----------
    job_name : str
        Name of worker jobs. Passed to `$PBS -N` option.
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#PBS -A` option.
    threads_per_worker : int
        Number of threads per process.
    processes : int
        Number of processes per node.
    memory : str
        Bytes of memory that the worker can use. This should be a string
        like "7GB" that can be interpretted both by PBS and Dask.
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l`
        option.
    walltime : str
        Walltime for each worker job.
    interface : str
        Network interface like 'eth0' or 'ib0'.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    job_extra : list
        Additional lines to put in PBS script header (usually starting with #PBS comment)
    worker_extra : str
        Additional arguments to pass to `dask-worker`
    kwargs : dict
        Additional keyword arguments to pass to `LocalCluster`

    Examples
    --------
    >>> from dask_jobqueue import PBSCluster
    >>> cluster = PBSCluster(project='...')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()
    """
    def __init__(self,
                 name='dask-worker',
                 queue=None,
                 project=None,
                 resource_spec='select=1:ncpus=24:mem=120GB',
                 walltime='00:30:00',
                 pbs_extra=[],
                 threads_per_worker=4,
                 processes=6,
                 memory='20GB',
                 interface=None,
                 death_timeout=60,
                 worker_extra='',
                 **kwargs):
        self._template = """
#!/bin/bash

#PBS -N %(name)s
#PBS -q %(queue)s
#PBS -A %(project)s
#PBS -l %(resource_spec)s
#PBS -l walltime=%(walltime)s
#PBS -j oe

%(base_path)s/dask-worker %(scheduler)s \
    --nthreads %(threads_per_worker)d \
    --nprocs %(processes)s \
    --memory-limit %(memory)s \
    --name %(name)s-%(n)d \
    --death-timeout %(death_timeout)s \
     %(extra)s
""".lstrip()

        project = project or os.environ.get('PBS_ACCOUNT')

        super(PBSCluster, self).__init__(threads_per_worker=threads_per_worker,processes=processes,
                 memory=memory,interface=interface,death_timeout=death_timeout,worker_extra=worker_extra,
                 **kwargs)

        self.config = {'name': name,
                       'queue': queue,
                       'project': project,
                       'threads_per_worker': threads_per_worker,
                       'processes': processes,
                       'walltime': walltime,
                       'scheduler': self.scheduler.address,
                       'resource_spec': resource_spec,
                       'base_path': dirname,
                       'memory': memory,
                       'death_timeout': death_timeout,
                       'extra': worker_extra}

        self._submitcmd = 'qsub'
        self._cancelcmd = 'qdel'

        logger.debug("Job script: \n %s" % self.job_script())
