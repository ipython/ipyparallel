import logging
import os
import socket
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
    name : str
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
    extra : str
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
                 name='dask',
                 queue='regular',
                 project=None,
                 resource_spec='select=1:ncpus=36:mem=109GB',
                 threads_per_worker=4,
                 processes=9,
                 memory='7GB',
                 walltime='00:30:00',
                 interface=None,
                 death_timeout=60,
                 extra='',
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

        if interface:
            host = get_ip_interface(interface)
            extra += ' --interface  %s ' % interface
        else:
            host = socket.gethostname()

        project = project or os.environ.get('PBS_ACCOUNT')
        if not project:
            raise ValueError("Must specify a project like `project='UCLB1234' "
                             "or set PBS_ACCOUNT environment variable")
        self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)
        memory = memory.replace(' ', '')
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
                       'extra': extra}
        self.jobs = dict()
        self.n = 0
        self._adaptive = None
        self._submitcmd = 'qsub'
        self._cancelcmd = 'qdel'

        logger.debug("Job script: \n %s" % self.job_script())
