import logging
import os
import socket
import sys

from distributed import LocalCluster
from distributed.utils import get_ip_interface

from .core import JobQueueCluster

logger = logging.getLogger(__name__)

dirname = os.path.dirname(sys.executable)


class SLURMCluster(JobQueueCluster):
    """ Launch Dask on a SLURM cluster

    Examples
    --------
    >>> from pangeo import SLURMCluster
    >>> cluster = SLURMCluster(project='...')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()
    """
    def __init__(self,
                 name='dask',
                 queue='',
                 project=None,
                 threads_per_worker=4,
                 processes=8,
                 memory='7GB',
                 walltime='00:30:00',
                 interface=None,
                 death_timeout=60,
                 extra='',
                 **kwargs):
        """ Initialize a SLURM Cluster

        Parameters
        ----------
        name : str
            Name of worker jobs. Passed to `#SBATCH -J` option.
        queue : str
            Destination queue for each worker job.
            Passed to `#SBATCH -p` option.
        project : str
            Accounting string associated with each worker job. Passed to
            `#SBATCH -A` option.
        threads_per_worker : int
            Number of threads per process.
        processes : int
            Number of processes per node.
        memory : str
            Bytes of memory that the worker can use. This should be a string
            like "7GB" that can be interpretted both by PBS and Dask.
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
        """
        self._template = """
#!/bin/bash

#SBATCH -J %(name)s
#SBATCH -n %(processes)d
#SBATCH -p %(queue)s
#SBATCH -A %(project)s
#SBATCH -t %(walltime)s
#SBATCH -e %(name)s.err
#SBATCH -o %(name)s.out

export LANG="en_US.utf8"
export LANGUAGE="en_US.utf8"
export LC_ALL="en_US.utf8"

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

        project = project or os.environ.get('SLURM_ACCOUNT')
        if not project:
            raise ValueError("Must specify a project like `project='UCLB1234' "
                             "or set SLURM_ACCOUNT environment variable")
        self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)
        memory = memory.replace(' ', '')
        self.config = {'name': name,
                       'queue': queue,
                       'project': project,
                       'threads_per_worker': threads_per_worker,
                       'processes': processes,
                       'scheduler': self.scheduler.address,
                       'walltime': walltime,
                       'base_path': dirname,
                       'memory': memory,
                       'death_timeout': death_timeout,
                       'extra': extra}
        self.jobs = dict()
        self.n = 0
        self._adaptive = None
        self._submitcmd = 'sbatch'
        self._cancelcmd = 'scancel'

        logger.debug("Job script: \n %s" % self.job_script())
