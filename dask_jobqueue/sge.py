import logging
import os
import socket
import sys

from distributed import LocalCluster
from distributed.utils import get_ip_interface

from .core import JobQueueCluster

logger = logging.getLogger(__name__)

dirname = os.path.dirname(sys.executable)


class SGECluster(JobQueueCluster):
    """ Launch Dask on a SGE cluster

    Parameters
    ----------
    name : str
        Name of worker jobs. Passed to `$SGE -N` option.
    queue : str
        Destination queue for each worker job. Passed to `#$ -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#$ -A` option.
    threads_per_worker : int
        Number of threads per process.
    processes : int
        Number of processes per node.
    memory : str
        Bytes of memory that the worker can use. This should be a string
        like "7GB" that can be interpretted both by SGE and Dask.
    resource_spec : str
        Request resources and specify job placement. Passed to `#$ -l`
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
    >>> from dask_jobqueue import SGECluster
    >>> cluster = SGECluster(project='...')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()
    """

    #Override class variables
    submit_command = 'qsub -terse'
    cancel_command = 'qdel'

    def __init__(self,
                 queue=None,
                 project=None,
                 resource_spec='h_vmem=36G',
                 walltime='0:30:0',
                 interface=None,
                 **kwargs):

        super(SGECluster, self).__init__(**kwargs)

#         self._header_template = """
# #!/bin/bash

# #$ -N %(name)s
# #$ -q %(queue)s
# #$ -P %(project)s
# #$ -l %(resource_spec)s
# #$ -l h_rt=%(walltime)s
# #$ -cwd
# #$ -j y

# """.lstrip()

        # if interface:
        #     host = get_ip_interface(interface)
        #     extra += ' --interface  %s ' % interface
        # else:
        #     host = socket.gethostname()

        project = project or os.environ.get('SGE_ACCOUNT')

        header_lines = ['#!/bin/bash']

        # XXX: change the getattr when this is fixed in master
        if getattr(self, 'name', None) is not None:
            header_lines.append('#$ -N %(name)s')
        if queue is not None:
            header_lines.append('#$ -q %(queue)s')
        if project is not None:
            header_lines.append('#$ -P %(project)s')
        if resource_spec is not None:
            # TODO clever default like in pbs???
            header_lines.append('#$ -l %(resource_spec)s')
        if walltime is not None:
            header_lines.append('#$ -l h_rt=%(walltime)s')
        header_lines.extend(['#$ -cwd', '#$ -j y'])
        self._header_template = '\n'.join(header_lines)

        # if not project:
        #     raise ValueError("Must specify a project like `project='UCLB1234' "
        #                      "or set SGE_ACCOUNT environment variable")
        # self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)
        # memory = memory.replace(' ', '')
        self.config = {'name': getattr(self, 'name', 'default-name'),
                       'queue': queue,
                       'project': project,
                       # 'threads_per_worker': threads_per_worker,
                       'processes': self.worker_processes,
                       'walltime': walltime,
                       # 'scheduler': self.scheduler.address,
                       'resource_spec': resource_spec,
                       # 'base_path': dirname,
                       # 'memory': memory,
                       # 'death_timeout': death_timeout,
                       # 'extra': extra
        }
        self.job_header = self._header_template % self.config
        # self.jobs = dict()
        # self.n = 0
        # self._adaptive = None

        logger.debug("Job script: \n %s" % self.job_script())
