import logging
import os

from .core import JobQueueCluster

logger = logging.getLogger(__name__)


class PBSCluster(JobQueueCluster):
    """ Launch Dask on a PBS cluster

    Parameters
    ----------
    name : str
        Name of worker jobs and Dask workers. Passed to `$PBS -N` option.
    queue : str
        Destination queue for each worker job. Passed to `#PBS -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#PBS -A` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#PBS -l`
        option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other PBS options, for example -j oe. Passed with '#PBS ' prefix
    local_directory : str
        Dask worker local directory for file spilling.
    kwargs : dict
        Additional keyword arguments to pass to `JobQueueCluster` and `LocalCluster`

    Inherited parameters
    --------------------
    threads : int
        Number of threads per process.
    processes : int
        Number of processes per node.
    memory : str
        Bytes of memory that the worker can use. This should be a string
        like "7GB" that can be interpretted both by PBS and Dask.
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
    >>> cluster = PBSCluster(queue= 'regular', project='DaskOnPBS')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()
    """

    #Override class variables
    submit_command = 'qsub'
    cancel_command = 'qdel'

    def __init__(self,
                 name='dask-worker',
                 queue=None,
                 project=None,
                 resource_spec='select=1:ncpus=24:mem=100GB',
                 walltime='00:30:00',
                 job_extra=[],
                 local_directory='$TMPDIR',
                 **kwargs):

        #Instantiate args and parameters from parent abstract class
        super(PBSCluster, self).__init__(name=name, local_directory=local_directory, **kwargs)

        #Try to find a project name from environment variable
        project = project or os.environ.get('PBS_ACCOUNT')

        #PBS header build
        header_lines = ['#PBS -N %s' % name]
        if queue is not None:
            header_lines.append('#PBS -q %s' % queue)
        if project is not None:
            header_lines.append('#PBS -A %s' % project)
        if resource_spec is not None:
            header_lines.append('#PBS -l %s' % resource_spec)
        if walltime is not None:
            header_lines.append('#PBS -l walltime=%s' % walltime)
        header_lines.extend(['#PBS %s' % arg for arg in job_extra])

        self._job_header = '\n'.join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())

    @property
    def job_header(self):
        return self._job_header
