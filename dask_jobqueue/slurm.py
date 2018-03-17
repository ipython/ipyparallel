import logging
import os
import sys

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

    #Override class variables
    submit_command = 'sbatch'
    cancel_command = 'scancel'

    def __init__(self,
                 name='dask',
                 queue='',
                 project=None,
                 processes=8,
                 memory='7GB',
                 walltime='00:30:00',
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
        processes : int
            Number of processes per node.
        memory : str
            Bytes of memory that the worker can use. This should be a string
            like "7GB" that can be interpretted both by PBS and Dask.
        walltime : str
            Walltime for each worker job.
        kwargs : dict
            Additional keyword arguments to pass to `JobQueueCluster` and `LocalCluster`
        """

        super(SLURMCluster, self).__init__(name=name, processes=processes, **kwargs)

        #TODO has this been tested? This seems weird to use only processes, and not processes * threads
        # There are no memory limit given to Slurm either?

        #Keeping template for now has I don't know much about slurm.
        self._header_template = """
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
""".lstrip()

        memory = memory.replace(' ', '')
        self.config = {'name': name,
                       'queue': queue,
                       'project': project,
                       'processes': processes,
                       'walltime': walltime,
                       # Not used
                       'memory': memory
                       }

        self.job_header = self._header_template % self.config

        logger.debug("Job script: \n %s" % self.job_script())

