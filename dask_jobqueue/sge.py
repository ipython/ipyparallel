import logging

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


@docstrings.with_indent(4)
class SGECluster(JobQueueCluster):
    """ Launch Dask on a SGE cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#$ -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#$ -A` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#$ -l`
        option.
    walltime : str
        Walltime for each worker job.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import SGECluster
    >>> cluster = SGECluster(queue='regular')
    >>> cluster.start_workers(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()
    """

    # Override class variables
    submit_command = 'qsub -terse'
    cancel_command = 'qdel'

    def __init__(self,
                 queue=None,
                 project=None,
                 resource_spec=None,
                 walltime='0:30:00',
                 **kwargs):

        super(SGECluster, self).__init__(**kwargs)

        header_lines = ['#!/bin/bash']

        if self.name is not None:
            header_lines.append('#$ -N %(name)s')
        if queue is not None:
            header_lines.append('#$ -q %(queue)s')
        if project is not None:
            header_lines.append('#$ -P %(project)s')
        if resource_spec is not None:
            header_lines.append('#$ -l %(resource_spec)s')
        if walltime is not None:
            header_lines.append('#$ -l h_rt=%(walltime)s')
        header_lines.extend(['#$ -cwd', '#$ -j y'])
        header_template = '\n'.join(header_lines)

        config = {'name': self.name,
                  'queue': queue,
                  'project': project,
                  'processes': self.worker_processes,
                  'walltime': walltime,
                  'resource_spec': resource_spec,}
        self.job_header = header_template % config

        logger.debug("Job script: \n %s" % self.job_script())
