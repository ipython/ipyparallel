from __future__ import absolute_import, division, print_function

import logging

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class SGECluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask on a SGE cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#$ -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#$ -A` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#$ -l` option.
    walltime : str
        Walltime for each worker job.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import SGECluster
    >>> cluster = SGECluster(queue='regular')
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """, 4)

    # Override class variables
    submit_command = 'qsub -terse'
    cancel_command = 'qdel'
    scheduler_name = 'sge'

    def __init__(self, queue=None, project=None, resource_spec=None, walltime=None, **kwargs):
        if queue is None:
            queue = dask.config.get('jobqueue.%s.queue' % self.scheduler_name)
        if project is None:
            project = dask.config.get('jobqueue.%s.project' % self.scheduler_name)
        if resource_spec is None:
            resource_spec = dask.config.get('jobqueue.%s.resource-spec' % self.scheduler_name)
        if walltime is None:
            walltime = dask.config.get('jobqueue.%s.walltime' % self.scheduler_name)

        super(SGECluster, self).__init__(**kwargs)

        header_lines = ['#!/usr/bin/env bash']
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
        if self.log_directory is not None:
            header_lines.append('#$ -e %(log_directory)s/')
            header_lines.append('#$ -o %(log_directory)s/')
        header_lines.extend(['#$ -cwd', '#$ -j y'])
        header_template = '\n'.join(header_lines)

        config = {'name': self.name,
                  'queue': queue,
                  'project': project,
                  'processes': self.worker_processes,
                  'walltime': walltime,
                  'resource_spec': resource_spec,
                  'log_directory': self.log_directory}
        self.job_header = header_template % config

        logger.debug("Job script: \n %s" % self.job_script())
