from __future__ import absolute_import, division, print_function

import logging
import math

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class SLURMCluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask on a SLURM cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#SBATCH -p` option.
    project : str
        Accounting string associated with each worker job. Passed to `#SBATCH -A` option.
    walltime : str
        Walltime for each worker job.
    job_cpu : int
        Number of cpu to book in SLURM, if None, defaults to worker `threads * processes`
    job_mem : str
        Amount of memory to request in SLURM. If None, defaults to worker
        processes * memory
    job_extra : list
        List of other Slurm options, for example -j oe. Each option will be prepended with the #SBATCH prefix.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import SLURMCluster
    >>> cluster = SLURMCluster(processes=6, cores=24, memory="120GB",
                               env_extra=['export LANG="en_US.utf8"',
                                          'export LANGUAGE="en_US.utf8"',
                                          'export LC_ALL="en_US.utf8"'])
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """, 4)

    # Override class variables
    submit_command = 'sbatch --parsable'
    cancel_command = 'scancel'
    scheduler_name = 'slurm'

    def __init__(self, queue=None, project=None, walltime=None, job_cpu=None, job_mem=None, job_extra=None, **kwargs):
        if queue is None:
            queue = dask.config.get('jobqueue.slurm.queue')
        if project is None:
            project = dask.config.get('jobqueue.slurm.project')
        if walltime is None:
            walltime = dask.config.get('jobqueue.slurm.walltime')
        if job_cpu is None:
            job_cpu = dask.config.get('jobqueue.slurm.job-cpu')
        if job_mem is None:
            job_mem = dask.config.get('jobqueue.slurm.job-mem')
        if job_extra is None:
            job_extra = dask.config.get('jobqueue.slurm.job-extra')

        super(SLURMCluster, self).__init__(**kwargs)

        # Always ask for only one task
        header_lines = ['#!/usr/bin/env bash']
        # SLURM header build
        if self.name is not None:
            header_lines.append('#SBATCH -J %s' % self.name)
        if self.log_directory is not None:
            header_lines.append('#SBATCH -e %s/%s-%%J.err' %
                                (self.log_directory, self.name or 'worker'))
            header_lines.append('#SBATCH -o %s/%s-%%J.out' %
                                (self.log_directory, self.name or 'worker'))
        if queue is not None:
            header_lines.append('#SBATCH -p %s' % queue)
        if project is not None:
            header_lines.append('#SBATCH -A %s' % project)

        # Init resources, always 1 task,
        # and then number of cpu is processes * threads if not set
        header_lines.append('#SBATCH -n 1')
        header_lines.append('#SBATCH --cpus-per-task=%d' % (job_cpu or self.worker_cores))
        # Memory
        memory = job_mem
        if job_mem is None:
            memory = slurm_format_bytes_ceil(self.worker_memory)
        if memory is not None:
            header_lines.append('#SBATCH --mem=%s' % memory)

        if walltime is not None:
            header_lines.append('#SBATCH -t %s' % walltime)
        header_lines.extend(['#SBATCH %s' % arg for arg in job_extra])

        header_lines.append('JOB_ID=${SLURM_JOB_ID%;*}')

        # Declare class attribute that shall be overriden
        self.job_header = '\n'.join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())


def slurm_format_bytes_ceil(n):
    """ Format bytes as text.

    SLURM expects KiB, MiB or Gib, but names it KB, MB, GB. SLURM does not handle Bytes, only starts at KB.

    >>> slurm_format_bytes_ceil(1)
    '1K'
    >>> slurm_format_bytes_ceil(1234)
    '2K'
    >>> slurm_format_bytes_ceil(12345678)
    '13M'
    >>> slurm_format_bytes_ceil(1234567890)
    '2G'
    >>> slurm_format_bytes_ceil(15000000000)
    '14G'
    """
    if n >= (1024**3):
        return '%dG' % math.ceil(n / (1024**3))
    if n >= (1024**2):
        return '%dM' % math.ceil(n / (1024**2))
    if n >= 1024:
        return '%dK' % math.ceil(n / 1024)
    return '1K' % n
