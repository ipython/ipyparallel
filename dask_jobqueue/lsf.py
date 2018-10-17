from __future__ import absolute_import, division, print_function

import logging
import math

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class LSFCluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(""" Launch Dask on a LSF cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#BSUB -q` option.
    project : str
        Accounting string associated with each worker job. Passed to
        `#BSUB -P` option.
    ncpus : int
        Number of cpus. Passed to `#BSUB -n` option.
    mem : int
        Request memory in bytes. Passed to `#BSUB -M` option.
    walltime : str
        Walltime for each worker job in HH:MM. Passed to `#BSUB -W` option.
    job_extra : list
        List of other LSF options, for example -u. Each option will be
        prepended with the #LSF prefix.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import LSFCluster
    >>> cluster = LSFcluster(queue='general', project='DaskonLSF',
    ...                      cores=15, memory='25GB')
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and
    kill workers based on load.

    >>> cluster.adapt()
    """, 4)

    # Override class variables
    submit_command = 'bsub <'
    cancel_command = 'bkill'
    scheduler_name = 'lsf'

    def __init__(self, queue=None, project=None, ncpus=None, mem=None, walltime=None, job_extra=None, **kwargs):
        if queue is None:
            queue = dask.config.get('jobqueue.%s.queue' % self.scheduler_name)
        if project is None:
            project = dask.config.get('jobqueue.%s.project' % self.scheduler_name)
        if ncpus is None:
            ncpus = dask.config.get('jobqueue.%s.ncpus' % self.scheduler_name)
        if mem is None:
            mem = dask.config.get('jobqueue.%s.mem' % self.scheduler_name)
        if walltime is None:
            walltime = dask.config.get('jobqueue.%s.walltime' % self.scheduler_name)
        if job_extra is None:
            job_extra = dask.config.get('jobqueue.%s.job-extra' % self.scheduler_name)

        # Instantiate args and parameters from parent abstract class
        super(LSFCluster, self).__init__(**kwargs)

        header_lines = []
        # LSF header build
        if self.name is not None:
            header_lines.append('#BSUB -J %s' % self.name)
        if self.log_directory is not None:
            header_lines.append('#BSUB -e %s/%s-%%J.err' %
                                (self.log_directory, self.name or 'worker'))
            header_lines.append('#BSUB -o %s/%s-%%J.out' %
                                (self.log_directory, self.name or 'worker'))
        if queue is not None:
            header_lines.append('#BSUB -q %s' % queue)
        if project is not None:
            header_lines.append('#BSUB -P %s' % project)
        if ncpus is None:
            # Compute default cores specifications
            ncpus = self.worker_cores
            logger.info("ncpus specification for LSF not set, initializing it to %s" % ncpus)
        if ncpus is not None:
            header_lines.append('#BSUB -n %s' % ncpus)
            if ncpus > 1:
                # span[hosts=1] _might_ affect queue waiting
                # time, and is not required if ncpus==1
                header_lines.append('#BSUB -R "span[hosts=1]"')
        if mem is None:
            # Compute default memory specifications
            mem = self.worker_memory
            logger.info("mem specification for LSF not set, initializing it to %s" % mem)
        if mem is not None:
            memory_string = lsf_format_bytes_ceil(mem)
            header_lines.append('#BSUB -M %s' % memory_string)
        if walltime is not None:
            header_lines.append('#BSUB -W %s' % walltime)
        header_lines.extend(['#BSUB %s' % arg for arg in job_extra])
        header_lines.append('JOB_ID=${LSB_JOBID%.*}')

        # Declare class attribute that shall be overriden
        self.job_header = '\n'.join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())

    def _submit_job(self, script_filename):
        piped_cmd = [self.submit_command + ' ' + script_filename + ' 2> /dev/null']
        return self._call(piped_cmd, shell=True)


def lsf_format_bytes_ceil(n):
    """ Format bytes as text

    Convert bytes to megabytes which LSF requires.

    Parameters
    ----------
    n: int
        Bytes

    Examples
    --------
    >>> lsf_format_bytes_ceil(1234567890)
    '1235'
    """
    return '%d' % math.ceil(n / (1000**2))
