from __future__ import absolute_import, division, print_function

import logging
import math
import os

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class LSFCluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(
        """ Launch Dask on a LSF cluster

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
    lsf_units : str
        Unit system for large units in resource usage set by the
        LSF_UNIT_FOR_LIMITS in the lsf.conf file of a cluster.
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
    """,
        4,
    )

    # Override class variables
    submit_command = "bsub <"
    cancel_command = "bkill"

    def __init__(
        self,
        queue=None,
        project=None,
        ncpus=None,
        mem=None,
        walltime=None,
        job_extra=None,
        lsf_units=None,
        config_name="lsf",
        **kwargs
    ):
        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % config_name)
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % config_name)
        if ncpus is None:
            ncpus = dask.config.get("jobqueue.%s.ncpus" % config_name)
        if mem is None:
            mem = dask.config.get("jobqueue.%s.mem" % config_name)
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % config_name)
        if lsf_units is None:
            lsf_units = dask.config.get("jobqueue.%s.lsf-units" % config_name)

        # Instantiate args and parameters from parent abstract class
        super(LSFCluster, self).__init__(config_name=config_name, **kwargs)

        header_lines = []
        # LSF header build
        if self.name is not None:
            header_lines.append("#BSUB -J %s" % self.name)
        if self.log_directory is not None:
            header_lines.append(
                "#BSUB -e %s/%s-%%J.err" % (self.log_directory, self.name or "worker")
            )
            header_lines.append(
                "#BSUB -o %s/%s-%%J.out" % (self.log_directory, self.name or "worker")
            )
        if queue is not None:
            header_lines.append("#BSUB -q %s" % queue)
        if project is not None:
            header_lines.append("#BSUB -P %s" % project)
        if ncpus is None:
            # Compute default cores specifications
            ncpus = self.worker_cores
            logger.info(
                "ncpus specification for LSF not set, initializing it to %s" % ncpus
            )
        if ncpus is not None:
            header_lines.append("#BSUB -n %s" % ncpus)
            if ncpus > 1:
                # span[hosts=1] _might_ affect queue waiting
                # time, and is not required if ncpus==1
                header_lines.append('#BSUB -R "span[hosts=1]"')
        if mem is None:
            # Compute default memory specifications
            mem = self.worker_memory
            logger.info(
                "mem specification for LSF not set, initializing it to %s bytes" % mem
            )
        if mem is not None:
            lsf_units = lsf_units if lsf_units is not None else lsf_detect_units()
            memory_string = lsf_format_bytes_ceil(mem, lsf_units=lsf_units)
            header_lines.append("#BSUB -M %s" % memory_string)
        if walltime is not None:
            header_lines.append("#BSUB -W %s" % walltime)
        header_lines.extend(["#BSUB %s" % arg for arg in job_extra])
        header_lines.append("JOB_ID=${LSB_JOBID%.*}")

        # Declare class attribute that shall be overridden
        self.job_header = "\n".join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())

    def _submit_job(self, script_filename):
        piped_cmd = [self.submit_command + " " + script_filename + " 2> /dev/null"]
        return self._call(piped_cmd, shell=True)


def lsf_format_bytes_ceil(n, lsf_units="mb"):
    """ Format bytes as text

    Convert bytes to megabytes which LSF requires.

    Parameters
    ----------
    n: int
        Bytes
    lsf_units: str
        Units for the memory in 2 character shorthand, kb through eb

    Examples
    --------
    >>> lsf_format_bytes_ceil(1234567890)
    '1235'
    """
    lsf_units = lsf_units.lower()[0]
    converter = {"k": 1, "m": 2, "g": 3, "t": 4, "p": 5, "e": 6, "z": 7}
    return "%d" % math.ceil(n / (1000 ** converter[lsf_units]))


def lsf_detect_units():
    """ Try to autodetect the unit scaling on an LSF system

    """
    # Search for automatically, Using docs from LSF 9.1.3 for search/defaults
    unit = "kb"  # Default fallback unit
    try:
        # Start looking for the LSF conf file
        conf_dir = "/etc"  # Fall back directory
        # Search the two environment variables the docs say it could be at (likely a typo in docs)
        for conf_env in ["LSF_ENVDIR", "LSF_CONFDIR"]:
            conf_search = os.environ.get(conf_env, None)
            if conf_search is not None:
                conf_dir = conf_search
                break
        conf_path = os.path.join(conf_dir, "lsf.conf")
        conf_file = open(conf_path, "r").readlines()
        # Reverse order search (in case defined twice, get the one which will actually be processed)
        for line in conf_file[::-1]:
            # Look for very specific line
            line = line.strip()
            if not line.strip().startswith("LSF_UNIT_FOR_LIMITS"):
                continue
            # Found the line, infer the unit, only first 2 chars after "="
            unit = line.split("=")[1].lower()[0]
            break
        logger.debug(
            "Setting units to %s from the LSF config file at %s" % (unit, conf_file)
        )
    # Trap the lsf.conf does not exist, and the conf file not setup right (i.e. "$VAR=xxx^" regex-form)
    except (EnvironmentError, IndexError):
        logger.debug(
            "Could not find LSF config or config file did not have LSF_UNIT_FOR_LIMITS set. Falling back to "
            "default unit of %s." % unit
        )
    return unit
