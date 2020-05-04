import logging
import os
from tornado.process import Subprocess

from .core import Job, JobQueueCluster, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class LocalJob(Job):
    __doc__ = """ Use Dask Jobqueue with local bash commands

    This is mostly for testing.  It uses all the same machinery of
    dask-jobqueue, but rather than submitting jobs to some external job
    queueing system, it launches them locally.  For normal local use, please
    see ``dask.distributed.LocalCluster``

    Parameters
    ----------
    {job}
    """.format(
        job=job_parameters
    )

    config_name = "local"

    def __init__(
        self,
        scheduler=None,
        name=None,
        queue=None,
        project=None,
        resource_spec=None,
        walltime=None,
        job_extra=None,
        config_name=None,
        **kwargs
    ):
        # Instantiate args and parameters from parent abstract class
        super().__init__(
            scheduler=scheduler,
            name=name,
            config_name=config_name,
            shebang="",
            **kwargs
        )

        # Declare class attribute that shall be overridden
        self.job_header = ""

        logger.debug("Job script: \n %s" % self.job_script())

    async def _submit_job(self, script_filename):
        # Should we make this async friendly?
        with open(script_filename) as f:
            text = f.read().strip().split()
        self.process = Subprocess(
            text, stdout=Subprocess.STREAM, stderr=Subprocess.STREAM
        )

        lines = []
        while True:
            line = await self.process.stderr.read_until(
                b"\n"
            )  # make sure that we start
            lines.append(line.decode())
            if b"Registered to:" in line:
                break
            if b"error" in line.lower():
                raise Exception("Worker failed\n\n" + "".join(lines))

        return str(self.process.pid)

    @classmethod
    def _close_job(self, job_id):
        os.kill(int(job_id), 9)
        # from distributed.utils_test import terminate_process
        # terminate_process(self.process)


class LocalCluster(JobQueueCluster):
    __doc__ = """ Use dask-jobqueue with local bash commands

    This is mostly for testing.  It uses all the same machinery of
    dask-jobqueue, but rather than submitting jobs to some external job
    queueing system, it launches them locally.  For normal local use, please
    see ``dask.distributed.LocalCluster``

    Parameters
    ----------
    {job}
    {cluster}

    Examples
    --------
    >>> from dask_jobqueue import LocalCluster
    >>> cluster = LocalCluster(cores=2, memory="4 GB")
    >>> cluster.scale(jobs=3)  # ask for 3 jobs

    See Also
    --------
    dask.distributed.LocalCluster
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = LocalJob
