from __future__ import absolute_import, division, print_function

import logging
import shlex

import dask

from .core import JobQueueCluster, docstrings

logger = logging.getLogger(__name__)


class OARCluster(JobQueueCluster):
    __doc__ = docstrings.with_indents(
        """ Launch Dask on a OAR cluster

    Parameters
    ----------
    queue : str
        Destination queue for each worker job. Passed to `#OAR -q` option.
    project : str
        Accounting string associated with each worker job. Passed to `#OAR -p` option.
    resource_spec : str
        Request resources and specify job placement. Passed to `#OAR -l` option.
    walltime : str
        Walltime for each worker job.
    job_extra : list
        List of other OAR options, for example `-t besteffort`. Each option will be prepended with the #OAR prefix.
    %(JobQueueCluster.parameters)s

    Examples
    --------
    >>> from dask_jobqueue import OARCluster
    >>> cluster = OARCluster(queue='regular')
    >>> cluster.scale(10)  # this may take a few seconds to launch

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt()
    """,
        4,
    )

    # Override class variables
    submit_command = "oarsub"
    cancel_command = "oardel"
    job_id_regexp = r"OAR_JOB_ID=(?P<job_id>\d+)"

    def __init__(
        self,
        queue=None,
        project=None,
        resource_spec=None,
        walltime=None,
        job_extra=None,
        config_name="oar",
        **kwargs
    ):
        if queue is None:
            queue = dask.config.get("jobqueue.%s.queue" % config_name)
        if project is None:
            project = dask.config.get("jobqueue.%s.project" % config_name)
        if resource_spec is None:
            resource_spec = dask.config.get("jobqueue.%s.resource-spec" % config_name)
        if walltime is None:
            walltime = dask.config.get("jobqueue.%s.walltime" % config_name)
        if job_extra is None:
            job_extra = dask.config.get("jobqueue.%s.job-extra" % config_name)

        super(OARCluster, self).__init__(config_name=config_name, **kwargs)

        header_lines = []
        if self.name is not None:
            header_lines.append("#OAR -n %s" % self.name)
        if queue is not None:
            header_lines.append("#OAR -q %s" % queue)
        if project is not None:
            header_lines.append("#OAR --project %s" % project)

        # OAR needs to have the resource on a single line otherwise it is
        # considered as a "moldable job" (i.e. the scheduler can chose between
        # multiple sets of resources constraints)
        resource_spec_list = []
        if resource_spec is None:
            # default resource_spec if not specified. Crucial to specify
            # nodes=1 to make sure the cores allocated are on the same node.
            resource_spec = "/nodes=1/core=%d" % self.worker_cores
        resource_spec_list.append(resource_spec)
        if walltime is not None:
            resource_spec_list.append("walltime=%s" % walltime)

        full_resource_spec = ",".join(resource_spec_list)
        header_lines.append("#OAR -l %s" % full_resource_spec)
        header_lines.extend(["#OAR %s" % arg for arg in job_extra])
        header_lines.append("JOB_ID=${OAR_JOB_ID}")

        self.job_header = "\n".join(header_lines)

        logger.debug("Job script: \n %s" % self.job_script())

    def _submit_job(self, fn):
        # OAR specificity: the submission script needs to exist on the worker
        # when the job starts on the worker. This is different from other
        # schedulers that only need the script on the submission node at
        # submission time. That means that we can not use the same strategy as
        # in JobQueueCluster: create a temporary submission file, submit the
        # script, delete the submission file. In order to reuse the code in
        # the base JobQueueCluster class, we read from the temporary file and
        # reconstruct the command line where the script is passed in as a
        # string (inline script in OAR jargon) rather than as a filename.
        with open(fn) as f:
            content_lines = f.readlines()

        oar_lines = [line for line in content_lines if line.startswith("#OAR ")]
        oarsub_options = [line.replace("#OAR ", "").strip() for line in oar_lines]
        inline_script_lines = [
            line for line in content_lines if not line.startswith("#")
        ]
        inline_script = "".join(inline_script_lines)
        oarsub_command = " ".join([self.submit_command] + oarsub_options)
        oarsub_command_split = shlex.split(oarsub_command) + [inline_script]
        return self._call(oarsub_command_split)
