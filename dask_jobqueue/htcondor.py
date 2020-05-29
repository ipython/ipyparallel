import logging
import re
import shlex

import dask
from distributed.utils import parse_bytes

from .core import JobQueueCluster, Job, job_parameters, cluster_parameters

logger = logging.getLogger(__name__)


class HTCondorJob(Job):
    _script_template = """
%(shebang)s

%(job_header)s

Environment = "%(quoted_environment)s"
Arguments = "%(quoted_arguments)s"
Executable = %(executable)s

Queue
""".lstrip()

    submit_command = "condor_submit"
    cancel_command = "condor_rm"
    job_id_regexp = r"(?P<job_id>\d+\.\d+)"

    # condor sets argv[0] of the executable to "condor_exec.exe", which confuses
    # Python (can't find its libs), so we have to go through the shell.
    executable = "/bin/sh"

    config_name = "htcondor"

    def __init__(
        self,
        scheduler=None,
        name=None,
        disk=None,
        job_extra=None,
        config_name=None,
        **base_class_kwargs
    ):
        super().__init__(
            scheduler=scheduler, name=name, config_name=config_name, **base_class_kwargs
        )

        if disk is None:
            disk = dask.config.get("jobqueue.%s.disk" % self.config_name)
        if disk is None:
            raise ValueError(
                "You must specify how much disk to use per job like ``disk='1 GB'``"
            )
        self.worker_disk = parse_bytes(disk)
        if job_extra is None:
            self.job_extra = dask.config.get(
                "jobqueue.%s.job-extra" % self.config_name, {}
            )
        else:
            self.job_extra = job_extra

        env_extra = base_class_kwargs.get("env_extra", None)
        if env_extra is None:
            env_extra = dask.config.get(
                "jobqueue.%s.env-extra" % self.config_name, default=[]
            )
        self.env_dict = self.env_lines_to_dict(env_extra)

        self.job_header_dict = {
            "MY.DaskWorkerName": '"htcondor--$F(MY.JobId)--"',
            "RequestCpus": "MY.DaskWorkerCores",
            "RequestMemory": "floor(MY.DaskWorkerMemory / 1048576)",
            "RequestDisk": "floor(MY.DaskWorkerDisk / 1024)",
            "MY.JobId": '"$(ClusterId).$(ProcId)"',
            "MY.DaskWorkerCores": self.worker_cores,
            "MY.DaskWorkerMemory": self.worker_memory,
            "MY.DaskWorkerDisk": self.worker_disk,
        }
        if self.log_directory:
            self.job_header_dict.update(
                {
                    "LogDirectory": self.log_directory,
                    # $F(...) strips quotes
                    "Output": "$(LogDirectory)/worker-$F(MY.JobId).out",
                    "Error": "$(LogDirectory)/worker-$F(MY.JobId).err",
                    "Log": "$(LogDirectory)/worker-$(ClusterId).log",
                    # We kill all the workers to stop them so we need to stream their
                    # output+error if we ever want to see anything
                    "Stream_Output": True,
                    "Stream_Error": True,
                }
            )
        if self.job_extra:
            self.job_header_dict.update(self.job_extra)

    def env_lines_to_dict(self, env_lines):
        """ Convert an array of export statements (what we get from env-extra
        in the config) into a dict """
        env_dict = {}
        for env_line in env_lines:
            split_env_line = shlex.split(env_line)
            if split_env_line[0] == "export":
                split_env_line = split_env_line[1:]
            for item in split_env_line:
                if "=" in item:
                    k, v = item.split("=", 1)
                    env_dict[k] = v
        return env_dict

    def job_script(self):
        """ Construct a job submission script """
        quoted_arguments = quote_arguments(["-c", self._command_template])
        quoted_environment = quote_environment(self.env_dict)
        job_header_lines = "\n".join(
            "%s = %s" % (k, v) for k, v in self.job_header_dict.items()
        )
        return self._script_template % {
            "shebang": self.shebang,
            "job_header": job_header_lines,
            "quoted_environment": quoted_environment,
            "quoted_arguments": quoted_arguments,
            "executable": self.executable,
        }

    def _job_id_from_submit_output(self, out):
        cluster_id_regexp = r"submitted to cluster (\d+)"
        match = re.search(cluster_id_regexp, out)
        if match is None:
            msg = (
                "Could not parse cluster id from submission command output.\n"
                "Cluster id regexp is {!r}\n"
                "Submission command output is:\n{}".format(cluster_id_regexp, out)
            )
            raise ValueError(msg)
        return "%s.0" % match.group(1)


def _double_up_quotes(instr):
    return instr.replace("'", "''").replace('"', '""')


def quote_arguments(args):
    """Quote a string or list of strings using the Condor submit file "new" argument quoting rules.

    Returns
    -------
    str
        The arguments in a quoted form.

    Warnings
    --------
    You will need to surround the result in double-quotes before using it in
    the Arguments attribute.

    Examples
    --------
    >>> quote_arguments(["3", "simple", "arguments"])
    '3 simple arguments'
    >>> quote_arguments(["one", "two with spaces", "three"])
    'one \'two with spaces\' three'
    >>> quote_arguments(["one", "\"two\"", "spacy 'quoted' argument"])
    'one ""two"" \'spacey \'\'quoted\'\' argument\''
    """
    if isinstance(args, str):
        args_list = [args]
    else:
        args_list = args

    quoted_args = []
    for a in args_list:
        qa = _double_up_quotes(a)
        if " " in qa or "'" in qa:
            qa = "'" + qa + "'"
        quoted_args.append(qa)
    return " ".join(quoted_args)


def quote_environment(env):
    """Quote a dict of strings using the Condor submit file "new" environment quoting rules.

    Returns
    -------
    str
        The environment in quoted form.

    Warnings
    --------
    You will need to surround the result in double-quotes before using it in
    the Environment attribute.

    Examples
    --------
    >>> from collections import OrderedDict
    >>> quote_environment(OrderedDict([("one", 1), ("two", '"2"'), ("three", "spacey 'quoted' value")]))
    'one=1 two=""2"" three=\'spacey \'\'quoted\'\' value\''
    """
    if not isinstance(env, dict):
        raise TypeError("env must be a dict")

    entries = []
    for k, v in env.items():
        qv = _double_up_quotes(str(v))
        if " " in qv or "'" in qv:
            qv = "'" + qv + "'"
        entries.append("%s=%s" % (k, qv))

    return " ".join(entries)


class HTCondorCluster(JobQueueCluster):
    __doc__ = """ Launch Dask on an HTCondor cluster with a shared file system

    Parameters
    ----------
    disk : str
        Total amount of disk per job
    job_extra : dict
        Extra submit file attributes for the job
    {job}
    {cluster}

    Examples
    --------
    >>> from dask_jobqueue.htcondor import HTCondorCluster
    >>> cluster = HTCondorCluster(cores=24, memory="4GB", disk="4GB")
    >>> cluster.scale(jobs=10)  # ask for 10 jobs

    >>> from dask.distributed import Client
    >>> client = Client(cluster)

    This also works with adaptive clusters.  This automatically launches and kill workers based on load.

    >>> cluster.adapt(maximum_jobs=20)
    """.format(
        job=job_parameters, cluster=cluster_parameters
    )
    job_cls = HTCondorJob
