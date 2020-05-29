from contextlib import contextmanager
import logging
import math
import os
import re
import shlex
import subprocess
import sys
import weakref
import abc

import dask
from dask.utils import ignoring

from distributed.deploy.spec import ProcessInterface, SpecCluster
from distributed.deploy.local import nprocesses_nthreads
from distributed.scheduler import Scheduler
from distributed.utils import format_bytes, parse_bytes, tmpfile

logger = logging.getLogger(__name__)

job_parameters = """
    cores : int
        Total number of cores per job
    memory: str
        Total amount of memory per job
    processes : int
        Cut the job up into this many processes. Good for GIL workloads or for
        nodes with many cores.
        By default, ``process ~= sqrt(cores)`` so that the number of processes
        and the number of threads per process is roughly the same.
    interface : str
        Network interface like 'eth0' or 'ib0'. This will be used both for the
        Dask scheduler and the Dask workers interface. If you need a different
        interface for the Dask scheduler you can pass it through
        the ``scheduler_options`` argument:
        ``interface=your_worker_interface, scheduler_options={'interface': your_scheduler_interface}``.
    nanny : bool
        Whether or not to start a nanny process
    local_directory : str
        Dask worker local directory for file spilling.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    extra : list
        Additional arguments to pass to `dask-worker`
    env_extra : list
        Other commands to add to script before launching worker.
    header_skip : list
        Lines to skip in the header.
        Header lines matching this text will be removed
    log_directory : str
        Directory to use for job scheduler logs.
    shebang : str
        Path to desired interpreter for your batch submission script.
    python : str
        Python executable used to launch Dask workers.
        Defaults to the Python that is submitting these jobs
    config_name : str
        Section to use from jobqueue.yaml configuration file.
    name : str
        Name of Dask worker.  This is typically set by the Cluster
""".strip()


cluster_parameters = """
    n_workers : int
        Number of workers to start by default.  Defaults to 0.
        See the scale method
    silence_logs : str
        Log level like "debug", "info", or "error" to emit here if the
        scheduler is started locally
    asynchronous : bool
        Whether or not to run this cluster object with the async/await syntax
    security : Security
        A dask.distributed security object if you're using TLS/SSL
    scheduler_options : dict
        Used to pass additional arguments to Dask Scheduler. For example use
        ``scheduler_options={'dasboard_address': ':12435'}`` to specify which
        port the web dashboard should use or ``scheduler_options={'host': 'your-host'}``
        to specify the host the Dask scheduler should run on. See
        :class:`distributed.Scheduler` for more details.
""".strip()


class Job(ProcessInterface, abc.ABC):
    """ Base class to launch Dask workers on Job queues

    This class should not be used directly, use a class appropriate for
    your queueing system (e.g. PBScluster or SLURMCluster) instead.

    Parameters
    ----------
    {job_parameters}

    Attributes
    ----------
    submit_command: str
        Abstract attribute for job scheduler submit command,
        should be overridden
    cancel_command: str
        Abstract attribute for job scheduler cancel command,
        should be overridden

    See Also
    --------
    PBSCluster
    SLURMCluster
    SGECluster
    OARCluster
    LSFCluster
    MoabCluster
    """.format(
        job_parameters=job_parameters
    )

    _script_template = """
%(shebang)s

%(job_header)s
%(env_header)s
%(worker_command)s
""".lstrip()

    # Following class attributes should be overridden by extending classes.
    submit_command = None
    cancel_command = None
    config_name = None
    job_id_regexp = r"(?P<job_id>\d+)"

    @abc.abstractmethod
    def __init__(
        self,
        scheduler=None,
        name=None,
        cores=None,
        memory=None,
        processes=None,
        nanny=True,
        protocol=None,
        security=None,
        interface=None,
        death_timeout=None,
        local_directory=None,
        extra=None,
        env_extra=None,
        header_skip=None,
        log_directory=None,
        shebang=None,
        python=sys.executable,
        job_name=None,
        config_name=None,
    ):
        self.scheduler = scheduler
        self.job_id = None

        super().__init__()

        default_config_name = self.default_config_name()
        if config_name is None:
            config_name = default_config_name
        self.config_name = config_name

        if cores is None:
            cores = dask.config.get("jobqueue.%s.cores" % self.config_name)
        if memory is None:
            memory = dask.config.get("jobqueue.%s.memory" % self.config_name)

        if cores is None or memory is None:
            job_class_name = self.__class__.__name__
            cluster_class_name = job_class_name.replace("Job", "Cluster")
            raise ValueError(
                "You must specify how much cores and memory per job you want to use, for example:\n"
                "cluster = {}(cores={}, memory={!r})".format(
                    cluster_class_name, cores or 8, memory or "24GB"
                )
            )

        if job_name is None:
            job_name = dask.config.get("jobqueue.%s.name" % self.config_name)
        if processes is None:
            processes = dask.config.get("jobqueue.%s.processes" % self.config_name)
            if processes is None:
                processes, _ = nprocesses_nthreads(cores)
        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % self.config_name)
        if death_timeout is None:
            death_timeout = dask.config.get(
                "jobqueue.%s.death-timeout" % self.config_name
            )
        if local_directory is None:
            local_directory = dask.config.get(
                "jobqueue.%s.local-directory" % self.config_name
            )
        if extra is None:
            extra = dask.config.get("jobqueue.%s.extra" % self.config_name)
        if env_extra is None:
            env_extra = dask.config.get("jobqueue.%s.env-extra" % self.config_name)
        if header_skip is None:
            header_skip = dask.config.get(
                "jobqueue.%s.header-skip" % self.config_name, ()
            )
        if log_directory is None:
            log_directory = dask.config.get(
                "jobqueue.%s.log-directory" % self.config_name
            )
        if shebang is None:
            shebang = dask.config.get("jobqueue.%s.shebang" % self.config_name)

        # This attribute should be set in the derived class
        self.job_header = None

        if interface:
            extra = extra + ["--interface", interface]
        if protocol:
            extra = extra + ["--protocol", protocol]
        if security:
            worker_security_dict = security.get_tls_config_for_role("worker")
            security_command_line_list = [
                ["--tls-" + key.replace("_", "-"), value]
                for key, value in worker_security_dict.items()
                # 'ciphers' parameter does not have a command-line equivalent
                if key != "ciphers"
            ]
            security_command_line = sum(security_command_line_list, [])
            extra = extra + security_command_line

        # Keep information on process, cores, and memory, for use in subclasses
        self.worker_memory = parse_bytes(memory) if memory is not None else None
        self.worker_processes = processes
        self.worker_cores = cores
        self.name = name
        self.job_name = job_name

        self.shebang = shebang

        self._env_header = "\n".join(filter(None, env_extra))
        self.header_skip = set(header_skip)

        # dask-worker command line build
        dask_worker_command = "%(python)s -m distributed.cli.dask_worker" % dict(
            python=python
        )
        command_args = [dask_worker_command, self.scheduler]
        command_args += ["--nthreads", self.worker_process_threads]
        if processes is not None and processes > 1:
            command_args += ["--nprocs", processes]

        command_args += ["--memory-limit", self.worker_process_memory]
        command_args += ["--name", str(name)]
        command_args += ["--nanny" if nanny else "--no-nanny"]

        if death_timeout is not None:
            command_args += ["--death-timeout", death_timeout]
        if local_directory is not None:
            command_args += ["--local-directory", local_directory]
        if extra is not None:
            command_args += extra

        self._command_template = " ".join(map(str, command_args))

        self.log_directory = log_directory
        if self.log_directory is not None:
            if not os.path.exists(self.log_directory):
                os.makedirs(self.log_directory)

    @classmethod
    def default_config_name(cls):
        config_name = getattr(cls, "config_name", None)
        if config_name is None:
            raise ValueError(
                "The class {} is required to have a 'config_name' class variable.\n"
                "If you have created this class, please add a 'config_name' class variable.\n"
                "If not this may be a bug, feel free to create an issue at: "
                "https://github.com/dask/dask-jobqueue/issues/new".format(cls)
            )
        return config_name

    def job_script(self):
        """ Construct a job submission script """
        header = "\n".join(
            [
                line
                for line in self.job_header.split("\n")
                if not any(skip in line for skip in self.header_skip)
            ]
        )
        pieces = {
            "shebang": self.shebang,
            "job_header": header,
            "env_header": self._env_header,
            "worker_command": self._command_template,
        }
        return self._script_template % pieces

    @contextmanager
    def job_file(self):
        """ Write job submission script to temporary file """
        with tmpfile(extension="sh") as fn:
            with open(fn, "w") as f:
                logger.debug("writing job script: \n%s", self.job_script())
                f.write(self.job_script())
            yield fn

    async def _submit_job(self, script_filename):
        # Should we make this async friendly?
        return self._call(shlex.split(self.submit_command) + [script_filename])

    @property
    def worker_process_threads(self):
        return int(self.worker_cores / self.worker_processes)

    @property
    def worker_process_memory(self):
        mem = format_bytes(self.worker_memory / self.worker_processes)
        mem = mem.replace(" ", "")
        return mem

    async def start(self):
        """ Start workers and point them to our local scheduler """
        logger.debug("Starting worker: %s", self.name)

        with self.job_file() as fn:
            out = await self._submit_job(fn)
            self.job_id = self._job_id_from_submit_output(out)

        weakref.finalize(self, self._close_job, self.job_id)

        logger.debug("Starting job: %s", self.job_id)
        await super().start()

    def _job_id_from_submit_output(self, out):
        match = re.search(self.job_id_regexp, out)
        if match is None:
            msg = (
                "Could not parse job id from submission command "
                "output.\nJob id regexp is {!r}\nSubmission command "
                "output is:\n{}".format(self.job_id_regexp, out)
            )
            raise ValueError(msg)

        job_id = match.groupdict().get("job_id")
        if job_id is None:
            msg = (
                "You need to use a 'job_id' named group in your regexp, e.g. "
                "r'(?P<job_id>\\d+)', in your regexp. Your regexp was: "
                "{!r}".format(self.job_id_regexp)
            )
            raise ValueError(msg)

        return job_id

    async def close(self):
        logger.debug("Stopping worker: %s job: %s", self.name, self.job_id)
        self._close_job(self.job_id)

    @classmethod
    def _close_job(cls, job_id):
        if job_id:
            with ignoring(RuntimeError):  # deleting job when job already gone
                cls._call(shlex.split(cls.cancel_command) + [job_id])
            logger.debug("Closed job %s", job_id)

    @staticmethod
    def _call(cmd, **kwargs):
        """ Call a command using subprocess.Popen.

        This centralizes calls out to the command line, providing consistent
        outputs, logging, and an opportunity to go asynchronous in the future.

        Parameters
        ----------
        cmd: List(str))
            A command, each of which is a list of strings to hand to
            subprocess.Popen

        Examples
        --------
        >>> self._call(['ls', '/foo'])

        Returns
        -------
        The stdout produced by the command, as string.

        Raises
        ------
        RuntimeError if the command exits with a non-zero exit code
        """
        cmd_str = " ".join(cmd)
        logger.debug(
            "Executing the following command to command line\n{}".format(cmd_str)
        )

        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
        )

        out, err = proc.communicate()
        out, err = out.decode(), err.decode()

        if proc.returncode != 0:
            raise RuntimeError(
                "Command exited with non-zero exit code.\n"
                "Exit code: {}\n"
                "Command:\n{}\n"
                "stdout:\n{}\n"
                "stderr:\n{}\n".format(proc.returncode, cmd_str, out, err)
            )
        return out


class JobQueueCluster(SpecCluster):
    __doc__ = """ Deploy Dask on a Job queuing system

    This is a superclass, and is rarely used directly.  It is more common to
    use an object like SGECluster, SLURMCluster, PBSCluster, LSFCluster, or
    others.

    However, it can be used directly if you have a custom ``Job`` type.
    This class relies heavily on being passed a ``Job`` type that is able to
    launch one Job on a job queueing system.

    Parameters
    ----------
    Job : Job
        A class that can be awaited to ask for a single Job
    {cluster_parameters}
    """.format(
        cluster_parameters=cluster_parameters
    )

    def __init__(
        self,
        n_workers=0,
        job_cls: Job = None,
        # Cluster keywords
        loop=None,
        security=None,
        silence_logs="error",
        name=None,
        asynchronous=False,
        # Scheduler-only keywords
        dashboard_address=None,
        host=None,
        scheduler_options=None,
        # Options for both scheduler and workers
        interface=None,
        protocol="tcp://",
        # Job keywords
        config_name=None,
        **job_kwargs
    ):
        self.status = "created"

        default_job_cls = getattr(type(self), "job_cls", None)
        self.job_cls = default_job_cls
        if job_cls is not None:
            self.job_cls = job_cls

        if self.job_cls is None:
            raise ValueError(
                "You need to specify a Job type. Two cases:\n"
                "- you are inheriting from JobQueueCluster (most likely): you need to add a 'job_cls' class variable "
                "in your JobQueueCluster-derived class {}\n"
                "- you are using JobQueueCluster directly (less likely, only useful for tests): "
                "please explicitly pass a Job type through the 'job_cls' parameter.".format(
                    type(self)
                )
            )

        if dashboard_address is not None:
            raise ValueError(
                "Please pass 'dashboard_address' through 'scheduler_options': use\n"
                'cluster = {0}(..., scheduler_options={{"dashboard_address": ":12345"}}) rather than\n'
                'cluster = {0}(..., dashboard_address="12435")'.format(
                    self.__class__.__name__
                )
            )

        if host is not None:
            raise ValueError(
                "Please pass 'host' through 'scheduler_options': use\n"
                'cluster = {0}(..., scheduler_options={{"host": "your-host"}}) rather than\n'
                'cluster = {0}(..., host="your-host")'.format(self.__class__.__name__)
            )

        default_config_name = self.job_cls.default_config_name()
        if config_name is None:
            config_name = default_config_name

        if interface is None:
            interface = dask.config.get("jobqueue.%s.interface" % config_name)
        if scheduler_options is None:
            scheduler_options = dask.config.get(
                "jobqueue.%s.scheduler-options" % config_name, {}
            )

        default_scheduler_options = {
            "protocol": protocol,
            "dashboard_address": ":8787",
            "security": security,
        }
        # scheduler_options overrides parameters common to both workers and scheduler
        scheduler_options = dict(default_scheduler_options, **scheduler_options)

        # Use the same network interface as the workers if scheduler ip has not
        # been set through scheduler_options via 'host' or 'interface'
        if "host" not in scheduler_options and "interface" not in scheduler_options:
            scheduler_options["interface"] = interface

        scheduler = {
            "cls": Scheduler,  # Use local scheduler for now
            "options": scheduler_options,
        }

        job_kwargs["config_name"] = config_name
        job_kwargs["interface"] = interface
        job_kwargs["protocol"] = protocol
        job_kwargs["security"] = security
        self._job_kwargs = job_kwargs

        worker = {"cls": self.job_cls, "options": self._job_kwargs}
        if "processes" in self._job_kwargs and self._job_kwargs["processes"] > 1:
            worker["group"] = [
                "-" + str(i) for i in range(self._job_kwargs["processes"])
            ]

        self._dummy_job  # trigger property to ensure that the job is valid

        super().__init__(
            scheduler=scheduler,
            worker=worker,
            loop=loop,
            security=security,
            silence_logs=silence_logs,
            asynchronous=asynchronous,
            name=name,
        )

        if n_workers:
            self.scale(n_workers)

    @property
    def _dummy_job(self):
        """
        Creates a Job similar to what we will use in practice

        This is used for backwards functionality and a variety of convenience
        functions.  It is also used on construction to raise errors if any of
        the keywords are improper.
        """
        try:
            address = self.scheduler.address  # Have we already connected?
        except AttributeError:
            address = "tcp://<insert-scheduler-address-here>:8786"
        try:
            return self.job_cls(
                address or "tcp://<insert-scheduler-address-here>:8786",
                name="name",
                **self._job_kwargs
            )
        except TypeError as exc:
            # Very likely this error happened in the self.job_cls constructor
            # because an unexpected parameter was used in the JobQueueCluster
            # constructor. The next few lines builds a more user-friendly error message.
            match = re.search("(unexpected keyword argument.+)", str(exc))
            if not match:
                raise
            message_orig = match.group(1)
            raise ValueError(
                'Got {}. Very likely this unexpected parameter was passed in "job_kwargs" in the {} constructor:\n'
                "job_kwargs={}".format(
                    message_orig, self.__class__.__name__, self._job_kwargs
                )
            ) from exc

    @property
    def job_header(self):
        return self._dummy_job.job_header

    def job_script(self):
        return self._dummy_job.job_script()

    @property
    def job_name(self):
        return self._dummy_job.job_name

    def scale(self, n=None, jobs=0, memory=None, cores=None):
        """ Scale cluster to specified configurations.

        Parameters
        ----------
        n : int
           Target number of workers
        jobs : int
           Target number of jobs
        memory : str
           Target amount of memory
        cores : int
           Target number of cores

        """
        if n is not None:
            jobs = int(math.ceil(n / self._dummy_job.worker_processes))

        return super().scale(jobs, memory=memory, cores=cores)

    def adapt(
        self, *args, minimum_jobs: int = None, maximum_jobs: int = None, **kwargs
    ):
        """ Scale Dask cluster automatically based on scheduler activity.

        Parameters
        ----------
        minimum : int
           Minimum number of workers to keep around for the cluster
        maximum : int
           Maximum number of workers to keep around for the cluster
        minimum_memory : str
           Minimum amount of memory for the cluster
        maximum_memory : str
           Maximum amount of memory for the cluster
        minimum_jobs : int
           Minimum number of jobs
        maximum_jobs : int
           Maximum number of jobs
        **kwargs :
           Extra parameters to pass to dask.distributed.Adaptive

        See Also
        --------
        dask.distributed.Adaptive : for more keyword arguments
        """

        if minimum_jobs is not None:
            kwargs["minimum"] = minimum_jobs * self._dummy_job.worker_processes
        if maximum_jobs is not None:
            kwargs["maximum"] = maximum_jobs * self._dummy_job.worker_processes
        return super().adapt(*args, **kwargs)
