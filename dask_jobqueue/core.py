from contextlib import contextmanager
import logging
import shlex
import socket
import subprocess
import sys
import warnings

import dask
import docrep
from distributed import LocalCluster
from distributed.deploy import Cluster
from distributed.utils import (get_ip_interface, ignoring, parse_bytes, tmpfile,
                               format_bytes)

logger = logging.getLogger(__name__)
docstrings = docrep.DocstringProcessor()


threads_deprecation_message = """
The threads keyword has been removed and the memory keyword has changed.

Please specify job size with the following keywords:

-  cores: total cores per job, across all processes
-  memory: total memory per job, across all processes
-  processes: number of processes to launch, splitting the quantities above
""".strip()


@docstrings.get_sectionsf('JobQueueCluster')
class JobQueueCluster(Cluster):
    """ Base class to launch Dask Clusters for Job queues

    This class should not be used directly, use inherited class appropriate
    for your queueing system (e.g. PBScluster or SLURMCluster)

    Parameters
    ----------
    name : str
        Name of Dask workers.
    cores : int
        Total number of cores per job
    memory: str
        Total amount of memory per job
    processes : int
        Number of processes per job
    interface : str
        Network interface like 'eth0' or 'ib0'.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    local_directory : str
        Dask worker local directory for file spilling.
    extra : str
        Additional arguments to pass to `dask-worker`
    env_extra : list
        Other commands to add to script before launching worker.
    kwargs : dict
        Additional keyword arguments to pass to `LocalCluster`

    Attributes
    ----------
    submit_command: str
        Abstract attribute for job scheduler submit command,
        should be overriden
    cancel_command: str
        Abstract attribute for job scheduler cancel command,
        should be overriden

    See Also
    --------
    PBSCluster
    SLURMCluster
    """

    _script_template = """
#!/bin/bash

%(job_header)s

%(env_header)s

%(worker_command)s
""".lstrip()

    # Following class attributes should be overriden by extending classes.
    submit_command = None
    cancel_command = None
    scheduler_name = ''

    def __init__(self,
                 name=None,
                 cores=None,
                 memory=None,
                 processes=None,
                 interface=None,
                 death_timeout=None,
                 local_directory=None,
                 extra=None,
                 env_extra=None,
                 walltime=None,
                 threads=None,
                 **kwargs
                 ):
        """ """
        # """
        # This initializer should be considered as Abstract, and never used
        # directly.
        # """
        if threads is not None:
            raise ValueError(threads_deprecation_message)

        if not self.scheduler_name:
            raise NotImplementedError('JobQueueCluster is an abstract class '
                                      'that should not be instanciated.')

        if name is None:
            name = dask.config.get('jobqueue.%s.name' % self.scheduler_name)
        if cores is None:
            cores = dask.config.get('jobqueue.%s.cores' % self.scheduler_name)
        if memory is None:
            memory = dask.config.get('jobqueue.%s.memory' % self.scheduler_name)
        if processes is None:
            processes = dask.config.get('jobqueue.%s.processes' % self.scheduler_name)
        if interface is None:
            interface = dask.config.get('jobqueue.%s.interface' % self.scheduler_name)
        if death_timeout is None:
            death_timeout = dask.config.get('jobqueue.%s.death-timeout' % self.scheduler_name)
        if local_directory is None:
            local_directory = dask.config.get('jobqueue.%s.local-directory' % self.scheduler_name)
        if extra is None:
            extra = dask.config.get('jobqueue.%s.extra' % self.scheduler_name)
        if env_extra is None:
            env_extra = dask.config.get('jobqueue.%s.env-extra' % self.scheduler_name)

        if dask.config.get('jobqueue.%s.threads', None):
            warnings.warn(threads_deprecation_message)

        if cores is None:
            raise ValueError("You must specify how many cores to use per job "
                             "like ``cores=8``")

        if memory is None:
            raise ValueError("You must specify how much memory to use per job "
                             "like ``memory='24 GB'``")

        #This attribute should be overriden
        self.job_header = None

        if interface:
            host = get_ip_interface(interface)
            extra += ' --interface  %s ' % interface
        else:
            host = socket.gethostname()

        self.local_cluster = LocalCluster(n_workers=0, ip=host, **kwargs)

        # Keep information on process, cores, and memory, for use in subclasses
        self.worker_memory = parse_bytes(memory)

        self.worker_processes = processes
        self.worker_cores = cores
        self.name = name

        self.jobs = dict()
        self.n = 0
        self._adaptive = None

        self._env_header = '\n'.join(env_extra)

        # dask-worker command line build
        dask_worker_command = (
            '%(python)s -m distributed.cli.dask_worker' % dict(python=sys.executable))
        self._command_template = ' '.join([dask_worker_command, self.scheduler.address])
        self._command_template += " --nthreads %d" % self.worker_threads
        if processes is not None and processes > 1:
            self._command_template += " --nprocs %d" % processes

        mem = format_bytes(self.worker_memory / self.worker_processes)
        mem = mem.replace(' ', '')
        self._command_template += " --memory-limit %s" % mem

        if name is not None:
            self._command_template += " --name %s" % name
            self._command_template += "-%(n)d" # Keep %(n) to be replaced later
        if death_timeout is not None:
            self._command_template += " --death-timeout %s" % death_timeout
        if local_directory is not None:
            self._command_template += " --local-directory %s" % local_directory
        if extra is not None:
            self._command_template += extra

    @property
    def worker_threads(self):
        return int(self.worker_cores / self.worker_processes)

    def job_script(self):
        """ Construct a job submission script """
        self.n += 1
        template = self._command_template % {'n': self.n}
        return self._script_template % {'job_header': self.job_header,
                                        'env_header': self._env_header,
                                        'worker_command': template}

    @contextmanager
    def job_file(self):
        """ Write job submission script to temporary file """
        with tmpfile(extension='sh') as fn:
            with open(fn, 'w') as f:
                f.write(self.job_script())
            yield fn

    def start_workers(self, n=1):
        """ Start workers and point them to our local scheduler """
        workers = []
        for _ in range(n):
            with self.job_file() as fn:
                out = self._call(shlex.split(self.submit_command) + [fn])
                job = self._job_id_from_submit_output(out.decode())
                self.jobs[self.n] = job
                workers.append(self.n)
        return workers

    @property
    def scheduler(self):
        """ The scheduler of this cluster """
        return self.local_cluster.scheduler

    def _calls(self, cmds):
        """ Call a command using subprocess.communicate

        This centralzies calls out to the command line, providing consistent
        outputs, logging, and an opportunity to go asynchronous in the future

        Parameters
        ----------
        cmd: List(List(str))
            A list of commands, each of which is a list of strings to hand to
            subprocess.communicate

        Examples
        --------
        >>> self._calls([['ls'], ['ls', '/foo']])

        Returns
        -------
        The stdout result as a string
        Also logs any stderr information
        """
        logger.debug("Submitting the following calls to command line")
        for cmd in cmds:
            logger.debug(' '.join(cmd))
        procs = [subprocess.Popen(cmd,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)
                 for cmd in cmds]

        result = []
        for proc in procs:
            out, err = proc.communicate()
            if err:
                logger.error(err.decode())
            result.append(out)
        return result

    def _call(self, cmd):
        """ Singular version of _calls """
        return self._calls([cmd])[0]

    def stop_workers(self, workers):
        """ Stop a list of workers"""
        if not workers:
            return
        workers = list(map(int, workers))
        jobs = [self.jobs[w] for w in workers]
        self._call([self.cancel_command] + list(jobs))
        for w in workers:
            with ignoring(KeyError):
                del self.jobs[w]

    def scale_up(self, n, **kwargs):
        """ Brings total worker count up to ``n`` """
        return self.start_workers(n - len(self.jobs))

    def scale_down(self, workers):
        ''' Close the workers with the given addresses '''
        if isinstance(workers, dict):
            names = {v['name'] for v in workers.values()}
            job_ids = {name.split('-')[-2] for name in names}
            self.stop_workers(job_ids)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop_workers(self.jobs)
        self.local_cluster.__exit__(type, value, traceback)

    def _job_id_from_submit_output(self, out):
        raise NotImplementedError('_job_id_from_submit_output must be '
                                  'implemented when JobQueueCluster is '
                                  'inherited. It should convert the stdout '
                                  'from submit_command to the job id')
