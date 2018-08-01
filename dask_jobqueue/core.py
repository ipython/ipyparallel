from __future__ import absolute_import, division, print_function

import logging
import math
import shlex
import socket
import subprocess
import sys
import warnings
from collections import OrderedDict
from contextlib import contextmanager

import dask
import docrep
from distributed import LocalCluster
from distributed.deploy import Cluster
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.utils import (
    format_bytes, get_ip_interface, parse_bytes, tmpfile)

logger = logging.getLogger(__name__)
docstrings = docrep.DocstringProcessor()


threads_deprecation_message = """
The threads keyword has been removed and the memory keyword has changed.

Please specify job size with the following keywords:

-  cores: total cores per job, across all processes
-  memory: total memory per job, across all processes
-  processes: number of processes to launch, splitting the quantities above
""".strip()


def _job_id_from_worker_name(name):
    ''' utility to parse the job ID from the worker name

    template: 'prefix--jobid--suffix'
    '''
    _, job_id, _ = name.split('--')
    return job_id


class JobQueuePlugin(SchedulerPlugin):
    def __init__(self):
        self.pending_jobs = OrderedDict()
        self.running_jobs = OrderedDict()
        self.finished_jobs = OrderedDict()
        self.all_workers = {}

    def add_worker(self, scheduler, worker=None, name=None, **kwargs):
        ''' Run when a new worker enters the cluster'''
        logger.debug("adding worker %s" % worker)
        w = scheduler.workers[worker]
        job_id = _job_id_from_worker_name(w.name)
        logger.debug("job id for new worker: %s" % job_id)
        self.all_workers[worker] = (w.name, job_id)

        # if this is the first worker for this job, move job to running
        if job_id not in self.running_jobs:
            logger.debug("this is a new job")
            self.running_jobs[job_id] = self.pending_jobs.pop(job_id)

        # add worker to dict of workers in this job
        self.running_jobs[job_id][w.name] = w

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        ''' Run when a worker leaves the cluster'''
        logger.debug("removing worker %s" % worker)
        name, job_id = self.all_workers[worker]
        logger.debug("removing worker name (%s) and"
                     "job_id (%s)" % (name, job_id))

        # remove worker from this job
        del self.running_jobs[job_id][name]

        # once there are no more workers, move this job to finished_jobs
        if not self.running_jobs[job_id]:
            logger.debug("that was the last worker for job %s" % job_id)
            self.finished_jobs[job_id] = self.running_jobs.pop(job_id)


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
    _adaptive_options = {
        'worker_key': lambda ws: _job_id_from_worker_name(ws.name)}

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

        # Keep information on process, threads and memory, for use in
        # subclasses
        self.worker_memory = parse_bytes(memory) if memory is not None else None
        self.worker_processes = processes
        self.worker_cores = cores
        self.name = name

        # plugin for tracking job status
        self._scheduler_plugin = JobQueuePlugin()
        self.local_cluster.scheduler.add_plugin(self._scheduler_plugin)

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
        self._command_template += " --name %s--${JOB_ID}--" % name

        if death_timeout is not None:
            self._command_template += " --death-timeout %s" % death_timeout
        if local_directory is not None:
            self._command_template += " --local-directory %s" % local_directory
        if extra is not None:
            self._command_template += extra

    @property
    def pending_jobs(self):
        """ Jobs pending in the queue """
        return self._scheduler_plugin.pending_jobs

    @property
    def running_jobs(self):
        """ Jobs with currenly active workers """
        return self._scheduler_plugin.running_jobs

    @property
    def finished_jobs(self):
        """ Jobs that have finished """
        return self._scheduler_plugin.finished_jobs

    @property
    def worker_threads(self):
        return int(self.worker_cores / self.worker_processes)

    def job_script(self):
        """ Construct a job submission script """
        pieces = {'job_header': self.job_header,
                  'env_header': self._env_header,
                  'worker_command': self._command_template}
        return self._script_template % pieces

    @contextmanager
    def job_file(self):
        """ Write job submission script to temporary file """
        with tmpfile(extension='sh') as fn:
            with open(fn, 'w') as f:
                logger.debug("writing job script: \n%s" % self.job_script())
                f.write(self.job_script())
            yield fn

    def _submit_job(self, script_filename):
        return self._call(shlex.split(self.submit_command) + [script_filename])

    def start_workers(self, n=1):
        """ Start workers and point them to our local scheduler """
        logger.debug('starting %s workers' % n)
        num_jobs = math.ceil(n / self.worker_processes)
        for _ in range(num_jobs):
            with self.job_file() as fn:
                out = self._submit_job(fn)
                job = self._job_id_from_submit_output(out.decode())
                logger.debug("started job: %s" % job)
                self.pending_jobs[job] = {}

    @property
    def scheduler(self):
        """ The scheduler of this cluster """
        return self.local_cluster.scheduler

    def _calls(self, cmds, **kwargs):
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
        procs = []
        for cmd in cmds:
            logger.debug(' '.join(cmd))
            procs.append(subprocess.Popen(cmd,
                                          stdout=subprocess.PIPE,
                                          stderr=subprocess.PIPE,
                                          **kwargs))

        result = []
        for proc in procs:
            out, err = proc.communicate()
            if err:
                logger.error(err.decode())
            result.append(out)
        return result

    def _call(self, cmd, **kwargs):
        """ Singular version of _calls """
        return self._calls([cmd], **kwargs)[0]

    def stop_workers(self, workers):
        """ Stop a list of workers"""
        logger.debug("Stopping workers: %s" % workers)
        if not workers:
            return
        jobs = self._stop_pending_jobs()  # stop pending jobs too
        for w in workers:
            if isinstance(w, dict):
                jobs.append(_job_id_from_worker_name(w['name']))
            else:
                jobs.append(_job_id_from_worker_name(w.name))
        self.stop_jobs(set(jobs))

    def stop_jobs(self, jobs):
        """ Stop a list of jobs"""
        logger.debug("Stopping jobs: %s" % jobs)
        if jobs:
            jobs = list(jobs)
            self._call([self.cancel_command] + list(set(jobs)))

    def scale_up(self, n, **kwargs):
        """ Brings total worker count up to ``n`` """
        logger.debug("Scaling up to %d workers." % n)
        active_and_pending = sum([len(j) for j in self.running_jobs.values()])
        active_and_pending += self.worker_processes * len(self.pending_jobs)
        logger.debug("Found %d active/pending workers." % active_and_pending)
        self.start_workers(n - active_and_pending)

    def scale_down(self, workers):
        ''' Close the workers with the given addresses '''
        logger.debug("Scaling down. Workers: %s" % workers)
        worker_states = []
        for w in workers:
            try:
                # Get the actual WorkerState
                worker_states.append(self.scheduler.workers[w])
            except KeyError:
                logger.debug('worker %s is already gone' % w)
        self.stop_workers(worker_states)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        jobs = self._stop_pending_jobs()
        jobs += list(self.running_jobs.keys())
        self.stop_jobs(set(jobs))
        self.local_cluster.__exit__(type, value, traceback)

    def _stop_pending_jobs(self):
        jobs = list(self.pending_jobs.keys())
        logger.debug("Stopping pending jobs %s" % jobs)
        for job_id in jobs:
            del self.pending_jobs[job_id]
        return jobs

    def _job_id_from_submit_output(self, out):
        raise NotImplementedError('_job_id_from_submit_output must be '
                                  'implemented when JobQueueCluster is '
                                  'inherited. It should convert the stdout '
                                  'from submit_command to the job id')
