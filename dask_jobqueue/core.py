from __future__ import absolute_import, division, print_function

import logging
import math
import os
import re
import shlex
import subprocess
import sys
import warnings
from collections import OrderedDict
from contextlib import contextmanager

import six

import dask
import docrep
from distributed import LocalCluster
from distributed.deploy import Cluster
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.utils import format_bytes, parse_bytes, tmpfile, get_ip_interface

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
        logger.debug("adding worker %s", worker)
        w = scheduler.workers[worker]
        job_id = _job_id_from_worker_name(w.name)
        logger.debug("job id for new worker: %s", job_id)
        self.all_workers[worker] = (w.name, job_id)

        # if this is the first worker for this job, move job to running
        if job_id not in self.running_jobs:
            logger.debug("%s is a new job or restarting worker", job_id)
            if job_id in self.pending_jobs:
                logger.debug("%s is a new job, adding to running_jobs", job_id)
                self.running_jobs[job_id] = self.pending_jobs.pop(job_id)
            elif job_id in self.finished_jobs:
                logger.warning('Worker %s restart in Job %s. '
                               'This can be due to memory issue.', w, job_id)
                self.running_jobs[job_id] = self.finished_jobs.pop(job_id)
            else:
                logger.error('Unknown job_id: %s for worker %s', job_id, w)
                self.running_jobs[job_id] = {}

        # add worker to dict of workers in this job
        self.running_jobs[job_id][w.name] = w

    def remove_worker(self, scheduler=None, worker=None, **kwargs):
        ''' Run when a worker leaves the cluster'''
        logger.debug("removing worker %s", worker)
        name, job_id = self.all_workers[worker]
        logger.debug("removing worker name (%s) and job_id (%s)", name, job_id)

        # remove worker from this job
        self.running_jobs[job_id].pop(name, None)

        # once there are no more workers, move this job to finished_jobs
        if not self.running_jobs[job_id]:
            logger.debug("that was the last worker for job %s", job_id)
            self.finished_jobs[job_id] = self.running_jobs.pop(job_id)


@docstrings.get_sectionsf('JobQueueCluster')
class JobQueueCluster(Cluster):
    """ Base class to launch Dask Clusters for Job queues

    This class should not be used directly, use inherited class appropriate for your queueing system (e.g. PBScluster
    or SLURMCluster)

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
    extra : list
        Additional arguments to pass to `dask-worker`
    env_extra : list
        Other commands to add to script before launching worker.
    python : str
        Python executable used to launch Dask workers.
    kwargs : dict
        Additional keyword arguments to pass to `LocalCluster`

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
    """

    _script_template = """
#!/bin/bash

%(job_header)s

%(env_header)s

%(worker_command)s
""".lstrip()

    # Following class attributes should be overridden by extending classes.
    submit_command = None
    cancel_command = None
    scheduler_name = ''
    _adaptive_options = {'worker_key': lambda ws: _job_id_from_worker_name(ws.name)}
    job_id_regexp = r'(?P<job_id>\d+)'

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
                 log_directory=None,
                 walltime=None,
                 threads=None,
                 python=sys.executable,
                 **kwargs
                 ):
        """ """
        # """
        # This initializer should be considered as Abstract, and never used directly.
        # """
        if threads is not None:
            raise ValueError(threads_deprecation_message)

        if not self.scheduler_name:
            raise NotImplementedError('JobQueueCluster is an abstract class that should not be instanciated.')

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
        if log_directory is None:
            log_directory = dask.config.get('jobqueue.%s.log-directory' % self.scheduler_name)

        if dask.config.get('jobqueue.%s.threads', None):
            warnings.warn(threads_deprecation_message)

        if cores is None:
            raise ValueError("You must specify how many cores to use per job like ``cores=8``")

        if memory is None:
            raise ValueError("You must specify how much memory to use per job like ``memory='24 GB'``")

        # This attribute should be overridden
        self.job_header = None

        if interface:
            extra += ['--interface', interface]
            kwargs.setdefault('ip', get_ip_interface(interface))
        else:
            kwargs.setdefault('ip', '')

        # Bokeh diagnostics server should listen on all interfaces
        diagnostics_ip_and_port = ('', 8787)
        self.local_cluster = LocalCluster(n_workers=0, diagnostics_port=diagnostics_ip_and_port,
                                          **kwargs)

        # Keep information on process, cores, and memory, for use in subclasses
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
        dask_worker_command = '%(python)s -m distributed.cli.dask_worker' % dict(python=python)
        command_args = [dask_worker_command, self.scheduler.address]
        command_args += ['--nthreads', self.worker_threads]
        if processes is not None and processes > 1:
            command_args += ['--nprocs', processes]

        mem = format_bytes(self.worker_memory / self.worker_processes)
        command_args += ['--memory-limit', mem.replace(' ', '')]
        command_args += ['--name', '%s--${JOB_ID}--' % name]

        if death_timeout is not None:
            command_args += ['--death-timeout', death_timeout]
        if local_directory is not None:
            command_args += ['--local-directory', local_directory]
        if extra is not None:
            command_args += extra

        self._command_template = ' '.join(map(str, command_args))

        self._target_scale = 0

        self.log_directory = log_directory
        if self.log_directory is not None:
            if not os.path.exists(self.log_directory):
                os.makedirs(self.log_directory)

    def __repr__(self):
        running_workers = self._count_active_workers()
        running_cores = running_workers * self.worker_threads
        total_jobs = len(self.pending_jobs) + len(self.running_jobs)
        total_workers = total_jobs * self.worker_processes
        running_memory = running_workers * self.worker_memory / self.worker_processes

        return (self.__class__.__name__ +
                '(cores=%d, memory=%s, workers=%d/%d, jobs=%d/%d)' %
                (running_cores, format_bytes(running_memory), running_workers,
                 total_workers, len(self.running_jobs), total_jobs)
                )

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
                logger.debug("writing job script: \n%s", self.job_script())
                f.write(self.job_script())
            yield fn

    def _submit_job(self, script_filename):
        return self._call(shlex.split(self.submit_command) + [script_filename])

    def start_workers(self, n=1):
        """ Start workers and point them to our local scheduler """
        logger.debug('starting %s workers', n)
        num_jobs = int(math.ceil(n / self.worker_processes))
        for _ in range(num_jobs):
            with self.job_file() as fn:
                out = self._submit_job(fn)
                job = self._job_id_from_submit_output(out)
                if not job:
                    raise ValueError('Unable to parse jobid from output of %s' % out)
                logger.debug("started job: %s", job)
                self.pending_jobs[job] = {}

    @property
    def scheduler(self):
        """ The scheduler of this cluster """
        return self.local_cluster.scheduler

    def _call(self, cmd, **kwargs):
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
        cmd_str = ' '.join(cmd)
        logger.debug("Executing the following command to command line\n{}".format(cmd_str))

        proc = subprocess.Popen(cmd,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                **kwargs)

        out, err = proc.communicate()
        if six.PY3:
            out, err = out.decode(), err.decode()
        if proc.returncode != 0:
            raise RuntimeError('Command exited with non-zero exit code.\n'
                               'Exit code: {}\n'
                               'Command:\n{}\n'
                               'stdout:\n{}\n'
                               'stderr:\n{}\n'.format(proc.returncode,
                                                      cmd_str, out, err))
        return out

    def stop_workers(self, workers):
        """ Stop a list of workers"""
        logger.debug("Stopping workers: %s", workers)
        if not workers:
            return
        jobs = self._del_pending_jobs()  # stop pending jobs too
        for w in workers:
            if isinstance(w, dict):
                jobs.append(_job_id_from_worker_name(w['name']))
            else:
                jobs.append(_job_id_from_worker_name(w.name))
        self.stop_jobs(jobs)

    def stop_jobs(self, jobs):
        """ Stop a list of jobs"""
        logger.debug("Stopping jobs: %s", jobs)
        if jobs:
            jobs = list(jobs)
            self._call([self.cancel_command] + list(set(jobs)))

        # if any of these jobs were pending, we should remove those now
        for job_id in jobs:
            if job_id in self.pending_jobs:
                del self.pending_jobs[job_id]

    def scale_up(self, n, **kwargs):
        """ Brings total worker count up to ``n`` """
        active_and_pending = self._count_active_and_pending_workers()
        if n >= active_and_pending:
            logger.debug("Scaling up to %d workers.", n)
            self.start_workers(n - self._count_active_and_pending_workers())
        else:
            n_to_close = active_and_pending - n
            if n_to_close < self._count_pending_workers():
                # We only need to kill some pending jobs, this is actually a
                # scale down bu could not be handled upstream
                to_kill = int(n_to_close / self.worker_processes)
                jobs = list(self.pending_jobs.keys())[to_kill:]
                self.stop_jobs(jobs)
            else:
                # We should not end here, a new scale call should not begin
                # until a scale_up or scale_down has ended
                raise RuntimeError('JobQueueCluster.scale_up was called with'
                                   ' a number of worker lower than the '
                                   'currently connected workers')

    def _count_active_and_pending_workers(self):
        active_and_pending = (self._count_active_workers() +
                              self._count_pending_workers())
        logger.debug("Found %d active/pending workers.", active_and_pending)
        assert len(self.scheduler.workers) <= active_and_pending
        return active_and_pending

    def _count_active_workers(self):
        active_workers = sum([len(j) for j in self.running_jobs.values()])
        assert len(self.scheduler.workers) == active_workers
        return active_workers

    def _count_pending_workers(self):
        return self.worker_processes * len(self.pending_jobs)

    def scale_down(self, workers):
        ''' Close the workers with the given addresses '''
        logger.debug("Scaling down. Workers: %s", workers)
        worker_states = []
        for w in workers:
            try:
                # Get the actual WorkerState
                worker_states.append(self.scheduler.workers[w])
            except KeyError:
                logger.debug('worker %s is already gone', w)
        self.stop_workers(worker_states)

    def stop_all_jobs(self):
        ''' Stops all running and pending jobs '''
        jobs = self._del_pending_jobs()
        jobs += list(self.running_jobs.keys())
        self.stop_jobs(set(jobs))

    def close(self):
        ''' Stops all running and pending jobs and stops scheduler '''
        self.stop_all_jobs()
        self.local_cluster.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        self.local_cluster.__exit__(type, value, traceback)

    def _del_pending_jobs(self):
        jobs = list(self.pending_jobs.keys())
        logger.debug("Deleting pending jobs %s" % jobs)
        for job_id in jobs:
            del self.pending_jobs[job_id]
        return jobs

    def _job_id_from_submit_output(self, out):
        match = re.search(self.job_id_regexp, out)
        if match is None:
            msg = ('Could not parse job id from submission command '
                   "output.\nJob id regexp is {!r}\nSubmission command "
                   'output is:\n{}'.format(self.job_id_regexp, out))
            raise ValueError(msg)

        job_id = match.groupdict().get('job_id')
        if job_id is None:
            msg = ("You need to use a 'job_id' named group in your regexp, e.g. "
                   "r'(?P<job_id>\d+)', in your regexp. Your regexp was: "
                   "{!r}".format(self.job_id_regexp))
            raise ValueError(msg)

        return job_id
