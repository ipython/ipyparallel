from contextlib import contextmanager
import logging
import subprocess
import socket
import os
import sys

from distributed.utils import tmpfile, ignoring, get_ip_interface
from distributed import LocalCluster

dirname = os.path.dirname(sys.executable)

logger = logging.getLogger(__name__)


class JobQueueCluster(object):
    """ Base class to launch Dask Clusters for Job queues

    This class should not be used directly, use inherited class appropriate
    for your queueing system (e.g. PBScluster or SLURMCluster)

    Abstract init parameters
    ------------------------
    name : str
        Name of Dask workers.
    threads : int
        Number of threads per process.
    processes : int
        Number of processes per node.
    memory : str
        Bytes of memory that the worker can use. This should be a string
        like "7GB" that can be interpretted both by PBS and Dask.
    interface : str
        Network interface like 'eth0' or 'ib0'.
    death_timeout : float
        Seconds to wait for a scheduler before closing workers
    local_directory : str
        Dask worker local directory for file spilling.
    extra : str
        Additional arguments to pass to `dask-worker`
    kwargs : dict
        Additional keyword arguments to pass to `LocalCluster`

    See Also
    --------
    PBSCluster
    SLURMCluster
    """

    _script_template ="""
#!/bin/bash

%(job_header)s

%(worker_command)s 
""".lstrip()

    def __init__(self,
                 name='dask-worker',
                 threads=4,
                 processes=6,
                 memory='16GB',
                 interface=None,
                 death_timeout=60,
                 local_directory=None,
                 extra='',
                 **kwargs
                 ):
        """
        This initializer should be considered as Abstract, and never used directly.
        :param name:
        :param threads:
        :param processes:
        :param memory:
        :param interface:
        :param death_timeout:
        :param local_directory:
        :param extra:
        :param kwargs:
        """


        if interface:
            host = get_ip_interface(interface)
            extra += ' --interface  %s ' % interface
        else:
            host = socket.gethostname()

        self.cluster = LocalCluster(n_workers=0, ip=host, **kwargs)

        self.jobs = dict()
        self.n = 0
        self._adaptive = None

        self._command_template = "%s/dask-worker %s" % (dirname, self.scheduler.address)
        if threads != None: self._command_template += " --nthreads %d" % threads
        if processes != None: self._command_template += " --nprocs %d" % processes
        if memory != None: self._command_template += " --memory-limit %s" % memory
        if name != None:
            self._command_template += " --name %s" % name
            self._command_template += "-%(n)d" #Keep %(n) to be replaced later.
        if death_timeout != None: self._command_template += " --death-timeout %s" % death_timeout
        if local_directory != None: self._command_template += " --local-directory %s" % local_directory
        if extra!= None: self._command_template += extra

    def job_script(self):
        self.n += 1
        return self._script_template % {'job_header': self.job_header,
                                        'worker_command': self._command_template % {'n': self.n}
                                        }

    @property
    def job_header(self):
        """
        Abstract attribute for the Job scheduler script header part.
        :return: A string representation of the Job script header part.
        """
        raise NotImplementedError

    @property
    def submitcmd(self):
        """
        Abstract attribute for job scheduler submission command
        :return:
        """
        raise NotImplementedError

    @property
    def cancelcmd(self):
        """
        Abstract attribute for job scheduler cancel command
        :return:
        """
        raise NotImplementedError

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
                out = self._call([self.submitcmd, fn])
                job = out.decode().split('.')[0]
                self.jobs[self.n] = job
                workers.append(self.n)
        return workers

    @property
    def scheduler(self):
        return self.cluster.scheduler

    @property
    def scheduler_address(self):
        return self.cluster.scheduler_address

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
        if not workers:
            return
        workers = list(map(int, workers))
        jobs = [self.jobs[w] for w in workers]
        self._call([self.cancelcmd] + list(jobs))
        for w in workers:
            with ignoring(KeyError):
                del self.jobs[w]

    def scale_up(self, n, **kwargs):
        return self.start_workers(n - len(self.jobs))

    def scale_down(self, workers):
        if isinstance(workers, dict):
            names = {v['name'] for v in workers.values()}
            job_ids = {name.split('-')[-2] for name in names}
            self.stop_workers(job_ids)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.stop_workers(self.jobs)
        self.cluster.__exit__(type, value, traceback)

    def adapt(self):
        """ Start up an Adaptive deployment if not already started

        This makes the cluster request resources in accordance to current
        demand on the scheduler """
        from distributed.deploy import Adaptive
        if self._adaptive:
            return
        else:
            self._adaptive = Adaptive(self.scheduler, self, startup_cost=5,
                                      key=lambda ws: ws.host)
