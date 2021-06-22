# encoding: utf-8
"""
The Base Application class for ipyparallel apps
"""
import logging
import os
import re
import sys

from IPython.core.application import base_aliases as base_ip_aliases
from IPython.core.application import base_flags as base_ip_flags
from IPython.core.application import BaseIPythonApplication
from IPython.utils.path import expand_path
from IPython.utils.process import check_pid
from ipython_genutils import py3compat
from ipython_genutils.py3compat import unicode_type
from jupyter_client.session import Session
from tornado.ioloop import IOLoop
from traitlets import Bool
from traitlets import default
from traitlets import Dict
from traitlets import Instance
from traitlets import observe
from traitlets import Unicode
from traitlets.config.application import catch_config_error
from traitlets.config.application import LevelFormatter

from .._version import __version__

# -----------------------------------------------------------------------------
# Module errors
# -----------------------------------------------------------------------------


class PIDFileError(Exception):
    pass


# -----------------------------------------------------------------------------
# Main application
# -----------------------------------------------------------------------------
base_aliases = {}
base_aliases.update(base_ip_aliases)
base_aliases.update(
    {
        'work-dir': 'BaseParallelApplication.work_dir',
        'log-to-file': 'BaseParallelApplication.log_to_file',
        'clean-logs': 'BaseParallelApplication.clean_logs',
        'log-url': 'BaseParallelApplication.log_url',
        'cluster-id': 'BaseParallelApplication.cluster_id',
    }
)

base_flags = {
    'log-to-file': (
        {'BaseParallelApplication': {'log_to_file': True}},
        "send log output to a file",
    )
}
base_flags.update(base_ip_flags)


class BaseParallelApplication(BaseIPythonApplication):
    """The base Application for ipyparallel apps

    Primary extensions to BaseIPythonApplication:

    * work_dir
    * remote logging via pyzmq
    * IOLoop instance
    """

    version = __version__

    _deprecated_classes = None

    def init_crash_handler(self):
        # disable crash handler from IPython
        pass

    def _log_level_default(self):
        # temporarily override default_log_level to INFO
        return logging.INFO

    def _log_format_default(self):
        """override default log format to include time"""
        return u"%(asctime)s.%(msecs).03d [%(name)s]%(highlevel)s %(message)s"

    work_dir = Unicode(
        py3compat.getcwd(), config=True, help='Set the working dir for the process.'
    )

    @observe('work_dir')
    def _work_dir_changed(self, change):
        self.work_dir = unicode_type(expand_path(change['new']))

    log_to_file = Bool(config=True, help="whether to log to a file")

    clean_logs = Bool(
        False, config=True, help="whether to cleanup old logfiles before starting"
    )

    log_url = Unicode(
        '', config=True, help="The ZMQ URL of the iplogger to aggregate logging."
    )

    cluster_id = Unicode(
        '',
        config=True,
        help="""String id to add to runtime files, to prevent name collisions when
        using multiple clusters with a single profile simultaneously.

        When set, files will be named like: 'ipcontroller-<cluster_id>-engine.json'

        Since this is text inserted into filenames, typical recommendations apply:
        Simple character strings are ideal, and spaces are not recommended (but should
        generally work).
        """,
    )

    @observe('cluster_id')
    def _cluster_id_changed(self, change):
        if change['new']:
            self.name = '{}-{}'.format(self.__class__.name, change['new'])
        else:
            self.name = self.__class__.name

    def _config_files_default(self):
        return ['ipcontroller_config.py', 'ipengine_config.py', 'ipcluster_config.py']

    loop = Instance(IOLoop)

    def _loop_default(self):
        return IOLoop.current()

    session = Instance(Session)

    @default("session")
    def _default_session(self):
        return Session(parent=self)

    aliases = Dict(base_aliases)
    flags = Dict(base_flags)

    @catch_config_error
    def initialize(self, argv=None):
        """initialize the app"""
        super(BaseParallelApplication, self).initialize(argv)
        self.init_deprecated_config()
        self.to_work_dir()
        self.reinit_logging()

    def init_deprecated_config(self):
        if not self._deprecated_classes:
            return
        deprecated_config_found = False
        new_classname = self.__class__.__name__
        for deprecated_classname in self._deprecated_classes:
            if deprecated_classname in self.config:
                cfg = self.config[deprecated_classname]
                new_config = self.config[new_classname]
                for key, deprecated_value in list(cfg.items()):
                    if key in new_config:
                        new_value = new_config[key]
                        if new_value != deprecated_value:
                            self.log.warning(
                                f"Ignoring c.{deprecated_classname}.{key} = {deprecated_value}, overridden by c.{new_classname}.{key} = {new_value}"
                            )
                    else:
                        self.log.warning(
                            f"c.{deprecated_classname}.{key} is deprecated in ipyparallel 7, use c.{new_classname}.{key} = {deprecated_value}"
                        )
                        new_config[key] = deprecated_value
                        cfg.pop(key)
                        deprecated_config_found = True

        if deprecated_config_found:
            # reload config
            self.update_config(self.config)

    def to_work_dir(self):
        wd = self.work_dir
        if unicode_type(wd) != py3compat.getcwd():
            os.chdir(wd)
            self.log.info("Changing to working dir: %s" % wd)
        # This is the working dir by now.
        sys.path.insert(0, '')

    def reinit_logging(self):
        # Remove old log files
        log_dir = self.profile_dir.log_dir
        if self.clean_logs:
            for f in os.listdir(log_dir):
                if re.match(r'%s-\d+\.(log|err|out)' % self.name, f):
                    try:
                        os.remove(os.path.join(log_dir, f))
                    except (OSError, IOError):
                        # probably just conflict from sibling process
                        # already removing it
                        pass
        if self.log_to_file:
            # Start logging to the new log file
            log_filename = self.name + u'-' + str(os.getpid()) + u'.log'
            logfile = os.path.join(log_dir, log_filename)
            open_log_file = open(logfile, 'w')
        else:
            open_log_file = None
        if open_log_file is not None:
            while self.log.handlers:
                self.log.removeHandler(self.log.handlers[0])
            self._log_handler = logging.StreamHandler(open_log_file)
            self.log.addHandler(self._log_handler)
        else:
            self._log_handler = self.log.handlers[0]
        # Add timestamps to log format:
        self._log_formatter = LevelFormatter(self.log_format, datefmt=self.log_datefmt)
        self._log_handler.setFormatter(self._log_formatter)
        # do not propagate log messages to root logger
        # ipcluster app will sometimes print duplicate messages during shutdown
        # if this is 1 (default):
        self.log.propagate = False

    def write_pid_file(self, overwrite=False):
        """Create a .pid file in the pid_dir with my pid.

        This must be called after pre_construct, which sets `self.pid_dir`.
        This raises :exc:`PIDFileError` if the pid file exists already.
        """
        pid_file = os.path.join(self.profile_dir.pid_dir, self.name + u'.pid')
        if os.path.isfile(pid_file):
            pid = self.get_pid_from_file()
            if not overwrite:
                raise PIDFileError(
                    'The pid file [%s] already exists. \nThis could mean that this '
                    'server is already running with [pid=%s].' % (pid_file, pid)
                )
        with open(pid_file, 'w') as f:
            self.log.info("Creating pid file: %s" % pid_file)
            f.write(repr(os.getpid()) + '\n')

    def remove_pid_file(self):
        """Remove the pid file.

        This should be called at shutdown by registering a callback with
        :func:`reactor.addSystemEventTrigger`. This needs to return
        ``None``.
        """
        pid_file = os.path.join(self.profile_dir.pid_dir, self.name + u'.pid')
        if os.path.isfile(pid_file):
            try:
                self.log.info("Removing pid file: %s" % pid_file)
                os.remove(pid_file)
            except:
                self.log.warn("Error removing the pid file: %s" % pid_file)

    def get_pid_from_file(self):
        """Get the pid from the pid file.

        If the  pid file doesn't exist a :exc:`PIDFileError` is raised.
        """
        pid_file = os.path.join(self.profile_dir.pid_dir, self.name + u'.pid')
        if os.path.isfile(pid_file):
            with open(pid_file, 'r') as f:
                s = f.read().strip()
                try:
                    pid = int(s)
                except:
                    raise PIDFileError(
                        "invalid pid file: %s (contents: %r)" % (pid_file, s)
                    )
                return pid
        else:
            raise PIDFileError('pid file not found: %s' % pid_file)

    def check_pid(self, pid):
        try:
            return check_pid(pid)
        except Exception:
            self.log.warn(
                "Could not determine whether pid %i is running. "
                " Making the likely assumption that it is." % pid
            )
            return True
