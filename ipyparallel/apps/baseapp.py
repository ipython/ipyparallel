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
from jupyter_client.session import Session
from tornado.ioloop import IOLoop
from traitlets import Bool
from traitlets import default
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
        os.getcwd(), config=True, help='Set the working dir for the process.'
    )

    @observe('work_dir')
    def _work_dir_changed(self, change):
        self.work_dir = str(expand_path(change['new']))

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

    loop = Instance(IOLoop)

    def _loop_default(self):
        return IOLoop.current()

    session = Instance(Session)

    @default("session")
    def _default_session(self):
        return Session(parent=self)

    aliases = base_aliases
    flags = base_flags

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
        if wd != os.getcwd():
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
            log_filename = f"{self.name}-{self.cluster_id}-{os.getpid()}.log"
            logfile = os.path.join(log_dir, log_filename)
            if sys.__stderr__:
                print(f"Sending logs to {logfile}", file=sys.__stderr__)
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
