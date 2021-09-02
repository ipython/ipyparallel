#!/usr/bin/env python
# encoding: utf-8
"""
The IPython engine application
"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import json
import os
import signal
import sys
import time
from getpass import getpass
from io import FileIO
from io import TextIOWrapper
from logging import StreamHandler

import zmq
from ipykernel.kernelapp import IPKernelApp
from ipykernel.zmqshell import ZMQInteractiveShell
from IPython.core.profiledir import ProfileDir
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from jupyter_client.session import session_aliases
from jupyter_client.session import session_flags
from tornado import ioloop
from traitlets import Bool
from traitlets import Bytes
from traitlets import default
from traitlets import Dict
from traitlets import Float
from traitlets import Instance
from traitlets import Integer
from traitlets import List
from traitlets import observe
from traitlets import Type
from traitlets import Unicode
from traitlets.config import Config
from zmq.eventloop import zmqstream

from .kernel import IPythonParallelKernel as Kernel
from .log import EnginePUBHandler
from .nanny import start_nanny
from ipyparallel.apps.baseapp import base_aliases
from ipyparallel.apps.baseapp import base_flags
from ipyparallel.apps.baseapp import BaseParallelApplication
from ipyparallel.apps.baseapp import catch_config_error
from ipyparallel.controller.heartmonitor import Heart
from ipyparallel.util import disambiguate_ip_address
from ipyparallel.util import disambiguate_url

# -----------------------------------------------------------------------------
# Module level variables
# -----------------------------------------------------------------------------

_description = """Start an IPython engine for parallel computing.

IPython engines run in parallel and perform computations on behalf of a client
and controller. A controller needs to be started before the engines. The
engine can be configured using command line options or using a cluster
directory. Cluster directories contain config, log and security files and are
usually located in your ipython directory and named as "profile_name".
See the `profile` and `profile-dir` options for details.
"""

_examples = """
ipengine --file=path/to/ipcontroller-engine.json     # connect to hub using a specific url
ipengine --log-level=DEBUG > engine.log 2>&1  # log to a file with DEBUG verbosity
"""

DEFAULT_MPI_INIT = """
from mpi4py import MPI
mpi_rank = MPI.COMM_WORLD.Get_rank()
mpi_size = MPI.COMM_WORLD.Get_size()
"""

# -----------------------------------------------------------------------------
# Main application
# -----------------------------------------------------------------------------
aliases = dict(
    file='IPEngine.url_file',
    c='IPEngine.startup_command',
    s='IPEngine.startup_script',
    url='IPEngine.registration_url',
    ssh='IPEngine.sshserver',
    sshkey='IPEngine.sshkey',
    location='IPEngine.location',
    timeout='IPEngine.timeout',
)
aliases.update(base_aliases)
aliases.update(session_aliases)
flags = {
    'mpi': (
        {
            'IPEngine': {'use_mpi': True},
        },
        "enable MPI integration",
    ),
}
flags.update(base_flags)
flags.update(session_flags)


class IPEngine(BaseParallelApplication):

    name = 'ipengine'
    description = _description
    examples = _examples
    classes = List([ZMQInteractiveShell, ProfileDir, Session, Kernel])
    _deprecated_classes = ["EngineFactory", "IPEngineApp"]

    enable_nanny = Bool(
        True,
        config=True,
        help="""Enable the nanny process.

    The nanny process enables remote signaling of single engines
    and more responsive notification of engine shutdown.

    .. versionadded:: 7.0

    """,
    )

    startup_script = Unicode(
        u'', config=True, help='specify a script to be run at startup'
    )
    startup_command = Unicode(
        '', config=True, help='specify a command to be run at startup'
    )

    url_file = Unicode(
        u'',
        config=True,
        help="""The full location of the file containing the connection information for
        the controller. If this is not given, the file must be in the
        security directory of the cluster directory.  This location is
        resolved using the `profile` or `profile_dir` options.""",
    )
    wait_for_url_file = Float(
        10,
        config=True,
        help="""The maximum number of seconds to wait for url_file to exist.
        This is useful for batch-systems and shared-filesystems where the
        controller and engine are started at the same time and it
        may take a moment for the controller to write the connector files.""",
    )

    url_file_name = Unicode(u'ipcontroller-engine.json', config=True)

    @observe('cluster_id')
    def _cluster_id_changed(self, change):
        if change['new']:
            base = 'ipcontroller-{}'.format(change['new'])
        else:
            base = 'ipcontroller'
        self.url_file_name = "%s-engine.json" % base

    log_url = Unicode(
        '',
        config=True,
        help="""The URL for the iploggerapp instance, for forwarding
        logging to a central location.""",
    )

    registration_url = Unicode(
        config=True,
        help="""Override the registration URL""",
    )
    out_stream_factory = Type(
        'ipykernel.iostream.OutStream',
        config=True,
        help="""The OutStream for handling stdout/err.
        Typically 'ipykernel.iostream.OutStream'""",
    )
    display_hook_factory = Type(
        'ipykernel.displayhook.ZMQDisplayHook',
        config=True,
        help="""The class for handling displayhook.
        Typically 'ipykernel.displayhook.ZMQDisplayHook'""",
    )
    location = Unicode(
        config=True,
        help="""The location (an IP address) of the controller.  This is
        used for disambiguating URLs, to determine whether
        loopback should be used to connect or the public address.""",
    )
    timeout = Float(
        5.0,
        config=True,
        help="""The time (in seconds) to wait for the Controller to respond
        to registration requests before giving up.""",
    )
    max_heartbeat_misses = Integer(
        50,
        config=True,
        help="""The maximum number of times a check for the heartbeat ping of a
        controller can be missed before shutting down the engine.

        If set to 0, the check is disabled.""",
    )
    sshserver = Unicode(
        config=True,
        help="""The SSH server to use for tunneling connections to the Controller.""",
    )
    sshkey = Unicode(
        config=True,
        help="""The SSH private key file to use when tunneling connections to the Controller.""",
    )
    paramiko = Bool(
        sys.platform == 'win32',
        config=True,
        help="""Whether to use paramiko instead of openssh for tunnels.""",
    )

    use_mpi = Bool(
        False,
        config=True,
        help="""Enable MPI integration.

        If set, MPI rank will be requested for my rank,
        and additionally `mpi_init` will be executed in the interactive shell.
        """,
    )
    init_mpi = Unicode(
        DEFAULT_MPI_INIT,
        config=True,
        help="""Code to execute in the user namespace when initializing MPI""",
    )
    mpi_registration_delay = Float(
        0.02,
        config=True,
        help="""Per-engine delay for mpiexec-launched engines

        avoids flooding the controller with registrations,
        which can stall under heavy load.

        Default: .02 (50 engines/sec, or 3000 engines/minute)
        """,
    )

    # not configurable:
    user_ns = Dict()
    id = Integer(
        None,
        allow_none=True,
        config=True,
        help="""Request this engine ID.

        If run in MPI, will use the MPI rank.
        Otherwise, let the Hub decide what our rank should be.
        """,
    )

    @default('id')
    def _id_default(self):
        if not self.use_mpi:
            return None
        from mpi4py import MPI

        if MPI.COMM_WORLD.size > 1:
            self.log.debug("MPI rank = %i", MPI.COMM_WORLD.rank)
            return MPI.COMM_WORLD.rank

    registrar = Instance('zmq.eventloop.zmqstream.ZMQStream', allow_none=True)
    kernel = Instance(Kernel, allow_none=True)
    hb_check_period = Integer()

    # States for the heartbeat monitoring
    # Initial values for monitored and pinged must satisfy "monitored > pinged == False" so that
    # during the first check no "missed" ping is reported. Must be floats for Python 3 compatibility.
    _hb_last_pinged = 0.0
    _hb_last_monitored = 0.0
    _hb_missed_beats = 0
    # The zmq Stream which receives the pings from the Heart
    _hb_listener = None

    bident = Bytes()
    ident = Unicode()

    @default("ident")
    def _default_ident(self):
        return self.session.session

    @default("bident")
    def _default_bident(self):
        return self.ident.encode("utf8")

    @observe("ident")
    def _ident_changed(self, change):
        self.bident = self._default_bident()

    using_ssh = Bool(False)

    context = Instance(zmq.Context)

    @default("context")
    def _default_context(self):
        return zmq.Context.instance()

    # an IPKernelApp instance, used to setup listening for shell frontends
    kernel_app = Instance(IPKernelApp, allow_none=True)

    aliases = Dict(aliases)
    flags = Dict(flags)

    def find_url_file(self):
        """Set the url file.

        Here we don't try to actually see if it exists for is valid as that
        is hadled by the connection logic.
        """
        # Find the actual controller key file
        if not self.url_file:
            self.url_file = os.path.join(
                self.profile_dir.security_dir, self.url_file_name
            )

    def load_connector_file(self):
        """load config from a JSON connector file,
        at a *lower* priority than command-line/config files.
        """

        self.log.info("Loading connection file %r", self.url_file)
        config = self.config

        with open(self.url_file) as f:
            d = json.load(f)

        # allow hand-override of location for disambiguation
        # and ssh-server
        if 'IPEngine.location' not in self.cli_config:
            self.location = d['location']
        if 'ssh' in d and not self.sshserver:
            self.sshserver = d.get("ssh")

        proto, ip = d['interface'].split('://')
        ip = disambiguate_ip_address(ip, self.location)
        d['interface'] = '%s://%s' % (proto, ip)

        # DO NOT allow override of basic URLs, serialization, or key
        # JSON file takes top priority there
        config.Session.key = d['key'].encode('utf8')
        config.Session.signature_scheme = d['signature_scheme']

        self.registration_url = d['interface'] + ':%i' % d['registration']

        config.Session.packer = d['pack']
        config.Session.unpacker = d['unpack']
        self.session = Session(parent=self)

        self.log.debug("Config changed:")
        self.log.debug("%r", config)
        self.connection_info = d

    def bind_kernel(self, **kwargs):
        """Promote engine to listening kernel, accessible to frontends."""
        if self.kernel_app is not None:
            return

        self.log.info("Opening ports for direct connections as an IPython kernel")

        kernel = self.kernel

        kwargs.setdefault('config', self.config)
        kwargs.setdefault('log', self.log)
        kwargs.setdefault('profile_dir', self.profile_dir)
        kwargs.setdefault('session', self.session)

        app = self.kernel_app = IPKernelApp(**kwargs)

        # allow IPKernelApp.instance():
        IPKernelApp._instance = app

        app.init_connection_file()
        # relevant contents of init_sockets:

        app.shell_port = app._bind_socket(kernel.shell_streams[0], app.shell_port)
        app.log.debug("shell ROUTER Channel on port: %i", app.shell_port)

        iopub_socket = kernel.iopub_socket
        # ipykernel 4.3 iopub_socket is an IOThread wrapper:
        if hasattr(iopub_socket, 'socket'):
            iopub_socket = iopub_socket.socket

        app.iopub_port = app._bind_socket(iopub_socket, app.iopub_port)
        app.log.debug("iopub PUB Channel on port: %i", app.iopub_port)

        kernel.stdin_socket = self.context.socket(zmq.ROUTER)
        app.stdin_port = app._bind_socket(kernel.stdin_socket, app.stdin_port)
        app.log.debug("stdin ROUTER Channel on port: %i", app.stdin_port)

        # start the heartbeat, and log connection info:

        app.init_heartbeat()

        app.log_connection_info()
        app.connection_dir = self.profile_dir.security_dir
        app.write_connection_file()

    @property
    def tunnel_mod(self):
        from zmq.ssh import tunnel

        return tunnel

    def init_connector(self):
        """construct connection function, which handles tunnels."""
        self.using_ssh = bool(self.sshkey or self.sshserver)

        if self.sshkey and not self.sshserver:
            # We are using ssh directly to the controller, tunneling localhost to localhost
            self.sshserver = self.registration_url.split('://')[1].split(':')[0]

        if self.using_ssh:
            if self.tunnel_mod.try_passwordless_ssh(
                self.sshserver, self.sshkey, self.paramiko
            ):
                password = False
            else:
                password = getpass("SSH Password for %s: " % self.sshserver)
        else:
            password = False

        def connect(s, url):
            url = disambiguate_url(url, self.location)
            if self.using_ssh:
                self.log.debug("Tunneling connection to %s via %s", url, self.sshserver)
                return self.tunnel_mod.tunnel_connection(
                    s,
                    url,
                    self.sshserver,
                    keyfile=self.sshkey,
                    paramiko=self.paramiko,
                    password=password,
                )
            else:
                return s.connect(url)

        def maybe_tunnel(url):
            """like connect, but don't complete the connection (for use by heartbeat)"""
            url = disambiguate_url(url, self.location)
            if self.using_ssh:
                self.log.debug("Tunneling connection to %s via %s", url, self.sshserver)
                url, tunnelobj = self.tunnel_mod.open_tunnel(
                    url,
                    self.sshserver,
                    keyfile=self.sshkey,
                    paramiko=self.paramiko,
                    password=password,
                )
            return str(url)

        return connect, maybe_tunnel

    def register(self):
        """send the registration_request"""
        if self.use_mpi and self.id and self.id >= 100 and self.mpi_registration_delay:
            # Some launchres implement delay at the Launcher level,
            # but mpiexec must implement it int he engine process itself
            # delay based on our rank

            delay = self.id * self.mpi_registration_delay
            self.log.info(
                f"Delaying registration for {self.id} by {int(delay * 1000)}ms"
            )
            time.sleep(delay)

        self.log.info("Registering with controller at %s" % self.registration_url)
        ctx = self.context
        connect, maybe_tunnel = self.init_connector()
        reg = ctx.socket(zmq.DEALER)
        reg.setsockopt(zmq.IDENTITY, self.bident)
        connect(reg, self.registration_url)

        self.registrar = zmqstream.ZMQStream(reg, self.loop)

        content = dict(uuid=self.ident)
        if self.id is not None:
            self.log.info("Requesting id: %i", self.id)
            content['id'] = self.id
        self._registration_completed = False
        self.registrar.on_recv(
            lambda msg: self.complete_registration(msg, connect, maybe_tunnel)
        )

        self.session.send(self.registrar, "registration_request", content=content)

    def _report_ping(self, msg):
        """Callback for when the heartmonitor.Heart receives a ping"""
        # self.log.debug("Received a ping: %s", msg)
        self._hb_last_pinged = time.time()

    def complete_registration(self, msg, connect, maybe_tunnel):
        try:
            self._complete_registration(msg, connect, maybe_tunnel)
        except Exception as e:
            self.log.critical(f"Error completing registration: {e}", exc_info=True)
            self.exit(255)

    def _complete_registration(self, msg, connect, maybe_tunnel):
        ctx = self.context
        loop = self.loop
        identity = self.bident
        idents, msg = self.session.feed_identities(msg)
        msg = self.session.deserialize(msg)
        content = msg['content']
        info = self.connection_info

        def url(key):
            """get zmq url for given channel"""
            return str(info["interface"] + ":%i" % info[key])

        def urls(key):
            return [f'{info["interface"]}:{port}' for port in info[key]]

        if content['status'] == 'ok':
            requested_id = self.id
            self.id = content['id']
            if requested_id is not None and self.id != requested_id:
                self.log.warning(
                    f"Did not get the requested id: {self.id} != {requested_id}"
                )
                self.log.name = self.log.name.rsplit(".", 1)[0] + f".{self.id}"
            elif self.id is None:
                self.log.name += f".{self.id}"

            # create Shell Connections (MUX, Task, etc.):

            # select which broadcast endpoint to connect to
            # use rank % len(broadcast_leaves)
            broadcast_urls = urls('broadcast')
            broadcast_leaves = len(broadcast_urls)
            broadcast_index = self.id % len(broadcast_urls)
            broadcast_url = broadcast_urls[broadcast_index]

            shell_addrs = [url('mux'), url('task'), broadcast_url]
            self.log.info(f'Shell_addrs: {shell_addrs}')

            # Use only one shell stream for mux and tasks
            stream = zmqstream.ZMQStream(ctx.socket(zmq.ROUTER), loop)
            stream.setsockopt(zmq.IDENTITY, identity)
            # TODO: enable PROBE_ROUTER when schedulers can handle the empty message
            # stream.setsockopt(zmq.PROBE_ROUTER, 1)
            self.log.debug("Setting shell identity %r", identity)

            shell_streams = [stream]
            for addr in shell_addrs:
                self.log.info("Connecting shell to %s", addr)
                connect(stream, addr)

            # control stream:
            control_url = url('control')
            if self.enable_nanny:
                nanny_url, self.nanny_pipe = self.start_nanny(
                    control_url=control_url,
                )
                control_url = nanny_url
            control_stream = zmqstream.ZMQStream(ctx.socket(zmq.ROUTER), loop)
            control_stream.setsockopt(zmq.IDENTITY, identity)
            connect(control_stream, control_url)

            # create iopub stream:
            iopub_addr = url('iopub')
            iopub_socket = ctx.socket(zmq.PUB)
            iopub_socket.setsockopt(zmq.IDENTITY, identity)
            connect(iopub_socket, iopub_addr)
            try:
                from ipykernel.iostream import IOPubThread
            except ImportError:
                pass
            else:
                iopub_socket = IOPubThread(iopub_socket)
                iopub_socket.start()

            # disable history:
            self.config.HistoryManager.hist_file = ':memory:'

            # Redirect input streams and set a display hook.
            if self.out_stream_factory:
                sys.stdout = self.out_stream_factory(
                    self.session, iopub_socket, u'stdout'
                )
                sys.stdout.topic = f"engine.{self.id}.stdout".encode("ascii")
                sys.stderr = self.out_stream_factory(
                    self.session, iopub_socket, u'stderr'
                )
                sys.stderr.topic = f"engine.{self.id}.stderr".encode("ascii")

                # copied from ipykernel 6, which captures sys.__stderr__ at the FD-level
                if getattr(sys.stderr, "_original_stdstream_copy", None) is not None:
                    for handler in self.log.handlers:
                        if isinstance(handler, StreamHandler) and (
                            handler.stream.buffer.fileno() == 2
                        ):
                            self.log.debug(
                                "Seeing logger to stderr, rerouting to raw filedescriptor."
                            )

                            handler.stream = TextIOWrapper(
                                FileIO(sys.stderr._original_stdstream_copy, "w")
                            )
            if self.display_hook_factory:
                sys.displayhook = self.display_hook_factory(self.session, iopub_socket)
                sys.displayhook.topic = f"engine.{self.id}.execute_result".encode(
                    "ascii"
                )

            self.kernel = Kernel(
                parent=self,
                engine_id=self.id,
                ident=self.ident,
                session=self.session,
                control_stream=control_stream,
                shell_streams=shell_streams,
                iopub_socket=iopub_socket,
                loop=loop,
                user_ns=self.user_ns,
                log=self.log,
            )

            self.kernel.shell.display_pub.topic = f"engine.{self.id}.displaypub".encode(
                "ascii"
            )

            # FIXME: This is a hack until IPKernelApp and IPEngineApp can be fully merged
            self.init_signal()
            app = IPKernelApp(
                parent=self, shell=self.kernel.shell, kernel=self.kernel, log=self.log
            )
            if self.use_mpi and self.init_mpi:
                app.exec_lines.insert(0, self.init_mpi)
            app.init_profile_dir()
            app.init_code()

            self.kernel.start()
        else:
            self.log.fatal("Registration Failed: %s" % msg)
            raise Exception("Registration Failed: %s" % msg)

        self.start_heartbeat(
            maybe_tunnel(url('hb_ping')),
            maybe_tunnel(url('hb_pong')),
            content['hb_period'],
            identity,
        )
        self.log.info("Completed registration with id %i" % self.id)
        self.loop.remove_timeout(self._abort_timeout)

    def start_nanny(self, control_url):
        self.log.info("Starting nanny")
        config = Config()
        config.Session = self.config.Session
        return start_nanny(
            engine_id=self.id,
            identity=self.bident,
            control_url=control_url,
            registration_url=self.registration_url,
            config=config,
        )

    def start_heartbeat(self, hb_ping, hb_pong, hb_period, identity):
        """Start our heart beating"""
        self.log.info("Starting heartbeat")

        hb_monitor = None
        if self.max_heartbeat_misses > 0:
            # Add a monitor socket which will record the last time a ping was seen
            mon = self.context.socket(zmq.SUB)
            mport = mon.bind_to_random_port('tcp://%s' % localhost())
            mon.setsockopt(zmq.SUBSCRIBE, b"")
            self._hb_listener = zmqstream.ZMQStream(mon, self.loop)
            self._hb_listener.on_recv(self._report_ping)

            hb_monitor = "tcp://%s:%i" % (localhost(), mport)

        heart = Heart(hb_ping, hb_pong, hb_monitor, heart_id=identity)
        heart.start()

        # periodically check the heartbeat pings of the controller
        # Should be started here and not in "start()" so that the right period can be taken
        # from the hubs HeartBeatMonitor.period
        if self.max_heartbeat_misses > 0:
            # Use a slightly bigger check period than the hub signal period to not warn unnecessary
            self.hb_check_period = hb_period + 500
            self.log.info(
                "Starting to monitor the heartbeat signal from the hub every %i ms.",
                self.hb_check_period,
            )
            self._hb_reporter = ioloop.PeriodicCallback(
                self._hb_monitor, self.hb_check_period
            )
            self._hb_reporter.start()
        else:
            self.log.info(
                "Monitoring of the heartbeat signal from the hub is not enabled."
            )

    def abort(self):
        self.log.fatal("Registration timed out after %.1f seconds" % self.timeout)
        if "127." in self.registration_url:
            self.log.fatal(
                """
            If the controller and engines are not on the same machine,
            you will have to instruct the controller to listen on an external IP (in ipcontroller_config.py):
                c.IPController.ip = '0.0.0.0' # for all interfaces, internal and external
                c.IPController.ip = '192.168.1.101' # or any interface that the engines can see
            or tunnel connections via ssh.
            """
            )
        self.session.send(
            self.registrar, "unregistration_request", content=dict(id=self.id)
        )
        time.sleep(1)
        sys.exit(255)

    def _hb_monitor(self):
        """Callback to monitor the heartbeat from the controller"""
        self._hb_listener.flush()
        if self._hb_last_monitored > self._hb_last_pinged:
            self._hb_missed_beats += 1
            self.log.warning(
                "No heartbeat in the last %s ms (%s time(s) in a row).",
                self.hb_check_period,
                self._hb_missed_beats,
            )
        else:
            # self.log.debug("Heartbeat received (after missing %s beats).", self._hb_missed_beats)
            self._hb_missed_beats = 0

        if self._hb_missed_beats >= self.max_heartbeat_misses:
            self.log.fatal(
                "Maximum number of heartbeats misses reached (%s times %s ms), shutting down.",
                self.max_heartbeat_misses,
                self.hb_check_period,
            )
            self.session.send(
                self.registrar, "unregistration_request", content=dict(id=self.id)
            )
            self.loop.stop()

        self._hb_last_monitored = time.time()

    def init_engine(self):
        # This is the working dir by now.
        sys.path.insert(0, '')
        config = self.config
        # print config
        self.find_url_file()

        if self.wait_for_url_file and not os.path.exists(self.url_file):
            self.log.warning("url_file %r not found", self.url_file)
            self.log.warning(
                "Waiting up to %.1f seconds for it to arrive.", self.wait_for_url_file
            )
            tic = time.time()
            while not os.path.exists(self.url_file) and (
                time.time() - tic < self.wait_for_url_file
            ):
                # wait for url_file to exist, or until time limit
                time.sleep(0.1)

        if os.path.exists(self.url_file):
            self.load_connector_file()
        else:
            self.log.fatal("Fatal: url file never arrived: %s", self.url_file)
            self.exit(1)

        exec_lines = []
        for app in ('IPKernelApp', 'InteractiveShellApp'):
            if '%s.exec_lines' % app in config:
                exec_lines = config[app].exec_lines
                break

        exec_files = []
        for app in ('IPKernelApp', 'InteractiveShellApp'):
            if '%s.exec_files' % app in config:
                exec_files = config[app].exec_files
                break

        config.IPKernelApp.exec_lines = exec_lines
        config.IPKernelApp.exec_files = exec_files

        if self.startup_script:
            exec_files.append(self.startup_script)
        if self.startup_command:
            exec_lines.append(self.startup_command)

    def forward_logging(self):
        if self.log_url:
            self.log.info("Forwarding logging to %s", self.log_url)
            context = self.context
            lsock = context.socket(zmq.PUB)
            lsock.connect(self.log_url)
            handler = EnginePUBHandler(self.engine, lsock)
            handler.setLevel(self.log_level)
            self.log.addHandler(handler)

    @catch_config_error
    def initialize(self, argv=None):
        super().initialize(argv)
        self.init_engine()
        self.forward_logging()

    def init_signal(self):
        signal.signal(signal.SIGINT, self._signal_sigint)
        signal.signal(signal.SIGTERM, self._signal_stop)

    def _signal_sigint(self, sig, frame):
        self.log.warning("Ignoring SIGINT. Terminate with SIGTERM.")

    def _signal_stop(self, sig, frame):
        self.log.critical(f"received signal {sig}, stopping")
        self.loop.add_callback_from_signal(self.loop.stop)

    def start(self):
        if self.id is not None:
            self.log.name += f".{self.id}"
        loop = self.loop

        def _start():
            self.register()
            self._abort_timeout = loop.add_timeout(
                loop.time() + self.timeout, self.abort
            )

        self.loop.add_callback(_start)
        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.log.critical("Engine Interrupted, shutting down...\n")


main = launch_new_instance = IPEngine.launch_instance


if __name__ == '__main__':
    main()
