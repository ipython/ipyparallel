#!/usr/bin/env python
# encoding: utf-8
"""
The IPython controller application.
"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
from __future__ import with_statement

import json
import os
import socket
import stat
import sys
import time
from multiprocessing import Process
from signal import SIGABRT
from signal import SIGINT
from signal import signal
from signal import SIGTERM

import zmq
from IPython.core.profiledir import ProfileDir
from jupyter_client.localinterfaces import localhost
from jupyter_client.session import Session
from jupyter_client.session import session_aliases
from jupyter_client.session import session_flags
from traitlets import Bool
from traitlets import default
from traitlets import Dict
from traitlets import import_item
from traitlets import Instance
from traitlets import Integer
from traitlets import List
from traitlets import observe
from traitlets import Tuple
from traitlets import Type
from traitlets import Unicode
from traitlets import Union
from traitlets import validate
from zmq.devices import ProcessMonitoredQueue
from zmq.eventloop.zmqstream import ZMQStream
from zmq.log.handlers import PUBHandler

from .broadcast_scheduler import BroadcastScheduler
from ipyparallel import util
from ipyparallel.apps.baseapp import base_aliases
from ipyparallel.apps.baseapp import base_flags
from ipyparallel.apps.baseapp import BaseParallelApplication
from ipyparallel.apps.baseapp import catch_config_error
from ipyparallel.controller.broadcast_scheduler import launch_broadcast_scheduler
from ipyparallel.controller.dictdb import DictDB
from ipyparallel.controller.heartmonitor import HeartMonitor
from ipyparallel.controller.hub import Hub
from ipyparallel.controller.scheduler import launch_scheduler
from ipyparallel.controller.task_scheduler import TaskScheduler
from ipyparallel.util import disambiguate_url

# conditional import of SQLiteDB / MongoDB backend class
real_dbs = []

try:
    from ipyparallel.controller.sqlitedb import SQLiteDB
except ImportError:
    pass
else:
    real_dbs.append(SQLiteDB)

try:
    from ipyparallel.controller.mongodb import MongoDB
except ImportError:
    pass
else:
    real_dbs.append(MongoDB)


# -----------------------------------------------------------------------------
# Module level variables
# -----------------------------------------------------------------------------


_description = """Start the IPython controller for parallel computing.

The IPython controller provides a gateway between the IPython engines and
clients. The controller needs to be started before the engines and can be
configured using command line options or using a cluster directory. Cluster
directories contain config, log and security files and are usually located in
your ipython directory and named as "profile_name". See the `profile`
and `profile-dir` options for details.
"""

_examples = """
ipcontroller --ip=192.168.0.1 --port=1000  # listen on ip, port for engines
ipcontroller --scheme=pure  # use the pure zeromq scheduler
"""


# -----------------------------------------------------------------------------
# The main application
# -----------------------------------------------------------------------------
flags = {}
flags.update(base_flags)
flags.update(
    {
        'usethreads': (
            {'IPController': {'use_threads': True}},
            'Use threads instead of processes for the schedulers',
        ),
        'sqlitedb': (
            {'IPController': {'db_class': 'ipyparallel.controller.sqlitedb.SQLiteDB'}},
            'use the SQLiteDB backend',
        ),
        'mongodb': (
            {'IPController': {'db_class': 'ipyparallel.controller.mongodb.MongoDB'}},
            'use the MongoDB backend',
        ),
        'dictdb': (
            {'IPController': {'db_class': 'ipyparallel.controller.dictdb.DictDB'}},
            'use the in-memory DictDB backend',
        ),
        'nodb': (
            {'IPController': {'db_class': 'ipyparallel.controller.dictdb.NoDB'}},
            """use dummy DB backend, which doesn't store any information.

                    This is the default as of IPython 0.13.

                    To enable delayed or repeated retrieval of results from the Hub,
                    select one of the true db backends.
                    """,
        ),
        'reuse': (
            {'IPController': {'reuse_files': True}},
            'reuse existing json connection files',
        ),
        'restore': (
            {'IPController': {'restore_engines': True, 'reuse_files': True}},
            'Attempt to restore engines from a JSON file.  '
            'For use when resuming a crashed controller',
        ),
    }
)

flags.update(session_flags)

aliases = dict(
    ssh='IPController.ssh_server',
    enginessh='IPController.engine_ssh_server',
    location='IPController.location',
    ip='IPController.ip',
    transport='IPController.transport',
    port='IPController.regport',
    ping='HeartMonitor.period',
    scheme='TaskScheduler.scheme_name',
    hwm='TaskScheduler.hwm',
)
aliases.update(base_aliases)
aliases.update(session_aliases)

_db_shortcuts = {
    'sqlitedb': 'ipyparallel.controller.sqlitedb.SQLiteDB',
    'mongodb': 'ipyparallel.controller.mongodb.MongoDB',
    'dictdb': 'ipyparallel.controller.dictdb.DictDB',
    'nodb': 'ipyparallel.controller.dictdb.NoDB',
}


class IPController(BaseParallelApplication):

    name = u'ipcontroller'
    description = _description
    examples = _examples
    classes = [
        ProfileDir,
        Session,
        Hub,
        TaskScheduler,
        HeartMonitor,
        DictDB,
    ] + real_dbs
    _deprecated_classes = ("HubFactory", "IPControllerApp")

    # change default to True
    auto_create = Bool(
        True, config=True, help="""Whether to create profile dir if it doesn't exist."""
    )

    reuse_files = Bool(
        False,
        config=True,
        help="""Whether to reuse existing json connection files.
        If False, connection files will be removed on a clean exit.
        """,
    )
    restore_engines = Bool(
        False,
        config=True,
        help="""Reload engine state from JSON file
        """,
    )
    ssh_server = Unicode(
        u'',
        config=True,
        help="""ssh url for clients to use when connecting to the Controller
        processes. It should be of the form: [user@]server[:port]. The
        Controller's listening addresses must be accessible from the ssh server""",
    )
    engine_ssh_server = Unicode(
        u'',
        config=True,
        help="""ssh url for engines to use when connecting to the Controller
        processes. It should be of the form: [user@]server[:port]. The
        Controller's listening addresses must be accessible from the ssh server""",
    )
    location = Unicode(
        socket.gethostname(),
        config=True,
        help="""The external IP or domain name of the Controller, used for disambiguating
        engine and client connections.""",
    )

    use_threads = Bool(
        False, config=True, help='Use threads instead of processes for the schedulers'
    )

    engine_json_file = Unicode(
        'ipcontroller-engine.json',
        config=True,
        help="JSON filename where engine connection info will be stored.",
    )
    client_json_file = Unicode(
        'ipcontroller-client.json',
        config=True,
        help="JSON filename where client connection info will be stored.",
    )

    @observe('cluster_id')
    def _cluster_id_changed(self, change):
        base = 'ipcontroller'
        if change.new:
            base = f"{base}-{change.new}"
        self.engine_json_file = f"{base}-engine.json"
        self.client_json_file = f"{base}-client.json"

    # internal
    children = List()
    mq_class = Unicode('zmq.devices.ProcessMonitoredQueue')

    @observe('use_threads')
    def _use_threads_changed(self, change):
        self.mq_class = 'zmq.devices.{}MonitoredQueue'.format(
            'Thread' if change['new'] else 'Process'
        )

    write_connection_files = Bool(
        True,
        help="""Whether to write connection files to disk.
        True in all cases other than runs with `reuse_files=True` *after the first*
        """,
    )

    aliases = Dict(aliases)
    flags = Dict(flags)

    # port-pairs for schedulers:
    hb = Tuple(
        Integer(),
        Integer(),
        config=True,
        help="""PUB/ROUTER Port pair for Engine heartbeats""",
    )

    def _hb_default(self):
        return tuple(util.select_random_ports(2))

    mux = Tuple(
        Integer(),
        Integer(),
        config=True,
        help="""Client/Engine Port pair for MUX queue""",
    )

    def _mux_default(self):
        return tuple(util.select_random_ports(2))

    task = Tuple(
        Integer(),
        Integer(),
        config=True,
        help="""Client/Engine Port pair for Task queue""",
    )

    def _task_default(self):
        return tuple(util.select_random_ports(2))

    broadcast_scheduler_depth = Integer(
        1,
        config=True,
        help="Depth of spanning tree schedulers",
    )
    number_of_leaf_schedulers = Integer()
    number_of_broadcast_schedulers = Integer()
    number_of_non_leaf_schedulers = Integer()

    @default('number_of_leaf_schedulers')
    def get_number_of_leaf_schedulers(self):
        return 2 ** self.broadcast_scheduler_depth

    @default('number_of_broadcast_schedulers')
    def get_number_of_broadcast_schedulers(self):
        return 2 * self.number_of_leaf_schedulers - 1

    @default('number_of_non_leaf_schedulers')
    def get_number_of_non_leaf_schedulers(self):
        return self.number_of_broadcast_schedulers - self.number_of_leaf_schedulers

    broadcast = List(
        Integer(), config=True, help="List of available ports for broadcast"
    )

    def _broadcast_default(self):
        return util.select_random_ports(
            self.number_of_leaf_schedulers + self.number_of_broadcast_schedulers
        )

    regport = Integer(config=True, help="Port for engine registration")

    @default("regport")
    def _regport_default(self):
        return util.select_random_ports(1)[0]

    control = Tuple(
        Integer(),
        Integer(),
        config=True,
        help="""Client/Engine Port pair for Control queue""",
    )

    def _control_default(self):
        return tuple(util.select_random_ports(2))

    iopub = Tuple(
        Integer(),
        Integer(),
        config=True,
        help="""Client/Engine Port pair for IOPub relay""",
    )

    def _iopub_default(self):
        return tuple(util.select_random_ports(2))

    # single ports:
    mon_port = Integer(config=True, help="""Monitor (SUB) port for queue traffic""")

    def _mon_port_default(self):
        return util.select_random_ports(1)[0]

    notifier_port = Integer(
        config=True, help="""PUB port for sending engine status notifications"""
    )

    def _notifier_port_default(self):
        return util.select_random_ports(1)[0]

    engine_ip = Unicode(
        config=True,
        help="IP on which to listen for engine connections. [default: loopback]",
    )

    def _engine_ip_default(self):
        return localhost()

    engine_transport = Unicode(
        'tcp', config=True, help="0MQ transport for engine connections. [default: tcp]"
    )

    client_ip = Unicode(
        config=True,
        help="IP on which to listen for client connections. [default: loopback]",
    )
    client_transport = Unicode(
        'tcp', config=True, help="0MQ transport for client connections. [default : tcp]"
    )

    monitor_ip = Unicode(
        config=True,
        help="IP on which to listen for monitor messages. [default: loopback]",
    )
    monitor_transport = Unicode(
        'tcp', config=True, help="0MQ transport for monitor messages. [default : tcp]"
    )

    _client_ip_default = _monitor_ip_default = _engine_ip_default

    monitor_url = Unicode('')

    @default("monitor_url")
    def _default_monitor_url(self):
        return f"{self.monitor_transport}://{self.monitor_ip}:{self.mon_port}"

    db_class = Union(
        [Unicode(), Type()],
        default_value=DictDB,
        config=True,
        help="""The class to use for the DB backend

        Options include:

        SQLiteDB: SQLite
        MongoDB : use MongoDB
        DictDB  : in-memory storage (fastest, but be mindful of memory growth of the Hub)
        NoDB    : disable database altogether (default)

        """,
    )

    @validate("db_class")
    def _validate_db_class(self, proposal):
        value = proposal.value
        if isinstance(value, str):
            # if it's a string, import it
            value = _db_shortcuts.get(value.lower(), value)
            return import_item(value)
        return value

    registration_timeout = Integer(
        0,
        config=True,
        help="Engine registration timeout in seconds [default: max(30,"
        "10*heartmonitor.period)]",
    )

    def _registration_timeout_default(self):
        if self.heartmonitor is None:
            # early initialization, this value will be ignored
            return 0
            # heartmonitor period is in milliseconds, so 10x in seconds is .01
        return max(30, int(0.01 * self.heartmonitor.period))

    # not configurable
    db = Instance('ipyparallel.controller.dictdb.BaseDB', allow_none=True)
    heartmonitor = Instance(
        'ipyparallel.controller.heartmonitor.HeartMonitor', allow_none=True
    )

    ip = Unicode(
        "127.0.0.1", config=True, help="""Set the controller ip for all connections."""
    )
    transport = Unicode(
        "tcp", config=True, help="""Set the zmq transport for all connections."""
    )

    @observe('ip')
    def _ip_changed(self, change):
        new = change['new']
        self.engine_ip = new
        self.client_ip = new
        self.monitor_ip = new
        self._update_monitor_url()

    def _update_monitor_url(self):
        self.monitor_url = self._default_monitor_url()

    @observe('transport')
    def _transport_changed(self, change):
        new = change['new']
        self.engine_transport = new
        self.client_transport = new
        self.monitor_transport = new
        self._update_monitor_url()

    context = Instance(zmq.Context)

    @default("context")
    def _defaut_context(self):
        return zmq.Context.instance()

    def client_url(self, channel, index=None):
        """return full zmq url for a named client channel"""
        return "%s://%s:%i" % (
            self.client_transport,
            self.client_ip,
            self.client_info[channel]
            if index is None
            else self.client_info[channel][index],
        )

    def engine_url(self, channel, index=None):
        """return full zmq url for a named engine channel"""
        return "%s://%s:%i" % (
            self.engine_transport,
            self.engine_ip,
            self.engine_info[channel]
            if index is None
            else self.engine_info[channel][index],
        )

    def save_connection_dict(self, fname, cdict):
        """save a connection dict to json file."""
        fname = os.path.join(self.profile_dir.security_dir, fname)
        self.log.info("writing connection info to %s", fname)
        with open(fname, 'w') as f:
            f.write(json.dumps(cdict, indent=2))
        os.chmod(fname, stat.S_IRUSR | stat.S_IWUSR)

    def load_config_from_json(self):
        """load config from existing json connector files."""
        c = self.config
        self.log.debug("loading config from JSON")

        # load engine config

        fname = os.path.join(self.profile_dir.security_dir, self.engine_json_file)
        self.log.info("loading connection info from %s", fname)
        with open(fname) as f:
            ecfg = json.loads(f.read())

        # json gives unicode, Session.key wants bytes
        c.Session.key = ecfg['key'].encode('ascii')

        xport, ip = ecfg['interface'].split('://')

        c.IPController.engine_ip = ip
        c.IPController.engine_transport = xport

        self.location = ecfg['location']
        if not self.engine_ssh_server:
            self.engine_ssh_server = ecfg['ssh']

        # load client config

        fname = os.path.join(self.profile_dir.security_dir, self.client_json_file)
        self.log.info("loading connection info from %s", fname)
        with open(fname) as f:
            ccfg = json.loads(f.read())

        for key in ('key', 'registration', 'pack', 'unpack', 'signature_scheme'):
            assert ccfg[key] == ecfg[key], (
                "mismatch between engine and client info: %r" % key
            )

        xport, ip = ccfg['interface'].split('://')

        c.IPController.client_transport = xport
        c.IPController.client_ip = ip
        if not self.ssh_server:
            self.ssh_server = ccfg['ssh']

        # load port config:
        c.IPController.regport = ecfg['registration']
        c.IPController.hb = (ecfg['hb_ping'], ecfg['hb_pong'])
        c.IPController.control = (ccfg['control'], ecfg['control'])
        c.IPController.mux = (ccfg['mux'], ecfg['mux'])
        c.IPController.task = (ccfg['task'], ecfg['task'])
        c.IPController.iopub = (ccfg['iopub'], ecfg['iopub'])
        c.IPController.notifier_port = ccfg['notification']

    def cleanup_connection_files(self):
        if self.reuse_files:
            self.log.debug("leaving JSON connection files for reuse")
            return
        self.log.debug("cleaning up JSON connection files")
        for f in (self.client_json_file, self.engine_json_file):
            f = os.path.join(self.profile_dir.security_dir, f)
            try:
                os.remove(f)
            except Exception as e:
                self.log.error("Failed to cleanup connection file: %s", e)
            else:
                self.log.debug(u"removed %s", f)

    def load_secondary_config(self):
        """secondary config, loading from JSON and setting defaults"""
        if self.reuse_files:
            try:
                self.load_config_from_json()
            except (AssertionError, IOError) as e:
                self.log.error("Could not load config from JSON: %s" % e)
            else:
                # successfully loaded config from JSON, and reuse=True
                # no need to wite back the same file
                self.write_connection_files = False

        self.log.debug("Config changed")
        self.log.debug(repr(self.config))

    def init_hub(self):
        c = self.config

        ctx = self.context
        loop = self.loop
        if 'TaskScheduler.scheme_name' in self.config:
            scheme = self.config.TaskScheduler.scheme_name
        else:
            from .task_scheduler import TaskScheduler

            scheme = TaskScheduler.scheme_name.default_value

        # build connection dicts
        engine = self.engine_info = {
            'interface': "%s://%s" % (self.engine_transport, self.engine_ip),
            'registration': self.regport,
            'control': self.control[1],
            'mux': self.mux[1],
            'hb_ping': self.hb[0],
            'hb_pong': self.hb[1],
            'task': self.task[1],
            'iopub': self.iopub[1],
            BroadcastScheduler.port_name: self.broadcast[
                -self.number_of_leaf_schedulers :
            ],
        }

        client = self.client_info = {
            'interface': "%s://%s" % (self.client_transport, self.client_ip),
            'registration': self.regport,
            'control': self.control[0],
            'mux': self.mux[0],
            'task': self.task[0],
            'task_scheme': scheme,
            'iopub': self.iopub[0],
            'notification': self.notifier_port,
            BroadcastScheduler.port_name: self.broadcast[
                : self.number_of_broadcast_schedulers
            ],
        }

        self.log.debug("Hub engine addrs: %s", self.engine_info)
        self.log.debug("Hub client addrs: %s", self.client_info)

        # Registrar socket
        q = ZMQStream(ctx.socket(zmq.ROUTER), loop)
        util.set_hwm(q, 0)
        q.bind(self.client_url('registration'))
        self.log.info(
            "Hub listening on %s for registration.", self.client_url('registration')
        )
        if self.client_ip != self.engine_ip:
            q.bind(self.engine_url('registration'))
            self.log.info(
                "Hub listening on %s for registration.", self.engine_url('registration')
            )

        ### Engine connections ###

        # heartbeat
        hpub = ctx.socket(zmq.PUB)
        hpub.bind(self.engine_url('hb_ping'))
        hrep = ctx.socket(zmq.ROUTER)
        util.set_hwm(hrep, 0)
        hrep.bind(self.engine_url('hb_pong'))
        self.heartmonitor = HeartMonitor(
            loop=loop,
            parent=self,
            log=self.log,
            pingstream=ZMQStream(hpub, loop),
            pongstream=ZMQStream(hrep, loop),
        )

        ### Client connections ###

        # Notifier socket
        n = ZMQStream(ctx.socket(zmq.PUB), loop)
        n.bind(self.client_url('notification'))

        ### build and launch the queues ###

        # monitor socket
        sub = ctx.socket(zmq.SUB)
        sub.setsockopt(zmq.SUBSCRIBE, b"")
        sub.bind(self.monitor_url)
        sub.bind('inproc://monitor')
        sub = ZMQStream(sub, loop)

        # connect the db
        db_class = self.db_class
        self.log.info(f'Hub using DB backend: {self.db_class.__name__}')
        self.db = self.db_class(session=self.session.session, parent=self, log=self.log)
        time.sleep(0.25)

        # resubmit stream
        r = ZMQStream(ctx.socket(zmq.DEALER), loop)
        url = util.disambiguate_url(self.client_url('task'))
        r.connect(url)

        self.hub = Hub(
            loop=loop,
            session=self.session,
            monitor=sub,
            heartmonitor=self.heartmonitor,
            query=q,
            notifier=n,
            resubmit=r,
            db=self.db,
            engine_info=self.engine_info,
            client_info=self.client_info,
            log=self.log,
            registration_timeout=self.registration_timeout,
            parent=self,
        )

        if self.write_connection_files:
            # save to new json config files
            base = {
                'key': self.session.key.decode('ascii'),
                'location': self.location,
                'pack': self.session.packer,
                'unpack': self.session.unpacker,
                'signature_scheme': self.session.signature_scheme,
            }

            cdict = {'ssh': self.ssh_server}
            cdict.update(self.client_info)
            cdict.update(base)
            self.save_connection_dict(self.client_json_file, cdict)

            edict = {'ssh': self.engine_ssh_server}
            edict.update(self.engine_info)
            edict.update(base)
            self.save_connection_dict(self.engine_json_file, edict)

        fname = "engines%s.json" % self.cluster_id
        self.hub.engine_state_file = os.path.join(self.profile_dir.log_dir, fname)
        if self.restore_engines:
            self.hub._load_engine_state()

    def launch_python_scheduler(self, name, scheduler_args, children):
        if 'Process' in self.mq_class:
            # run the Python scheduler in a Process
            q = Process(
                target=launch_scheduler,
                kwargs=scheduler_args,
                name=name,
                daemon=True,
            )
            children.append(q)
        else:
            # single-threaded Controller
            scheduler_args['in_thread'] = True
            launch_scheduler(**scheduler_args)

    def get_python_scheduler_args(
        self, scheduler_name, scheduler_class, monitor_url, identity=None
    ):
        return {
            'scheduler_class': scheduler_class,
            'in_addr': self.client_url(scheduler_name),
            'out_addr': self.engine_url(scheduler_name),
            'mon_addr': monitor_url,
            'not_addr': disambiguate_url(self.client_url('notification')),
            'reg_addr': disambiguate_url(self.client_url('registration')),
            'identity': identity if identity else bytes(scheduler_name, 'utf8'),
            'logname': 'scheduler',
            'loglevel': self.log_level,
            'log_url': self.log_url,
            'config': dict(self.config),
        }

    def launch_broadcast_schedulers(self, monitor_url, children):
        def launch_in_thread_or_process(scheduler_args, depth, identity):

            if 'Process' in self.mq_class:
                # run the Python scheduler in a Process
                q = Process(
                    target=launch_broadcast_scheduler,
                    kwargs=scheduler_args,
                    name=f"BroadcastScheduler(depth={depth}, id={identity})",
                    daemon=True,
                )
                children.append(q)
            else:
                # single-threaded Controller
                scheduler_args['in_thread'] = True
                launch_broadcast_scheduler(**scheduler_args)

        def recursively_start_schedulers(identity, depth):
            outgoing_id1 = identity * 2 + 1
            outgoing_id2 = outgoing_id1 + 1
            is_leaf = depth == self.broadcast_scheduler_depth
            is_root = depth == 0

            # FIXME: use localhost, not client ip for internal communication
            # this will still be localhost anyway for the most common cases
            # of localhost or */0.0.0.0
            in_addr = self.client_url(BroadcastScheduler.port_name, identity)
            if not is_root:
                # non-root schedulers connect, so they need a disambiguated url
                in_addr = disambiguate_url(in_addr)

            scheduler_args = dict(
                in_addr=in_addr,
                mon_addr=monitor_url,
                not_addr=disambiguate_url(self.client_url('notification')),
                reg_addr=disambiguate_url(self.client_url('registration')),
                identity=identity,
                config=dict(self.config),
                loglevel=self.log_level,
                log_url=self.log_url,
                outgoing_ids=[outgoing_id1, outgoing_id2],
                depth=depth,
                is_leaf=is_leaf,
            )
            if is_leaf:
                scheduler_args.update(
                    out_addrs=[
                        self.engine_url(
                            BroadcastScheduler.port_name,
                            identity - self.number_of_non_leaf_schedulers,
                        )
                    ],
                )
            else:
                scheduler_args.update(
                    out_addrs=[
                        self.client_url(BroadcastScheduler.port_name, outgoing_id1),
                        self.client_url(BroadcastScheduler.port_name, outgoing_id2),
                    ]
                )
            launch_in_thread_or_process(scheduler_args, depth=depth, identity=identity)
            if not is_leaf:
                recursively_start_schedulers(outgoing_id1, depth + 1)
                recursively_start_schedulers(outgoing_id2, depth + 1)

        recursively_start_schedulers(0, 0)

    def init_schedulers(self):
        children = self.children
        mq = import_item(str(self.mq_class))
        # ensure session key is shared across sessions
        self.config.Session.key = self.session.key
        ident = self.session.bsession
        # disambiguate url, in case of *
        monitor_url = disambiguate_url(self.monitor_url)
        # maybe_inproc = 'inproc://monitor' if self.use_threads else monitor_url
        # IOPub relay (in a Process)
        q = mq(zmq.PUB, zmq.SUB, zmq.PUB, b'N/A', b'iopub')
        q.name = "IOPubScheduler"
        q.bind_in(self.client_url('iopub'))
        q.setsockopt_in(zmq.IDENTITY, ident + b"_iopub")
        q.bind_out(self.engine_url('iopub'))
        q.setsockopt_out(zmq.SUBSCRIBE, b'')
        q.connect_mon(monitor_url)
        q.daemon = True
        children.append(q)

        # Multiplexer Queue (in a Process)
        q = mq(zmq.ROUTER, zmq.ROUTER, zmq.PUB, b'in', b'out')
        q.name = "DirectScheduler"

        q.bind_in(self.client_url('mux'))
        q.setsockopt_in(zmq.IDENTITY, b'mux_in')
        q.bind_out(self.engine_url('mux'))
        q.setsockopt_out(zmq.IDENTITY, b'mux_out')
        q.connect_mon(monitor_url)
        q.daemon = True
        children.append(q)

        # Control Queue (in a Process)
        q = mq(zmq.ROUTER, zmq.ROUTER, zmq.PUB, b'incontrol', b'outcontrol')
        q.name = "ControlScheduler"
        q.bind_in(self.client_url('control'))
        q.setsockopt_in(zmq.IDENTITY, b'control_in')
        q.bind_out(self.engine_url('control'))
        q.setsockopt_out(zmq.IDENTITY, b'control_out')
        q.connect_mon(monitor_url)
        q.daemon = True
        children.append(q)
        if 'TaskScheduler.scheme_name' in self.config:
            scheme = self.config.TaskScheduler.scheme_name
        else:
            scheme = TaskScheduler.scheme_name.default_value
        # Task Queue (in a Process)
        if scheme == 'pure':
            self.log.warn("task::using pure DEALER Task scheduler")
            q = mq(zmq.ROUTER, zmq.DEALER, zmq.PUB, b'intask', b'outtask')
            q.name = "TaskScheduler(pure)"
            # q.setsockopt_out(zmq.HWM, hub.hwm)
            q.bind_in(self.client_url('task'))
            q.setsockopt_in(zmq.IDENTITY, b'task_in')
            q.bind_out(self.engine_url('task'))
            q.setsockopt_out(zmq.IDENTITY, b'task_out')
            q.connect_mon(monitor_url)
            q.daemon = True
            children.append(q)
        elif scheme == 'none':
            self.log.warning("task::using no Task scheduler")

        else:
            self.log.info("task::using Python %s Task scheduler" % scheme)
            self.launch_python_scheduler(
                'TaskScheduler',
                self.get_python_scheduler_args('task', TaskScheduler, monitor_url),
                children,
            )

        self.launch_broadcast_schedulers(monitor_url, children)

        # set unlimited HWM for all relay devices
        if hasattr(zmq, 'SNDHWM'):
            q = children[0]
            q.setsockopt_in(zmq.RCVHWM, 0)
            q.setsockopt_out(zmq.SNDHWM, 0)

            for q in children[1:]:
                if not hasattr(q, 'setsockopt_in'):
                    continue
                q.setsockopt_in(zmq.SNDHWM, 0)
                q.setsockopt_in(zmq.RCVHWM, 0)
                q.setsockopt_out(zmq.SNDHWM, 0)
                q.setsockopt_out(zmq.RCVHWM, 0)
                q.setsockopt_mon(zmq.SNDHWM, 0)

    def terminate_children(self):
        child_procs = []
        for child in self.children:
            if isinstance(child, ProcessMonitoredQueue):
                child_procs.append(child.launcher)
            elif isinstance(child, Process):
                child_procs.append(child)
        if child_procs:
            self.log.critical("terminating children...")
            for child in child_procs:
                try:
                    child.terminate()
                except OSError:
                    # already dead
                    pass

    def handle_signal(self, sig, frame):
        self.log.critical("Received signal %i, shutting down", sig)
        self.terminate_children()
        self.loop.stop()

    def init_signal(self):
        for sig in (SIGINT, SIGABRT, SIGTERM):
            signal(sig, self.handle_signal)

    def forward_logging(self):
        if self.log_url:
            self.log.info("Forwarding logging to %s" % self.log_url)
            context = zmq.Context.instance()
            lsock = context.socket(zmq.PUB)
            lsock.connect(self.log_url)
            handler = PUBHandler(lsock)
            handler.root_topic = 'controller'
            handler.setLevel(self.log_level)
            self.log.addHandler(handler)

    @catch_config_error
    def initialize(self, argv=None):
        super(IPController, self).initialize(argv)
        self.forward_logging()
        self.load_secondary_config()
        self.init_hub()
        self.init_schedulers()

    def start(self):
        # Start the subprocesses:
        # children must be started before signals are setup,
        # otherwise signal-handling will fire multiple times
        for child in self.children:
            child.start()
            if hasattr(child, 'launcher'):
                # apply name to actual process/thread for logging
                setattr(child.launcher, 'name', child.name)
            if not self.use_threads:
                process = getattr(child, 'launcher', child)
                self.log.debug(f"Started process {child.name}: {process.pid}")
            else:
                self.log.debug(f"Started thread {child.name}")

        self.init_signal()

        self.heartmonitor.start()
        self.log.info("Heartmonitor started")

        try:
            self.loop.start()
        except KeyboardInterrupt:
            self.log.critical("Interrupted, Exiting...\n")
        finally:
            self.cleanup_connection_files()


def main(*args, **kwargs):
    """Create and run the IPython controller"""
    if sys.platform == 'win32':
        # make sure we don't get called from a multiprocessing subprocess
        # this can result in infinite Controllers being started on Windows
        # which doesn't have a proper fork, so multiprocessing is wonky

        # this only comes up when IPython has been installed using vanilla
        # setuptools, and *not* distribute.
        import multiprocessing

        p = multiprocessing.current_process()
        # the main process has name 'MainProcess'
        # subprocesses will have names like 'Process-1'
        if p.name != 'MainProcess':
            # we are a subprocess, don't start another Controller!
            return
    return IPController.launch_instance(*args, **kwargs)


if __name__ == '__main__':
    main()
