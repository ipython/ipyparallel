"""A semi-synchronous Client for IPython parallel"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

import collections
from concurrent.futures import Future
from getpass import getpass
import json
import os
from pprint import pprint
import sys
from threading import Thread, Event, current_thread
import time
import types
import warnings

pjoin = os.path.join

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from traitlets.config.configurable import MultipleInstanceError
from IPython.core.application import BaseIPythonApplication
from IPython.core.profiledir import ProfileDir, ProfileDirError

from IPython import get_ipython
from IPython.utils.capture import RichOutput
from IPython.utils.coloransi import TermColors
from jupyter_client.localinterfaces import localhost, is_local_ip
from IPython.paths import get_ipython_dir
from IPython.utils.path import compress_user
from ipython_genutils.py3compat import cast_bytes, string_types, xrange, iteritems
from traitlets import (
    HasTraits, Instance, Unicode,
    Dict, List, Bool, Set, Any
)
from decorator import decorator

from ipyparallel import Reference
from ipyparallel import error
from ipyparallel import util

from jupyter_client.session import Session
from ipyparallel import serialize
from ipyparallel.serialize import PrePickled

from .asyncresult import AsyncResult, AsyncHubResult
from .futures import MessageFuture, multi_future
from .view import DirectView, LoadBalancedView

#--------------------------------------------------------------------------
# Decorators for Client methods
#--------------------------------------------------------------------------


@decorator
def unpack_message(f, self, msg_parts):
    """Unpack a message before calling the decorated method."""
    idents, msg = self.session.feed_identities(msg_parts, copy=False)
    try:
        msg = self.session.deserialize(msg, content=True, copy=False)
    except:
        self.log.error("Invalid Message", exc_info=True)
    else:
        if self.debug:
            pprint(msg)
        return f(self, msg)

#--------------------------------------------------------------------------
# Classes
#--------------------------------------------------------------------------


_no_connection_file_msg = """
Failed to connect because no Controller could be found.
Please double-check your profile and ensure that a cluster is running.
"""

class ExecuteReply(RichOutput):
    """wrapper for finished Execute results"""
    def __init__(self, msg_id, content, metadata):
        self.msg_id = msg_id
        self._content = content
        self.execution_count = content['execution_count']
        self.metadata = metadata
    
    # RichOutput overrides
    
    @property
    def source(self):
        execute_result = self.metadata['execute_result']
        if execute_result:
            return execute_result.get('source', '')
    
    @property
    def data(self):
        execute_result = self.metadata['execute_result']
        if execute_result:
            return execute_result.get('data', {})
        return {}
    
    @property
    def _metadata(self):
        execute_result = self.metadata['execute_result']
        if execute_result:
            return execute_result.get('metadata', {})
        return {}

    def display(self):
        from IPython.display import publish_display_data
        publish_display_data(self.data, self.metadata)

    def _repr_mime_(self, mime):
        if mime not in self.data:
            return
        data = self.data[mime]
        if mime in self._metadata:
            return data, self._metadata[mime]
        else:
            return data

    def _repr_mimebundle_(self, *args, **kwargs):
        data, md = self.data, self.metadata
        if 'text/plain' in data:
            data = data.copy()
            data['text/plain'] = self._plaintext()
        return data, md

    def __getitem__(self, key):
        return self.metadata[key]

    def __getattr__(self, key):
        if key not in self.metadata:
            raise AttributeError(key)
        return self.metadata[key]

    def __repr__(self):
        execute_result = self.metadata['execute_result'] or {'data':{}}
        text_out = execute_result['data'].get('text/plain', '')
        if len(text_out) > 32:
            text_out = text_out[:29] + '...'

        return "<ExecuteReply[%i]: %s>" % (self.execution_count, text_out)

    def _plaintext(self):
        execute_result = self.metadata['execute_result'] or {'data':{}}
        text_out = execute_result['data'].get('text/plain', '')

        if not text_out:
            return ''

        ip = get_ipython()
        if ip is None:
            colors = "NoColor"
        else:
            colors = ip.colors

        if colors == "NoColor":
            out = normal = ""
        else:
            out = TermColors.Red
            normal = TermColors.Normal

        if '\n' in text_out and not text_out.startswith('\n'):
            # add newline for multiline reprs
            text_out = '\n' + text_out

        return u''.join([
            out,
            u'Out[%i:%i]: ' % (
                self.metadata['engine_id'], self.execution_count
            ),
            normal,
            text_out,
            ])

    def _repr_pretty_(self, p, cycle):
        p.text(self._plaintext())


class Metadata(dict):
    """Subclass of dict for initializing metadata values.

    Attribute access works on keys.

    These objects have a strict set of keys - errors will raise if you try
    to add new keys.
    """
    def __init__(self, *args, **kwargs):
        dict.__init__(self)
        md = {'msg_id' : None,
              'submitted' : None,
              'started' : None,
              'completed' : None,
              'received' : None,
              'engine_uuid' : None,
              'engine_id' : None,
              'follow' : None,
              'after' : None,
              'status' : None,

              'execute_input' : None,
              'execute_result' : None,
              'error' : None,
              'stdout' : '',
              'stderr' : '',
              'outputs' : [],
              'data': {},
            }
        self.update(md)
        self.update(dict(*args, **kwargs))

    def __getattr__(self, key):
        """getattr aliased to getitem"""
        if key in self:
            return self[key]
        else:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        """setattr aliased to setitem, with strict"""
        if key in self:
            self[key] = value
        else:
            raise AttributeError(key)

    def __setitem__(self, key, value):
        """strict static key enforcement"""
        if key in self:
            dict.__setitem__(self, key, value)
        else:
            raise KeyError(key)


def _is_future(f):
    """light duck-typing check for Futures"""
    return hasattr(f, 'add_done_callback')


class Client(HasTraits):
    """A semi-synchronous client to an IPython parallel cluster

    Parameters
    ----------

    url_file : str
        The path to ipcontroller-client.json.
        This JSON file should contain all the information needed to connect to a cluster,
        and is likely the only argument needed.
        Connection information for the Hub's registration.  If a json connector
        file is given, then likely no further configuration is necessary.
        [Default: use profile]
    profile : bytes
        The name of the Cluster profile to be used to find connector information.
        If run from an IPython application, the default profile will be the same
        as the running application, otherwise it will be 'default'.
    cluster_id : str
        String id to added to runtime files, to prevent name collisions when using
        multiple clusters with a single profile simultaneously.
        When set, will look for files named like: 'ipcontroller-<cluster_id>-client.json'
        Since this is text inserted into filenames, typical recommendations apply:
        Simple character strings are ideal, and spaces are not recommended (but
        should generally work)
    context : zmq.Context
        Pass an existing zmq.Context instance, otherwise the client will create its own.
    debug : bool
        flag for lots of message printing for debug purposes
    timeout : float
        time (in seconds) to wait for connection replies from the Hub
        [Default: 10]
    
    Other Parameters
    ----------------
    
    sshserver : str
        A string of the form passed to ssh, i.e. 'server.tld' or 'user@server.tld:port'
        If keyfile or password is specified, and this is not, it will default to
        the ip given in addr.
    sshkey : str; path to ssh private key file
        This specifies a key to be used in ssh login, default None.
        Regular default ssh keys will be used without specifying this argument.
    password : str
        Your ssh password to sshserver. Note that if this is left None,
        you will be prompted for it if passwordless key based login is unavailable.
    paramiko : bool
        flag for whether to use paramiko instead of shell ssh for tunneling.
        [default: True on win32, False else]


    Attributes
    ----------

    ids : list of int engine IDs
        requesting the ids attribute always synchronizes
        the registration state. To request ids without synchronization,
        use semi-private _ids attributes.

    history : list of msg_ids
        a list of msg_ids, keeping track of all the execution
        messages you have submitted in order.

    outstanding : set of msg_ids
        a set of msg_ids that have been submitted, but whose
        results have not yet been received.

    results : dict
        a dict of all our results, keyed by msg_id

    block : bool
        determines default behavior when block not specified
        in execution methods

    """


    block = Bool(False)
    outstanding = Set()
    results = Instance('collections.defaultdict', (dict,))
    metadata = Instance('collections.defaultdict', (Metadata,))
    history = List()
    debug = Bool(False)
    _futures = Dict()
    _output_futures = Dict()
    _io_loop = Any()
    _io_thread = Any()

    profile=Unicode()
    def _profile_default(self):
        if BaseIPythonApplication.initialized():
            # an IPython app *might* be running, try to get its profile
            try:
                return BaseIPythonApplication.instance().profile
            except (AttributeError, MultipleInstanceError):
                # could be a *different* subclass of config.Application,
                # which would raise one of these two errors.
                return u'default'
        else:
            return u'default'


    _outstanding_dict = Instance('collections.defaultdict', (set,))
    _ids = List()
    _connected=Bool(False)
    _ssh=Bool(False)
    _context = Instance('zmq.Context', allow_none=True)
    _config = Dict()
    _engines=Instance(util.ReverseDict, (), {})
    _query_socket=Instance('zmq.Socket', allow_none=True)
    _control_socket=Instance('zmq.Socket', allow_none=True)
    _iopub_socket=Instance('zmq.Socket', allow_none=True)
    _notification_socket=Instance('zmq.Socket', allow_none=True)
    _mux_socket=Instance('zmq.Socket', allow_none=True)
    _task_socket=Instance('zmq.Socket', allow_none=True)
    _task_scheme=Unicode()
    _closed = False

    def __new__(self, *args, **kw):
        # don't raise on positional args
        return HasTraits.__new__(self, **kw)

    def __init__(self, url_file=None, profile=None, profile_dir=None, ipython_dir=None,
            context=None, debug=False,
            sshserver=None, sshkey=None, password=None, paramiko=None,
            timeout=10, cluster_id=None, **extra_args
            ):
        if profile:
            super(Client, self).__init__(debug=debug, profile=profile)
        else:
            super(Client, self).__init__(debug=debug)
        if context is None:
            context = zmq.Context.instance()
        self._context = context
        
        if 'url_or_file' in extra_args:
            url_file = extra_args['url_or_file']
            warnings.warn("url_or_file arg no longer supported, use url_file", DeprecationWarning)
        
        if url_file and util.is_url(url_file):
            raise ValueError("single urls cannot be specified, url-files must be used.")

        self._setup_profile_dir(self.profile, profile_dir, ipython_dir)
        
        no_file_msg = '\n'.join([
        "You have attempted to connect to an IPython Cluster but no Controller could be found.",
        "Please double-check your configuration and ensure that a cluster is running.",
        ])
        
        if self._cd is not None:
            if url_file is None:
                if not cluster_id:
                    client_json = 'ipcontroller-client.json'
                else:
                    client_json = 'ipcontroller-%s-client.json' % cluster_id
                url_file = pjoin(self._cd.security_dir, client_json)
                short = compress_user(url_file)
                if not os.path.exists(url_file):
                    print("Waiting for connection file: %s" % short)
                    waiting_time = 0.
                    while waiting_time < timeout:
                        time.sleep(min(timeout-waiting_time, 1))
                        waiting_time += 1
                        if os.path.exists(url_file):
                            break
                if not os.path.exists(url_file):
                    msg = '\n'.join([
                        "Connection file %r not found." % short,
                        no_file_msg,
                    ])
                    raise IOError(msg)
        if url_file is None:
            raise IOError(no_file_msg)
        
        if not os.path.exists(url_file):
            # Connection file explicitly specified, but not found
            raise IOError("Connection file %r not found. Is a controller running?" % \
                compress_user(url_file)
            )
        
        with open(url_file) as f:
            cfg = json.load(f)
        
        self._task_scheme = cfg['task_scheme']

        # sync defaults from args, json:
        if sshserver:
            cfg['ssh'] = sshserver

        location = cfg.setdefault('location', None)
        
        proto,addr = cfg['interface'].split('://')
        addr = util.disambiguate_ip_address(addr, location)
        cfg['interface'] = "%s://%s" % (proto, addr)
        
        # turn interface,port into full urls:
        for key in ('control', 'task', 'mux', 'iopub', 'notification', 'registration'):
            cfg[key] = cfg['interface'] + ':%i' % cfg[key]
        
        url = cfg['registration']
        
        if location is not None and addr == localhost():
            # location specified, and connection is expected to be local
            location_ip = util.ip_for_host(location)
            if not is_local_ip(location_ip) and not sshserver:
                # load ssh from JSON *only* if the controller is not on
                # this machine
                sshserver=cfg['ssh']
            if not is_local_ip(location_ip) and not sshserver:
                # warn if no ssh specified, but SSH is probably needed
                # This is only a warning, because the most likely cause
                # is a local Controller on a laptop whose IP is dynamic
                warnings.warn("""
            Controller appears to be listening on localhost, but not on this machine.
            If this is true, you should specify Client(...,sshserver='you@%s')
            or instruct your controller to listen on an external IP.""" % location,
                RuntimeWarning)
        elif not sshserver:
            # otherwise sync with cfg
            sshserver = cfg['ssh']

        self._config = cfg

        self._ssh = bool(sshserver or sshkey or password)
        if self._ssh and sshserver is None:
            # default to ssh via localhost
            sshserver = addr
        if self._ssh and password is None:
            from zmq.ssh import tunnel
            if tunnel.try_passwordless_ssh(sshserver, sshkey, paramiko):
                password=False
            else:
                password = getpass("SSH Password for %s: "%sshserver)
        ssh_kwargs = dict(keyfile=sshkey, password=password, paramiko=paramiko)

        # configure and construct the session
        try:
            extra_args['packer'] = cfg['pack']
            extra_args['unpacker'] = cfg['unpack']
            extra_args['key'] = cast_bytes(cfg['key'])
            extra_args['signature_scheme'] = cfg['signature_scheme']
        except KeyError as exc:
            msg = '\n'.join([
                "Connection file is invalid (missing '{}'), possibly from an old version of IPython.",
                "If you are reusing connection files, remove them and start ipcontroller again."
            ])
            raise ValueError(msg.format(exc.message))
        
        self.session = Session(**extra_args)

        self._query_socket = self._context.socket(zmq.DEALER)

        if self._ssh:
            from zmq.ssh import tunnel
            tunnel.tunnel_connection(self._query_socket, cfg['registration'], sshserver,
                                     timeout=timeout, **ssh_kwargs)
        else:
            self._query_socket.connect(cfg['registration'])

        self.session.debug = self.debug

        self._notification_handlers = {'registration_notification' : self._register_engine,
                                    'unregistration_notification' : self._unregister_engine,
                                    'shutdown_notification' : lambda msg: self.close(),
                                    }
        self._queue_handlers = {'execute_reply' : self._handle_execute_reply,
                                'apply_reply' : self._handle_apply_reply}
        
        try:
            self._connect(sshserver, ssh_kwargs, timeout)
        except:
            self.close(linger=0)
            raise
        
        # last step: setup magics, if we are in IPython:
        
        ip = get_ipython()
        if ip is None:
            return
        else:
            if 'px' not in ip.magics_manager.magics:
                # in IPython but we are the first Client.
                # activate a default view for parallel magics.
                self.activate()

    def __del__(self):
        """cleanup sockets, but _not_ context."""
        self.close()
    
    def _setup_profile_dir(self, profile, profile_dir, ipython_dir):
        if ipython_dir is None:
            ipython_dir = get_ipython_dir()
        if profile_dir is not None:
            try:
                self._cd = ProfileDir.find_profile_dir(profile_dir)
                return
            except ProfileDirError:
                pass
        elif profile is not None:
            try:
                self._cd = ProfileDir.find_profile_dir_by_name(
                    ipython_dir, profile)
                return
            except ProfileDirError:
                pass
        self._cd = None

    def _update_engines(self, engines):
        """Update our engines dict and _ids from a dict of the form: {id:uuid}."""
        for k,v in iteritems(engines):
            eid = int(k)
            if eid not in self._engines:
                self._ids.append(eid)
            self._engines[eid] = v
        self._ids = sorted(self._ids)
        if sorted(self._engines.keys()) != list(range(len(self._engines))) and \
                        self._task_scheme == 'pure' and self._task_socket:
            self._stop_scheduling_tasks()

    def _stop_scheduling_tasks(self):
        """Stop scheduling tasks because an engine has been unregistered
        from a pure ZMQ scheduler.
        """
        self._task_socket.close()
        self._task_socket = None
        msg = "An engine has been unregistered, and we are using pure " +\
              "ZMQ task scheduling.  Task farming will be disabled."
        if self.outstanding:
            msg += " If you were running tasks when this happened, " +\
                   "some `outstanding` msg_ids may never resolve."
        warnings.warn(msg, RuntimeWarning)

    def _build_targets(self, targets):
        """Turn valid target IDs or 'all' into two lists:
        (int_ids, uuids).
        """
        if not self._ids:
            # flush notification socket if no engines yet, just in case
            if not self.ids:
                raise error.NoEnginesRegistered("Can't build targets without any engines")

        if targets is None:
            targets = self._ids
        elif isinstance(targets, string_types):
            if targets.lower() == 'all':
                targets = self._ids
            else:
                raise TypeError("%r not valid str target, must be 'all'"%(targets))
        elif isinstance(targets, int):
            if targets < 0:
                targets = self.ids[targets]
            if targets not in self._ids:
                raise IndexError("No such engine: %i"%targets)
            targets = [targets]

        if isinstance(targets, slice):
            indices = list(range(len(self._ids))[targets])
            ids = self.ids
            targets = [ ids[i] for i in indices ]

        if not isinstance(targets, (tuple, list, xrange)):
            raise TypeError("targets by int/slice/collection of ints only, not %s"%(type(targets)))

        return [cast_bytes(self._engines[t]) for t in targets], list(targets)

    def _connect(self, sshserver, ssh_kwargs, timeout):
        """setup all our socket connections to the cluster. This is called from
        __init__."""

        # Maybe allow reconnecting?
        if self._connected:
            return
        self._connected=True

        def connect_socket(s, url):
            if self._ssh:
                from zmq.ssh import tunnel
                return tunnel.tunnel_connection(s, url, sshserver, **ssh_kwargs)
            else:
                return s.connect(url)

        self.session.send(self._query_socket, 'connection_request')
        # use Poller because zmq.select has wrong units in pyzmq 2.1.7
        poller = zmq.Poller()
        poller.register(self._query_socket, zmq.POLLIN)
        # poll expects milliseconds, timeout is seconds
        evts = poller.poll(timeout*1000)
        if not evts:
            raise error.TimeoutError("Hub connection request timed out")
        idents, msg = self.session.recv(self._query_socket, mode=0)
        if self.debug:
            pprint(msg)
        content = msg['content']
        # self._config['registration'] = dict(content)
        cfg = self._config
        if content['status'] == 'ok':
            self._mux_socket = self._context.socket(zmq.DEALER)
            connect_socket(self._mux_socket, cfg['mux'])

            self._task_socket = self._context.socket(zmq.DEALER)
            connect_socket(self._task_socket, cfg['task'])

            self._notification_socket = self._context.socket(zmq.SUB)
            self._notification_socket.setsockopt(zmq.SUBSCRIBE, b'')
            connect_socket(self._notification_socket, cfg['notification'])

            self._control_socket = self._context.socket(zmq.DEALER)
            connect_socket(self._control_socket, cfg['control'])

            self._iopub_socket = self._context.socket(zmq.SUB)
            self._iopub_socket.setsockopt(zmq.SUBSCRIBE, b'')
            connect_socket(self._iopub_socket, cfg['iopub'])

            self._update_engines(dict(content['engines']))
        else:
            self._connected = False
            tb = '\n'.join(content.get('traceback', []))
            raise Exception("Failed to connect! %s" % tb)

        self._start_io_thread()

    #--------------------------------------------------------------------------
    # handlers and callbacks for incoming messages
    #--------------------------------------------------------------------------

    def _unwrap_exception(self, content):
        """unwrap exception, and remap engine_id to int."""
        e = error.unwrap_exception(content)
        # print e.traceback
        if e.engine_info:
            e_uuid = e.engine_info['engine_uuid']
            eid = self._engines[e_uuid]
            e.engine_info['engine_id'] = eid
        return e

    def _extract_metadata(self, msg):
        header = msg['header']
        parent = msg['parent_header']
        msg_meta = msg['metadata']
        content = msg['content']
        md = {'msg_id' : parent['msg_id'],
              'received' : util.utcnow(),
              'engine_uuid' : msg_meta.get('engine', None),
              'follow' : msg_meta.get('follow', []),
              'after' : msg_meta.get('after', []),
              'status' : content['status'],
            }

        if md['engine_uuid'] is not None:
            md['engine_id'] = self._engines.get(md['engine_uuid'], None)

        if 'date' in parent:
            md['submitted'] = parent['date']
        if 'started' in msg_meta:
            md['started'] = util._parse_date(msg_meta['started'])
        if 'date' in header:
            md['completed'] = header['date']
        return md

    def _register_engine(self, msg):
        """Register a new engine, and update our connection info."""
        content = msg['content']
        eid = content['id']
        d = {eid : content['uuid']}
        self._update_engines(d)

    def _unregister_engine(self, msg):
        """Unregister an engine that has died."""
        content = msg['content']
        eid = int(content['id'])
        if eid in self._ids:
            self._ids.remove(eid)
            uuid = self._engines.pop(eid)

            self._handle_stranded_msgs(eid, uuid)

        if self._task_socket and self._task_scheme == 'pure':
            self._stop_scheduling_tasks()

    def _handle_stranded_msgs(self, eid, uuid):
        """Handle messages known to be on an engine when the engine unregisters.

        It is possible that this will fire prematurely - that is, an engine will
        go down after completing a result, and the client will be notified
        of the unregistration and later receive the successful result.
        """

        outstanding = self._outstanding_dict[uuid]

        for msg_id in list(outstanding):
            if msg_id in self.results:
                # we already
                continue
            try:
                raise error.EngineError("Engine %r died while running task %r"%(eid, msg_id))
            except:
                content = error.wrap_exception()
            # build a fake message:
            msg = self.session.msg('apply_reply', content=content)
            msg['parent_header']['msg_id'] = msg_id
            msg['metadata']['engine'] = uuid
            self._handle_apply_reply(msg)

    def _handle_execute_reply(self, msg):
        """Save the reply to an execute_request into our results.

        execute messages are never actually used. apply is used instead.
        """

        parent = msg['parent_header']
        msg_id = parent['msg_id']
        future = self._futures.get(msg_id, None)
        if msg_id not in self.outstanding:
            if msg_id in self.history:
                print("got stale result: %s"%msg_id)
            else:
                print("got unknown result: %s"%msg_id)
        else:
            self.outstanding.remove(msg_id)

        content = msg['content']
        header = msg['header']

        # construct metadata:
        md = self.metadata[msg_id]
        md.update(self._extract_metadata(msg))
        
        e_outstanding = self._outstanding_dict[md['engine_uuid']]
        if msg_id in e_outstanding:
            e_outstanding.remove(msg_id)

        # construct result:
        if content['status'] == 'ok':
            self.results[msg_id] = ExecuteReply(msg_id, content, md)
        elif content['status'] == 'aborted':
            self.results[msg_id] = error.TaskAborted(msg_id)
            # aborted tasks will not get output
            out_future = self._output_futures.get(msg_id)
            if out_future and not out_future.done():
                out_future.set_result(None)
        elif content['status'] == 'resubmitted':
            # TODO: handle resubmission
            pass
        else:
            self.results[msg_id] = self._unwrap_exception(content)
        if content['status'] != 'ok' and not content.get('engine_info'):
            # not an engine failure, don't expect output
            out_future = self._output_futures.get(msg_id)
            if out_future and not out_future.done():
                out_future.set_result(None)
        if future:
            future.set_result(self.results[msg_id])

    def _handle_apply_reply(self, msg):
        """Save the reply to an apply_request into our results."""
        parent = msg['parent_header']
        msg_id = parent['msg_id']
        future = self._futures.get(msg_id, None)
        if msg_id not in self.outstanding:
            if msg_id in self.history:
                print("got stale result: %s"%msg_id)
                print(self.results[msg_id])
                print(msg)
            else:
                print("got unknown result: %s"%msg_id)
        else:
            self.outstanding.remove(msg_id)
        content = msg['content']
        header = msg['header']

        # construct metadata:
        md = self.metadata[msg_id]
        md.update(self._extract_metadata(msg))

        e_outstanding = self._outstanding_dict[md['engine_uuid']]
        if msg_id in e_outstanding:
            e_outstanding.remove(msg_id)

        # construct result:
        if content['status'] == 'ok':
            self.results[msg_id] = serialize.deserialize_object(msg['buffers'])[0]
        elif content['status'] == 'aborted':
            self.results[msg_id] = error.TaskAborted(msg_id)
            out_future = self._output_futures.get(msg_id)
            if out_future and not out_future.done():
                out_future.set_result(None)
        elif content['status'] == 'resubmitted':
            # TODO: handle resubmission
            pass
        else:
            self.results[msg_id] = self._unwrap_exception(content)
        if content['status'] != 'ok' and not content.get('engine_info'):
            # not an engine failure, don't expect output
            out_future = self._output_futures.get(msg_id)
            if out_future and not out_future.done():
                out_future.set_result(None)
        if future:
            future.set_result(self.results[msg_id])

    def _make_io_loop(self):
        """Make my IOLoop. Override with IOLoop.current to return"""
        if 'asyncio' in sys.modules:
            # tornado 5 on asyncio requires creating a new asyncio loop
            import asyncio
            try:
                asyncio.get_event_loop()
            except RuntimeError:
                # no asyncio loop, make one
                # e.g.
                asyncio.set_event_loop(asyncio.new_event_loop())
        loop = IOLoop()
        loop.make_current()
        return loop

    def _stop_io_thread(self):
        """Stop my IO thread"""
        if self._io_loop:
            self._io_loop.add_callback(self._io_loop.stop)
        if self._io_thread and self._io_thread is not current_thread():
            self._io_thread.join()

    def _setup_streams(self):
        self._query_stream = ZMQStream(self._query_socket, self._io_loop)
        self._query_stream.on_recv(self._dispatch_single_reply, copy=False)
        self._control_stream = ZMQStream(self._control_socket, self._io_loop)
        self._control_stream.on_recv(self._dispatch_single_reply, copy=False)
        self._mux_stream = ZMQStream(self._mux_socket, self._io_loop)
        self._mux_stream.on_recv(self._dispatch_reply, copy=False)
        self._task_stream = ZMQStream(self._task_socket, self._io_loop)
        self._task_stream.on_recv(self._dispatch_reply, copy=False)
        self._iopub_stream = ZMQStream(self._iopub_socket, self._io_loop)
        self._iopub_stream.on_recv(self._dispatch_iopub, copy=False)
        self._notification_stream = ZMQStream(self._notification_socket, self._io_loop)
        self._notification_stream.on_recv(self._dispatch_notification, copy=False)

    def _start_io_thread(self):
        """Start IOLoop in a background thread."""
        evt = Event()
        self._io_thread = Thread(target=self._io_main, args=(evt,))
        self._io_thread.daemon = True
        self._io_thread.start()
        # wait for the IOLoop to start
        for i in range(10):
            if evt.wait(1):
                return
            if not self._io_thread.is_alive():
                raise RuntimeError("IO Loop failed to start")
        else:
            raise RuntimeError("Start event was never set. Maybe a problem in the IO thread.")

    def _io_main(self, start_evt=None):
        """main loop for background IO thread"""
        self._io_loop = self._make_io_loop()
        self._setup_streams()
        # signal that start has finished
        # so that the main thread knows that all our attributes are defined
        if start_evt:
            start_evt.set()
        self._io_loop.start()
        self._io_loop.close()

    @unpack_message
    def _dispatch_single_reply(self, msg):
        """Dispatch single (non-execution) replies"""
        msg_id = msg['parent_header'].get('msg_id', None)
        future = self._futures.get(msg_id)
        if future is not None:
            future.set_result(msg)

    @unpack_message
    def _dispatch_notification(self, msg):
        """Dispatch notification messages"""
        msg_type = msg['header']['msg_type']
        handler = self._notification_handlers.get(msg_type, None)
        if handler is None:
            raise KeyError("Unhandled notification message type: %s" % msg_type)
        else:
            handler(msg)

    @unpack_message
    def _dispatch_reply(self, msg):
        """handle execution replies waiting in ZMQ queue."""
        msg_type = msg['header']['msg_type']
        handler = self._queue_handlers.get(msg_type, None)
        if handler is None:
            raise KeyError("Unhandled reply message type: %s" % msg_type)
        else:
            handler(msg)

    @unpack_message
    def _dispatch_iopub(self, msg):
        """handler for IOPub messages"""
        parent = msg['parent_header']
        if not parent or parent['session'] != self.session.session:
            # ignore IOPub messages not from here
            return
        msg_id = parent['msg_id']
        content = msg['content']
        header = msg['header']
        msg_type = msg['header']['msg_type']
        
        if msg_type == 'status' and msg_id not in self.metadata:
            # ignore status messages if they aren't mine
            return

        # init metadata:
        md = self.metadata[msg_id]

        if msg_type == 'stream':
            name = content['name']
            s = md[name] or ''
            md[name] = s + content['text']
        elif msg_type == 'error':
            md.update({'error' : self._unwrap_exception(content)})
        elif msg_type == 'execute_input':
            md.update({'execute_input' : content['code']})
        elif msg_type == 'display_data':
            md['outputs'].append(content)
        elif msg_type == 'execute_result':
            md['execute_result'] = content
        elif msg_type == 'data_message':
            data, remainder = serialize.deserialize_object(msg['buffers'])
            md['data'].update(data)
        elif msg_type == 'status':
            # idle message comes after all outputs
            if content['execution_state'] == 'idle':
                future = self._output_futures.get(msg_id)
                if future and not future.done():
                    # TODO: should probably store actual outputs on the Future
                    future.set_result(None)
        else:
            # unhandled msg_type (status, etc.)
            pass
    
    def _send(self, socket, msg_type, content=None, parent=None, ident=None,
              buffers=None, track=False, header=None, metadata=None):
        """Send a message in the IO thread
        
        returns msg object"""
        if self._closed:
            raise IOError("Connections have been closed.")
        msg = self.session.msg(msg_type, content=content, parent=parent,
                               header=header, metadata=metadata)
        msg_id = msg['header']['msg_id']
        asyncresult = False
        if msg_type in {'execute_request', 'apply_request'}:
            asyncresult = True
            # add future for output
            self._output_futures[msg_id] = output = MessageFuture(msg_id)
            # hook up metadata
            output.metadata = self.metadata[msg_id]
            
        
        self._futures[msg_id] = future = MessageFuture(msg_id, track=track)
        futures = [future]
        
        if asyncresult:
            future.output = output
            futures.append(output)
            output.metadata['submitted'] = util.utcnow()
        
        def cleanup(f):
            """Purge caches on Future resolution"""
            self.results.pop(msg_id, None)
            self._futures.pop(msg_id, None)
            self._output_futures.pop(msg_id, None)
            self.metadata.pop(msg_id, None)
            
        multi_future(futures).add_done_callback(cleanup)
        
        def _really_send():
            sent = self.session.send(socket, msg, track=track, buffers=buffers, ident=ident)
            if track:
                future.tracker.set_result(sent['tracker'])
        
        # hand off actual send to IO thread
        self._io_loop.add_callback(_really_send)
        return future
    
    def _send_recv(self, *args, **kwargs):
        """Send a message in the IO thread and return its reply"""
        future = self._send(*args, **kwargs)
        future.wait()
        return future.result()

    #--------------------------------------------------------------------------
    # len, getitem
    #--------------------------------------------------------------------------

    def __len__(self):
        """len(client) returns # of engines."""
        return len(self.ids)

    def __getitem__(self, key):
        """index access returns DirectView multiplexer objects

        Must be int, slice, or list/tuple/xrange of ints"""
        if not isinstance(key, (int, slice, tuple, list, xrange)):
            raise TypeError("key by int/slice/iterable of ints only, not %s"%(type(key)))
        else:
            return self.direct_view(key)
    
    def __iter__(self):
        """Since we define getitem, Client is iterable
        
        but unless we also define __iter__, it won't work correctly unless engine IDs
        start at zero and are continuous.
        """
        for eid in self.ids:
            yield self.direct_view(eid)
    
    #--------------------------------------------------------------------------
    # Begin public methods
    #--------------------------------------------------------------------------

    @property
    def ids(self):
        """Always up-to-date ids property."""
        # always copy:
        return list(self._ids)

    def activate(self, targets='all', suffix=''):
        """Create a DirectView and register it with IPython magics
        
        Defines the magics `%px, %autopx, %pxresult, %%px`
        
        Parameters
        ----------
        
        targets: int, list of ints, or 'all'
            The engines on which the view's magics will run
        suffix: str [default: '']
            The suffix, if any, for the magics.  This allows you to have
            multiple views associated with parallel magics at the same time.
            
            e.g. ``rc.activate(targets=0, suffix='0')`` will give you
            the magics ``%px0``, ``%pxresult0``, etc. for running magics just
            on engine 0.
        """
        view = self.direct_view(targets)
        view.block = True
        view.activate(suffix)
        return view

    def close(self, linger=None):
        """Close my zmq Sockets
        
        If `linger`, set the zmq LINGER socket option,
        which allows discarding of messages.
        """
        if self._closed:
            return
        self._stop_io_thread()
        snames = [ trait for trait in self.trait_names() if trait.endswith("socket") ]
        for name in snames:
            socket = getattr(self, name)
            if socket is not None and not socket.closed:
                if linger is not None:
                    socket.close(linger=linger)
                else:
                    socket.close()
        self._closed = True

    def spin_thread(self, interval=1):
        """DEPRECATED, DOES NOTHING"""
        warnings.warn("Client.spin_thread is deprecated now that IO is always in a thread", DeprecationWarning)
    
    def stop_spin_thread(self):
        """DEPRECATED, DOES NOTHING"""
        warnings.warn("Client.spin_thread is deprecated now that IO is always in a thread", DeprecationWarning)

    def spin(self):
        """DEPRECATED, DOES NOTHING"""
        warnings.warn("Client.spin is deprecated now that IO is in a thread", DeprecationWarning)

    
    def _await_futures(self, futures, timeout):
        """Wait for a collection of futures"""
        if not futures:
            return True
        
        event = Event()
        if timeout and timeout < 0:
            timeout = None
        
        f = multi_future(futures)
        f.add_done_callback(lambda f: event.set())
        return event.wait(timeout)
    
    def _futures_for_msgs(self, msg_ids):
        """Turn msg_ids into Futures
        
        msg_ids not in futures dict are presumed done.
        """
        futures = []
        for msg_id in msg_ids:
            f = self._futures.get(msg_id, None)
            if f:
                futures.append(f)
        return futures

    def wait(self, jobs=None, timeout=-1):
        """waits on one or more `jobs`, for up to `timeout` seconds.

        Parameters
        ----------

        jobs : int, str, or list of ints and/or strs, or one or more AsyncResult objects
                ints are indices to self.history
                strs are msg_ids
                default: wait on all outstanding messages
        timeout : float
                a time in seconds, after which to give up.
                default is -1, which means no timeout

        Returns
        -------

        True : when all msg_ids are done
        False : timeout reached, some msg_ids still outstanding
        """
        futures = []
        if jobs is None:
            if not self.outstanding:
                return True
            # make a copy, so that we aren't passing a mutable collection to _futures_for_msgs
            theids = set(self.outstanding)
        else:
            if isinstance(jobs, string_types + (int, AsyncResult)) \
            or not isinstance(jobs, collections.Iterable):
                jobs = [jobs]
            theids = set()
            for job in jobs:
                if isinstance(job, int):
                    # index access
                    job = self.history[job]
                elif isinstance(job, AsyncResult):
                    theids.update(job.msg_ids)
                    continue
                elif _is_future(job):
                    futures.append(job)
                    continue
                theids.add(job)
            if not futures and not theids.intersection(self.outstanding):
                return True
        
        futures.extend(self._futures_for_msgs(theids))
        return self._await_futures(futures, timeout)
    
    def wait_interactive(self, jobs=None, interval=1., timeout=-1.):
        """Wait interactively for jobs
        
        If no job is specified, will wait for all outstanding jobs to complete.
        """
        if jobs is None:
            # get futures for results
            futures = [ f for f in self._futures.values() if hasattr(f, 'output') ]
            ar = AsyncResult(self, futures, owner=False)
        else:
            ar = self._asyncresult_from_jobs(jobs, owner=False)
        return ar.wait_interactive(interval=interval, timeout=timeout)
    
    #--------------------------------------------------------------------------
    # Control methods
    #--------------------------------------------------------------------------

    def clear(self, targets=None, block=None):
        """Clear the namespace in target(s)."""
        block = self.block if block is None else block
        targets = self._build_targets(targets)[0]
        futures = []
        for t in targets:
            futures.append(self._send(self._control_stream, 'clear_request', content={}, ident=t))
        if not block:
            return multi_future(futures)
        for future in futures:
            future.wait()
            msg = future.result()
            if msg['content']['status'] != 'ok':
                raise self._unwrap_exception(msg['content'])


    def abort(self, jobs=None, targets=None, block=None):
        """Abort specific jobs from the execution queues of target(s).

        This is a mechanism to prevent jobs that have already been submitted
        from executing.

        Parameters
        ----------

        jobs : msg_id, list of msg_ids, or AsyncResult
            The jobs to be aborted
            
            If unspecified/None: abort all outstanding jobs.

        """
        block = self.block if block is None else block
        jobs = jobs if jobs is not None else list(self.outstanding)
        targets = self._build_targets(targets)[0]
        
        msg_ids = []
        if isinstance(jobs, string_types + (AsyncResult,)):
            jobs = [jobs]
        bad_ids = [obj for obj in jobs if not isinstance(obj, string_types + (AsyncResult,))]
        if bad_ids:
            raise TypeError("Invalid msg_id type %r, expected str or AsyncResult"%bad_ids[0])
        for j in jobs:
            if isinstance(j, AsyncResult):
                msg_ids.extend(j.msg_ids)
            else:
                msg_ids.append(j)
        content = dict(msg_ids=msg_ids)
        futures = []
        for t in targets:
            futures.append(self._send(self._control_stream, 'abort_request',
                    content=content, ident=t))
        
        if not block:
            return multi_future(futures)
        else:
            for f in futures:
                f.wait()
                msg = f.result()
                if msg['content']['status'] != 'ok':
                    raise self._unwrap_exception(msg['content'])

    def shutdown(self, targets='all', restart=False, hub=False, block=None):
        """Terminates one or more engine processes, optionally including the hub.
        
        Parameters
        ----------
        
        targets: list of ints or 'all' [default: all]
            Which engines to shutdown.
        hub: bool [default: False]
            Whether to include the Hub.  hub=True implies targets='all'.
        block: bool [default: self.block]
            Whether to wait for clean shutdown replies or not.
        restart: bool [default: False]
            NOT IMPLEMENTED
            whether to restart engines after shutting them down.
        """
        from ipyparallel.error import NoEnginesRegistered
        if restart:
            raise NotImplementedError("Engine restart is not yet implemented")
        
        block = self.block if block is None else block
        if hub:
            targets = 'all'
        try:
            targets = self._build_targets(targets)[0]
        except NoEnginesRegistered:
            targets = []
        
        futures = []
        for t in targets:
            futures.append(self._send(self._control_stream, 'shutdown_request',
                        content={'restart':restart},ident=t))
        error = False
        if block or hub:
            for f in futures:
                f.wait()
                msg = f.result()
                if msg['content']['status'] != 'ok':
                    error = self._unwrap_exception(msg['content'])

        if hub:
            # don't trigger close on shutdown notification, which will prevent us from receiving the reply
            self._notification_handlers['shutdown_notification'] = lambda msg: None
            msg = self._send_recv(self._query_stream, 'shutdown_request')
            if msg['content']['status'] != 'ok':
                error = self._unwrap_exception(msg['content'])
            if not error:
                self.close()

        if error:
            raise error

    def become_dask(self, targets='all', port=0, nanny=False, scheduler_args=None, **worker_args):
        """Turn the IPython cluster into a dask.distributed cluster

        Parameters
        ----------

        targets: target spec (default: all)
            Which engines to turn into dask workers.
        port: int (default: random)
            Which port
        nanny: bool (default: False)
            Whether to start workers as subprocesses instead of in the engine process.
            Using a nanny allows restarting the worker processes via ``executor.restart``.
        scheduler_args: dict
            Keyword arguments (e.g. ip) to pass to the distributed.Scheduler constructor.
        **worker_args:
            Any additional keyword arguments (e.g. ncores) are passed to the distributed.Worker constructor.

        Returns
        -------

        client = distributed.Client
            A dask.distributed.Client connected to the dask cluster.
        """
        import distributed

        dview = self.direct_view(targets)

        if scheduler_args is None:
            scheduler_args = {}
        else:
            scheduler_args = dict(scheduler_args) # copy

        # Start a Scheduler on the Hub:
        reply = self._send_recv(self._query_stream, 'become_dask_request',
            {'scheduler_args': scheduler_args},
        )
        if reply['content']['status'] != 'ok':
            raise self._unwrap_exception(reply['content'])
        distributed_info = reply['content']

        # Start a Worker on the selected engines:
        worker_args['ip'] = distributed_info['ip']
        worker_args['port'] = distributed_info['port']
        worker_args['nanny'] = nanny
        # set default ncores=1, since that's how an IPython cluster is typically set up.
        worker_args.setdefault('ncores', 1)
        dview.apply_sync(util.become_dask_worker, **worker_args)

        # Finally, return a Client connected to the Scheduler
        try:
            distributed_Client = distributed.Client
        except AttributeError:
            # For distributed pre-1.18.1
            distributed_Client = distributed.Executor

        client = distributed_Client('{ip}:{port}'.format(**distributed_info))

        return client


    def stop_dask(self, targets='all'):
        """Stop the distributed Scheduler and Workers started by become_dask.

        Parameters
        ----------

        targets: target spec (default: all)
            Which engines to stop dask workers on.
        """
        dview = self.direct_view(targets)

        # Start a Scheduler on the Hub:
        reply = self._send_recv(self._query_stream, 'stop_distributed_request')
        if reply['content']['status'] != 'ok':
            raise self._unwrap_exception(reply['content'])

        # Finally, stop all the Workers on the engines
        dview.apply_sync(util.stop_distributed_worker)
    
    # aliases:
    become_distributed = become_dask
    stop_distributed = stop_dask
    
    #--------------------------------------------------------------------------
    # Execution related methods
    #--------------------------------------------------------------------------

    def _maybe_raise(self, result):
        """wrapper for maybe raising an exception if apply failed."""
        if isinstance(result, error.RemoteError):
            raise result

        return result

    def send_apply_request(self, socket, f, args=None, kwargs=None, metadata=None, track=False,
                            ident=None):
        """construct and send an apply message via a socket.

        This is the principal method with which all engine execution is performed by views.
        """

        if self._closed:
            raise RuntimeError("Client cannot be used after its sockets have been closed")
        
        # defaults:
        args = args if args is not None else []
        kwargs = kwargs if kwargs is not None else {}
        metadata = metadata if metadata is not None else {}

        # validate arguments
        if not callable(f) and not isinstance(f, (Reference, PrePickled)):
            raise TypeError("f must be callable, not %s"%type(f))
        if not isinstance(args, (tuple, list)):
            raise TypeError("args must be tuple or list, not %s"%type(args))
        if not isinstance(kwargs, dict):
            raise TypeError("kwargs must be dict, not %s"%type(kwargs))
        if not isinstance(metadata, dict):
            raise TypeError("metadata must be dict, not %s"%type(metadata))

        bufs = serialize.pack_apply_message(f, args, kwargs,
            buffer_threshold=self.session.buffer_threshold,
            item_threshold=self.session.item_threshold,
        )

        future = self._send(socket, "apply_request", buffers=bufs, ident=ident,
                            metadata=metadata, track=track)

        msg_id = future.msg_id
        self.outstanding.add(msg_id)
        if ident:
            # possibly routed to a specific engine
            if isinstance(ident, list):
                ident = ident[-1]
            if ident in self._engines.values():
                # save for later, in case of engine death
                self._outstanding_dict[ident].add(msg_id)
        self.history.append(msg_id)

        return future

    def send_execute_request(self, socket, code, silent=True, metadata=None, ident=None):
        """construct and send an execute request via a socket.

        """

        if self._closed:
            raise RuntimeError("Client cannot be used after its sockets have been closed")
        
        # defaults:
        metadata = metadata if metadata is not None else {}

        # validate arguments
        if not isinstance(code, string_types):
            raise TypeError("code must be text, not %s" % type(code))
        if not isinstance(metadata, dict):
            raise TypeError("metadata must be dict, not %s" % type(metadata))
        
        content = dict(code=code, silent=bool(silent), user_expressions={})


        future = self._send(socket, "execute_request", content=content, ident=ident,
                            metadata=metadata)

        msg_id = future.msg_id
        self.outstanding.add(msg_id)
        if ident:
            # possibly routed to a specific engine
            if isinstance(ident, list):
                ident = ident[-1]
            if ident in self._engines.values():
                # save for later, in case of engine death
                self._outstanding_dict[ident].add(msg_id)
        self.history.append(msg_id)
        self.metadata[msg_id]['submitted'] = util.utcnow()

        return future

    #--------------------------------------------------------------------------
    # construct a View object
    #--------------------------------------------------------------------------

    def load_balanced_view(self, targets=None, **kwargs):
        """construct a DirectView object.

        If no arguments are specified, create a LoadBalancedView
        using all engines.

        Parameters
        ----------

        targets: list,slice,int,etc. [default: use all engines]
            The subset of engines across which to load-balance execution
        kwargs: passed to LoadBalancedView
        """
        if targets == 'all':
            targets = None
        if targets is not None:
            targets = self._build_targets(targets)[1]
        return LoadBalancedView(client=self, socket=self._task_stream, targets=targets,
                                **kwargs)
    
    def executor(self, targets=None):
        """Construct a PEP-3148 Executor with a LoadBalancedView
        
        Parameters
        ----------

        targets: list,slice,int,etc. [default: use all engines]
            The subset of engines across which to load-balance execution
        
        Returns
        -------

        executor: Executor
            The Executor object
        """
        return self.load_balanced_view(targets).executor

    def direct_view(self, targets='all', **kwargs):
        """construct a DirectView object.

        If no targets are specified, create a DirectView using all engines.
        
        rc.direct_view('all') is distinguished from rc[:] in that 'all' will
        evaluate the target engines at each execution, whereas rc[:] will connect to
        all *current* engines, and that list will not change.
        
        That is, 'all' will always use all engines, whereas rc[:] will not use
        engines added after the DirectView is constructed.

        Parameters
        ----------

        targets: list,slice,int,etc. [default: use all engines]
            The engines to use for the View
        kwargs: passed to DirectView
        """
        single = isinstance(targets, int)
        # allow 'all' to be lazily evaluated at each execution
        if targets != 'all':
            targets = self._build_targets(targets)[1]
        if single:
            targets = targets[0]
        return DirectView(client=self, socket=self._mux_stream, targets=targets,
                          **kwargs)

    #--------------------------------------------------------------------------
    # Query methods
    #--------------------------------------------------------------------------
    
    def get_result(self, indices_or_msg_ids=None, block=None, owner=True):
        """Retrieve a result by msg_id or history index, wrapped in an AsyncResult object.

        If the client already has the results, no request to the Hub will be made.

        This is a convenient way to construct AsyncResult objects, which are wrappers
        that include metadata about execution, and allow for awaiting results that
        were not submitted by this Client.

        It can also be a convenient way to retrieve the metadata associated with
        blocking execution, since it always retrieves

        Examples
        --------
        ::

            In [10]: r = client.apply()

        Parameters
        ----------

        indices_or_msg_ids : integer history index, str msg_id, AsyncResult,
            or a list of same.
            The indices or msg_ids of indices to be retrieved

        block : bool
            Whether to wait for the result to be done
        owner : bool [default: True]
            Whether this AsyncResult should own the result.
            If so, calling `ar.get()` will remove data from the
            client's result and metadata cache.
            There should only be one owner of any given msg_id.

        Returns
        -------

        AsyncResult
            A single AsyncResult object will always be returned.

        AsyncHubResult
            A subclass of AsyncResult that retrieves results from the Hub

        """
        block = self.block if block is None else block
        if indices_or_msg_ids is None:
            indices_or_msg_ids = -1

        ar = self._asyncresult_from_jobs(indices_or_msg_ids, owner=owner)

        if block:
            ar.wait()

        return ar

    def resubmit(self, indices_or_msg_ids=None, metadata=None, block=None):
        """Resubmit one or more tasks.

        in-flight tasks may not be resubmitted.

        Parameters
        ----------

        indices_or_msg_ids : integer history index, str msg_id, or list of either
            The indices or msg_ids of indices to be retrieved

        block : bool
            Whether to wait for the result to be done

        Returns
        -------

        AsyncHubResult
            A subclass of AsyncResult that retrieves results from the Hub

        """
        block = self.block if block is None else block
        if indices_or_msg_ids is None:
            indices_or_msg_ids = -1

        theids = self._msg_ids_from_jobs(indices_or_msg_ids)
        content = dict(msg_ids = theids)

        reply = self._send_recv(self._query_stream, 'resubmit_request', content)
        content = reply['content']
        if content['status'] != 'ok':
            raise self._unwrap_exception(content)
        mapping = content['resubmitted']
        new_ids = [ mapping[msg_id] for msg_id in theids ]

        ar = AsyncHubResult(self, new_ids)

        if block:
            ar.wait()

        return ar

    def result_status(self, msg_ids, status_only=True):
        """Check on the status of the result(s) of the apply request with `msg_ids`.

        If status_only is False, then the actual results will be retrieved, else
        only the status of the results will be checked.

        Parameters
        ----------

        msg_ids : list of msg_ids
            if int:
                Passed as index to self.history for convenience.
        status_only : bool (default: True)
            if False:
                Retrieve the actual results of completed tasks.

        Returns
        -------

        results : dict
            There will always be the keys 'pending' and 'completed', which will
            be lists of msg_ids that are incomplete or complete. If `status_only`
            is False, then completed results will be keyed by their `msg_id`.
        """
        theids = self._msg_ids_from_jobs(msg_ids)

        completed = []
        local_results = {}

        # comment this block out to temporarily disable local shortcut:
        for msg_id in theids:
            if msg_id in self.results:
                completed.append(msg_id)
                local_results[msg_id] = self.results[msg_id]
                theids.remove(msg_id)

        if theids: # some not locally cached
            content = dict(msg_ids=theids, status_only=status_only)
            reply = self._send_recv(self._query_stream, "result_request", content=content)
            content = reply['content']
            if content['status'] != 'ok':
                raise self._unwrap_exception(content)
            buffers = reply['buffers']
        else:
            content = dict(completed=[],pending=[])

        content['completed'].extend(completed)

        if status_only:
            return content

        failures = []
        # load cached results into result:
        content.update(local_results)

        # update cache with results:
        for msg_id in sorted(theids):
            if msg_id in content['completed']:
                rec = content[msg_id]
                parent = util.extract_dates(rec['header'])
                header = util.extract_dates(rec['result_header'])
                rcontent = rec['result_content']
                iodict = rec['io']
                if isinstance(rcontent, str):
                    rcontent = self.session.unpack(rcontent)

                md = self.metadata[msg_id]
                md_msg = dict(
                    content=rcontent,
                    parent_header=parent,
                    header=header,
                    metadata=rec['result_metadata'],
                )
                md.update(self._extract_metadata(md_msg))
                if rec.get('received'):
                    md['received'] = util._parse_date(rec['received'])
                md.update(iodict)
                
                if rcontent['status'] == 'ok':
                    if header['msg_type'] == 'apply_reply':
                        res,buffers = serialize.deserialize_object(buffers)
                    elif header['msg_type'] == 'execute_reply':
                        res = ExecuteReply(msg_id, rcontent, md)
                    else:
                        raise KeyError("unhandled msg type: %r" % header['msg_type'])
                else:
                    res = self._unwrap_exception(rcontent)
                    failures.append(res)

                self.results[msg_id] = res
                content[msg_id] = res

        if len(theids) == 1 and failures:
            raise failures[0]

        error.collect_exceptions(failures, "result_status")
        return content

    def queue_status(self, targets='all', verbose=False):
        """Fetch the status of engine queues.

        Parameters
        ----------

        targets : int/str/list of ints/strs
                the engines whose states are to be queried.
                default : all
        verbose : bool
                Whether to return lengths only, or lists of ids for each element
        """
        if targets == 'all':
            # allow 'all' to be evaluated on the engine
            engine_ids = None
        else:
            engine_ids = self._build_targets(targets)[1]
        content = dict(targets=engine_ids, verbose=verbose)
        reply = self._send_recv(self._query_stream, "queue_request", content=content)
        content = reply['content']
        status = content.pop('status')
        if status != 'ok':
            raise self._unwrap_exception(content)
        content = util.int_keys(content)
        if isinstance(targets, int):
            return content[targets]
        else:
            return content

    def _msg_ids_from_target(self, targets=None):
        """Build a list of msg_ids from the list of engine targets"""
        if not targets: # needed as _build_targets otherwise uses all engines
            return []
        target_ids = self._build_targets(targets)[0] 
        return [md_id for md_id in self.metadata if self.metadata[md_id]["engine_uuid"] in target_ids]
    
    def _msg_ids_from_jobs(self, jobs=None):
        """Given a 'jobs' argument, convert it to a list of msg_ids.
        
        Can be either one or a list of:
        
        - msg_id strings
        - integer indices to this Client's history
        - AsyncResult objects
        """
        if not isinstance(jobs, (list, tuple, set, types.GeneratorType)):
            jobs = [jobs]
        msg_ids = []
        for job in jobs:
            if isinstance(job, int):
                msg_ids.append(self.history[job])
            elif isinstance(job, string_types):
                msg_ids.append(job)
            elif isinstance(job, AsyncResult):
                msg_ids.extend(job.msg_ids)
            else:
                raise TypeError("Expected msg_id, int, or AsyncResult, got %r" % job)
        return msg_ids
    
    def _asyncresult_from_jobs(self, jobs=None, owner=False):
        """Construct an AsyncResult from msg_ids or asyncresult objects"""
        if not isinstance(jobs, (list, tuple, set, types.GeneratorType)):
            single = True
            jobs = [jobs]
        else:
            single = False
        futures = []
        msg_ids = []
        for job in jobs:
            if isinstance(job, int):
                job = self.history[job]
            if isinstance(job, string_types):
                if job in self._futures:
                    futures.append(job)
                elif job in self.results:
                    f = MessageFuture(job)
                    f.set_result(self.results[job])
                    f.output = Future()
                    f.output.metadata = self.metadata[job]
                    f.output.set_result(None)
                    futures.append(f)
                else:
                    msg_ids.append(job)
            elif isinstance(job, AsyncResult):
                if job._children:
                    futures.extend(job._children)
                else:
                    msg_ids.extend(job.msg_ids)
            else:
                raise TypeError("Expected msg_id, int, or AsyncResult, got %r" % job)
        if msg_ids:
            if single:
                msg_ids = msg_ids[0]
            return AsyncHubResult(self, msg_ids, owner=owner)
        else:
            if single and futures:
                futures = futures[0]
            return AsyncResult(self, futures, owner=owner)
    
    def purge_local_results(self, jobs=[], targets=[]):
        """Clears the client caches of results and their metadata.

        Individual results can be purged by msg_id, or the entire
        history of specific targets can be purged.

        Use `purge_local_results('all')` to scrub everything from the Clients's
        results and metadata caches.

        After this call all `AsyncResults` are invalid and should be discarded.

        If you must "reget" the results, you can still do so by using
        `client.get_result(msg_id)` or `client.get_result(asyncresult)`. This will
        redownload the results from the hub if they are still available
        (i.e `client.purge_hub_results(...)` has not been called.        

        Parameters
        ----------

        jobs : str or list of str or AsyncResult objects
                the msg_ids whose results should be purged.
        targets : int/list of ints
                The engines, by integer ID, whose entire result histories are to be purged.

        Raises
        ------

        RuntimeError : if any of the tasks to be purged are still outstanding.

        """
        if not targets and not jobs:
            raise ValueError("Must specify at least one of `targets` and `jobs`")
        
        if jobs == 'all':
            if self.outstanding:
                raise RuntimeError("Can't purge outstanding tasks: %s" % self.outstanding)
            self.results.clear()
            self.metadata.clear()
            self._futures.clear()
            self._output_futures.clear()
        else:
            msg_ids = set()
            msg_ids.update(self._msg_ids_from_target(targets))
            msg_ids.update(self._msg_ids_from_jobs(jobs))
            still_outstanding = self.outstanding.intersection(msg_ids)
            if still_outstanding:
                raise RuntimeError("Can't purge outstanding tasks: %s" % still_outstanding)
            for mid in msg_ids:
                self.results.pop(mid, None)
                self.metadata.pop(mid, None)
                self._futures.pop(mid, None)
                self._output_futures.pop(mid, None)


    def purge_hub_results(self, jobs=[], targets=[]):
        """Tell the Hub to forget results.

        Individual results can be purged by msg_id, or the entire
        history of specific targets can be purged.

        Use `purge_results('all')` to scrub everything from the Hub's db.

        Parameters
        ----------

        jobs : str or list of str or AsyncResult objects
                the msg_ids whose results should be forgotten.
        targets : int/str/list of ints/strs
                The targets, by int_id, whose entire history is to be purged.

                default : None
        """
        if not targets and not jobs:
            raise ValueError("Must specify at least one of `targets` and `jobs`")
        if targets:
            targets = self._build_targets(targets)[1]

        # construct msg_ids from jobs
        if jobs == 'all':
            msg_ids = jobs
        else:
            msg_ids = self._msg_ids_from_jobs(jobs)

        content = dict(engine_ids=targets, msg_ids=msg_ids)
        reply = self._send_recv(self._query_stream, "purge_request", content=content)
        content = reply['content']
        if content['status'] != 'ok':
            raise self._unwrap_exception(content)

    def purge_results(self,  jobs=[], targets=[]):
        """Clears the cached results from both the hub and the local client
                
        Individual results can be purged by msg_id, or the entire
        history of specific targets can be purged.

        Use `purge_results('all')` to scrub every cached result from both the Hub's and 
        the Client's db.
        
        Equivalent to calling both `purge_hub_results()` and `purge_client_results()` with 
        the same arguments.

        Parameters
        ----------

        jobs : str or list of str or AsyncResult objects
                the msg_ids whose results should be forgotten.
        targets : int/str/list of ints/strs
                The targets, by int_id, whose entire history is to be purged.

                default : None
        """
        self.purge_local_results(jobs=jobs, targets=targets)
        self.purge_hub_results(jobs=jobs, targets=targets)

    def purge_everything(self):
        """Clears all content from previous Tasks from both the hub and the local client
        
        In addition to calling `purge_results("all")` it also deletes the history and 
        other bookkeeping lists.        
        """
        self.purge_results("all")
        self.history = []
        self.session.digest_history.clear()

    def hub_history(self):
        """Get the Hub's history

        Just like the Client, the Hub has a history, which is a list of msg_ids.
        This will contain the history of all clients, and, depending on configuration,
        may contain history across multiple cluster sessions.

        Any msg_id returned here is a valid argument to `get_result`.

        Returns
        -------

        msg_ids : list of strs
                list of all msg_ids, ordered by task submission time.
        """

        reply = self._send_recv(self._query_stream, "history_request", content={})
        content = reply['content']
        if content['status'] != 'ok':
            raise self._unwrap_exception(content)
        else:
            return content['history']

    def db_query(self, query, keys=None):
        """Query the Hub's TaskRecord database

        This will return a list of task record dicts that match `query`

        Parameters
        ----------

        query : mongodb query dict
            The search dict. See mongodb query docs for details.
        keys : list of strs [optional]
            The subset of keys to be returned.  The default is to fetch everything but buffers.
            'msg_id' will *always* be included.
        """
        if isinstance(keys, string_types):
            keys = [keys]
        content = dict(query=query, keys=keys)
        reply = self._send_recv(self._query_stream, "db_request", content=content)
        content = reply['content']
        if content['status'] != 'ok':
            raise self._unwrap_exception(content)

        records = content['records']

        buffer_lens = content['buffer_lens']
        result_buffer_lens = content['result_buffer_lens']
        buffers = reply['buffers']
        has_bufs = buffer_lens is not None
        has_rbufs = result_buffer_lens is not None
        for i,rec in enumerate(records):
            # unpack datetime objects
            for hkey in ('header', 'result_header'):
                if hkey in rec:
                    rec[hkey] = util.extract_dates(rec[hkey])
            for dtkey in ('submitted', 'started', 'completed', 'received'):
                if dtkey in rec:
                    rec[dtkey] = util._parse_date(rec[dtkey])
            # relink buffers
            if has_bufs:
                blen = buffer_lens[i]
                rec['buffers'], buffers = buffers[:blen],buffers[blen:]
            if has_rbufs:
                blen = result_buffer_lens[i]
                rec['result_buffers'], buffers = buffers[:blen],buffers[blen:]

        return records

__all__ = [ 'Client' ]
