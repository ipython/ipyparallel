"""The IPython Controller Hub with 0MQ

This is the master object that handles connections from engines and clients,
and monitors traffic through the various queues.
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import inspect
import json
import os
import sys
import time
from collections import deque
from datetime import datetime

import zmq
from jupyter_client.jsonutil import parse_date
from jupyter_client.session import Session
from tornado import ioloop
from traitlets import (
    Any,
    Bytes,
    Dict,
    Float,
    HasTraits,
    Instance,
    Integer,
    Set,
    Unicode,
    default,
)
from traitlets.config import LoggingConfigurable
from zmq.eventloop.zmqstream import ZMQStream

from ipyparallel import error, util

from ..util import extract_dates
from .heartmonitor import HeartMonitor

# internal:


def _passer(*args, **kwargs):
    return


def _printer(*args, **kwargs):
    print(args)
    print(kwargs)


def empty_record():
    """Return an empty dict with all record keys."""
    return {
        'msg_id': None,
        'header': None,
        'metadata': None,
        'content': None,
        'buffers': None,
        'submitted': None,
        'client_uuid': None,
        'engine_uuid': None,
        'started': None,
        'completed': None,
        'resubmitted': None,
        'received': None,
        'result_header': None,
        'result_metadata': None,
        'result_content': None,
        'result_buffers': None,
        'queue': None,
        'execute_input': None,
        'execute_result': None,
        'error': None,
        'stdout': '',
        'stderr': '',
    }


def ensure_date_is_parsed(header):
    if not isinstance(header['date'], datetime):
        header['date'] = parse_date(header['date'])


def init_record(msg):
    """Initialize a TaskRecord based on a request."""
    header = msg['header']

    ensure_date_is_parsed(header)
    return {
        'msg_id': header['msg_id'],
        'header': header,
        'content': msg['content'],
        'metadata': msg['metadata'],
        'buffers': msg['buffers'],
        'submitted': util.ensure_timezone(header['date']),
        'client_uuid': None,
        'engine_uuid': None,
        'started': None,
        'completed': None,
        'resubmitted': None,
        'received': None,
        'result_header': None,
        'result_metadata': None,
        'result_content': None,
        'result_buffers': None,
        'queue': None,
        'execute_input': None,
        'execute_result': None,
        'error': None,
        'stdout': '',
        'stderr': '',
    }


class EngineConnector(HasTraits):
    """A simple object for accessing the various zmq connections of an object.
    Attributes are:
    id (int): engine ID
    uuid (unicode): engine UUID
    pending: set of msg_ids
    stallback: tornado timeout for stalled registration
    registration_started (float): time when registration began
    """

    id = Integer()
    uuid = Unicode()
    ident = Bytes()
    pending = Set()
    stallback = Any()
    registration_started = Float()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.registration_started:
            self.registration_started = time.monotonic()


class Hub(LoggingConfigurable):
    """The IPython Controller Hub with 0MQ connections

    Parameters
    ==========
    loop: zmq IOLoop instance
    session: Session object
    <removed> context: zmq context for creating new connections (?)
    queue: ZMQStream for monitoring the command queue (SUB)
    query: ZMQStream for engine registration and client queries requests (ROUTER)
    heartbeat: HeartMonitor object checking the pulse of the engines
    notifier: ZMQStream for broadcasting engine registration changes (PUB)
    db: connection to db for out of memory logging of commands
                NotImplemented
    engine_info: dict of zmq connection information for engines to connect
                to the queues.
    client_info: dict of zmq connection information for engines to connect
                to the queues.
    """

    engine_state_file = Unicode()

    # internal data structures:
    ids = Set()  # engine IDs
    by_ident = Dict()  # map bytes identities : int engine id
    engines = Dict()  # map int engine id : EngineConnector
    hearts = Dict()  # map bytes identities : int engine id, only for active heartbeats
    heartmonitor_period = Integer()
    pending = Set()
    queues = Dict()  # pending msg_ids keyed by engine_id
    tasks = Dict()  # pending msg_ids submitted as tasks, keyed by client_id
    completed = Dict()  # completed msg_ids keyed by engine_id
    all_completed = Set()  # completed msg_ids keyed by engine_id
    unassigned = Set()  # set of task msg_ids not yet assigned a destination
    incoming_registrations = Dict()
    registration_timeout = Integer()
    _idcounter = Integer(0)
    distributed_scheduler = Any()

    expect_stopped_hearts = Instance(deque)

    @default("expect_stopped_hearts")
    def _default_expect_stopped_hearts(self):
        # remember the last this-many hearts
        # silences warnings about ignoring stopped hearts for unregistered engines
        # harmless, but noisy if they happen on every unregistration
        return deque(maxlen=1024)

    loop = Instance(ioloop.IOLoop)

    @default("loop")
    def _default_loop(self):
        return ioloop.IOLoop.current()

    # objects from constructor:
    session = Instance(Session)
    query = Instance(ZMQStream, allow_none=True)
    monitor = Instance(ZMQStream, allow_none=True)
    notifier = Instance(ZMQStream, allow_none=True)
    resubmit = Instance(ZMQStream, allow_none=True)
    heartmonitor = Instance(HeartMonitor, allow_none=True)
    db = Instance(object, allow_none=True)
    client_info = Dict()
    engine_info = Dict()

    def __init__(self, **kwargs):
        """
        # universal:
        loop: IOLoop for creating future connections
        session: streamsession for sending serialized data
        # engine:
        queue: ZMQStream for monitoring queue messages
        query: ZMQStream for engine+client registration and client requests
        heartbeat: HeartMonitor object for tracking engines
        # extra:
        db: ZMQStream for db connection (NotImplemented)
        engine_info: zmq address/protocol dict for engine connections
        client_info: zmq address/protocol dict for client connections
        """

        super().__init__(**kwargs)

        # register our callbacks
        self.query.on_recv(self.dispatch_query)
        self.monitor.on_recv(self.dispatch_monitor_traffic)

        self.monitor_handlers = {
            b'in': self.save_queue_request,
            b'out': self.save_queue_result,
            b'intask': self.save_task_request,
            b'outtask': self.save_task_result,
            b'inbcast': self.save_broadcast_request,
            b'outbcast': self.save_broadcast_result,
            b'tracktask': self.save_task_destination,
            b'incontrol': _passer,
            b'outcontrol': _passer,
            b'iopub': self.monitor_iopub_message,
            b'heartmonitor': self.heartmonitor_message,
        }

        self.query_handlers = {
            'queue_request': self.queue_status,
            'result_request': self.get_results,
            'history_request': self.get_history,
            'db_request': self.db_query,
            'purge_request': self.purge_results,
            'load_request': self.check_load,
            'resubmit_request': self.resubmit_task,
            'shutdown_request': self.shutdown_request,
            'registration_request': self.register_engine,
            'unregistration_request': self.unregister_engine,
            'connection_request': self.connection_request,
            'become_dask_request': self.become_dask,
            'stop_distributed_request': self.stop_distributed,
        }

        # ignore resubmit replies
        self.resubmit.on_recv(lambda msg: None, copy=False)

        self.log.info("hub::created hub")

    def new_engine_id(self, requested_id=None):
        """generate a new engine integer id.

        No longer reuse old ids, just count from 0.

        If an id is requested and available, use that.
        Otherwise, use the counter.
        """
        if requested_id is not None:
            if requested_id in self.engines:
                self.log.warning(
                    "Engine id %s in use by engine with uuid=%s",
                    requested_id,
                    self.engines[requested_id].uuid,
                )
            elif requested_id in {ec.id for ec in self.incoming_registrations.values()}:
                self.log.warning(
                    "Engine id %s registration already pending", requested_id
                )
            else:
                self._idcounter = max(requested_id + 1, self._idcounter)
                return requested_id
        newid = self._idcounter
        self._idcounter += 1
        return newid

    # -----------------------------------------------------------------------------
    # message validation
    # -----------------------------------------------------------------------------

    def _validate_targets(self, targets):
        """turn any valid targets argument into a list of integer ids"""
        if targets is None:
            # default to all
            return self.ids

        if isinstance(targets, (int, str)):
            # only one target specified
            targets = [targets]
        _targets = []
        for t in targets:
            # map raw identities to ids
            if isinstance(t, str):
                t = self.by_ident.get(t.encode("utf8", "replace"), t)
            _targets.append(t)
        targets = _targets
        bad_targets = [t for t in targets if t not in self.ids]
        if bad_targets:
            raise IndexError("No Such Engine: %r" % bad_targets)
        if not targets:
            raise IndexError("No Engines Registered")
        return targets

    # -----------------------------------------------------------------------------
    # dispatch methods (1 per stream)
    # -----------------------------------------------------------------------------

    @util.log_errors
    def dispatch_monitor_traffic(self, msg):
        """all ME and Task queue messages come through here, as well as
        IOPub traffic."""
        self.log.debug("monitor traffic: %r", msg[0])
        switch = msg[0]
        try:
            idents, msg = self.session.feed_identities(msg[1:])
        except ValueError:
            idents = []
        if not idents:
            self.log.error("Monitor message without topic: %r", msg)
            return
        handler = self.monitor_handlers.get(switch, None)
        if handler is not None:
            handler(idents, msg)
        else:
            self.log.error("Unrecognized monitor topic: %r", switch)

    @util.log_errors
    async def dispatch_query(self, msg):
        """Route registration requests and queries from clients."""
        try:
            idents, msg = self.session.feed_identities(msg)
        except ValueError:
            idents = []
        if not idents:
            self.log.error("Bad Query Message: %r", msg)
            return
        client_id = idents[0]
        try:
            msg = self.session.deserialize(msg, content=True)
        except Exception:
            content = error.wrap_exception()
            self.log.error("Bad Query Message: %r", msg, exc_info=True)
            self.session.send(
                self.query, "hub_error", ident=client_id, content=content, parent=msg
            )
            return
        # print client_id, header, parent, content
        # switch on message type:
        msg_type = msg['header']['msg_type']
        self.log.info("client::client %r requested %r", client_id, msg_type)
        handler = self.query_handlers.get(msg_type, None)
        try:
            if handler is None:
                raise KeyError("Bad Message Type: %r" % msg_type)
        except Exception:
            content = error.wrap_exception()
            self.log.error("Bad Message Type: %r", msg_type, exc_info=True)
            self.session.send(
                self.query, "hub_error", ident=client_id, content=content, parent=msg
            )
            return

        try:
            f = handler(idents, msg)
            if f and inspect.isawaitable(f):
                await f
        except Exception:
            content = error.wrap_exception()
            self.log.error("Error handling request: %r", msg_type, exc_info=True)
            self.session.send(
                self.query, "hub_error", ident=client_id, content=content, parent=msg
            )

    # ---------------------------------------------------------------------------
    # handler methods (1 per event)
    # ---------------------------------------------------------------------------

    # ----------------------- Heartbeat --------------------------------------

    @util.log_errors
    def heartmonitor_message(self, topics, msg):
        """Handle a message from the heart monitor"""
        try:
            msg = self.session.deserialize(msg)
        except Exception:
            self.log.error(
                "heartmonitor::invalid message %r",
                msg,
                exc_info=True,
            )
            return
        msg_type = msg['header']['msg_type']
        if msg_type == 'new_heart':
            hearts = msg['content']['hearts']
            self.log.info(f"Registering {len(hearts)} new hearts")
            for heart in hearts:
                self.handle_new_heart(heart.encode("utf8"))
        elif msg_type == 'stopped_heart':
            hearts = msg['content']['hearts']
            self.log.warning(f"{len(hearts)} hearts stopped")
            for heart in hearts:
                self.handle_stopped_heart(heart.encode("utf8"))

    def handle_new_heart(self, heart):
        """Handle a new heart that just started beating"""
        self.log.debug("heartbeat::handle_new_heart(%r)", heart)
        if heart not in self.incoming_registrations:
            self.log.info("heartbeat::ignoring new heart: %r", heart)
        else:
            self.finish_registration(heart)

    def handle_stopped_heart(self, heart):
        """Handle notification that heart has stopped"""
        self.log.debug("heartbeat::handle_stopped_heart(%r)", heart)
        eid = self.hearts.get(heart, None)
        if eid is None:
            if heart in self.expect_stopped_hearts:
                log = self.log.debug
            else:
                log = self.log.info
            log(
                "heartbeat::ignoring heart failure %r (probably unregistered already)",
                heart,
            )
        else:
            uuid = self.engines[eid].uuid
            self.unregister_engine(heart, dict(content=dict(id=eid, queue=uuid)))

    # ----------------------- MUX Queue Traffic ------------------------------

    def save_queue_request(self, idents, msg):
        if len(idents) < 2:
            self.log.error("invalid identity prefix: %r", idents)
            return
        queue_id, client_id = idents[:2]
        try:
            msg = self.session.deserialize(msg)
        except Exception:
            self.log.error(
                "queue::client %r sent invalid message to %r: %r",
                client_id,
                queue_id,
                msg,
                exc_info=True,
            )
            return

        eid = self.by_ident.get(queue_id, None)
        if eid is None:
            self.log.error("queue::target %r not registered", queue_id)
            self.log.debug("queue::    valid are: %r", self.by_ident.keys())
            return
        record = init_record(msg)
        msg_id = record['msg_id']
        self.log.info(
            "queue::client %r submitted request %r to %s", client_id, msg_id, eid
        )
        # Unicode in records
        record['engine_uuid'] = queue_id.decode('ascii')
        record['client_uuid'] = msg['header']['session']
        record['queue'] = 'mux'

        try:
            # it's posible iopub arrived first:
            existing = self.db.get_record(msg_id)
            for key, evalue in existing.items():
                rvalue = record.get(key, None)
                if evalue and rvalue and evalue != rvalue:
                    self.log.warning(
                        "conflicting initial state for record: %r:%r <%r> %r",
                        msg_id,
                        rvalue,
                        key,
                        evalue,
                    )
                elif evalue and not rvalue:
                    record[key] = evalue
            try:
                self.db.update_record(msg_id, record)
            except Exception:
                self.log.error("DB Error updating record %r", msg_id, exc_info=True)
        except KeyError:
            try:
                self.db.add_record(msg_id, record)
            except Exception:
                self.log.error("DB Error adding record %r", msg_id, exc_info=True)

        self.pending.add(msg_id)
        self.queues[eid].append(msg_id)

    def save_queue_result(self, idents, msg):
        if len(idents) < 2:
            self.log.error("invalid identity prefix: %r", idents)
            return

        client_id, queue_id = idents[:2]
        try:
            msg = self.session.deserialize(msg)
        except Exception:
            self.log.error(
                "queue::engine %r sent invalid message to %r: %r",
                queue_id,
                client_id,
                msg,
                exc_info=True,
            )
            return

        eid = self.by_ident.get(queue_id, None)
        if eid is None:
            self.log.error("queue::unknown engine %r is sending a reply: ", queue_id)
            return

        parent = msg['parent_header']
        if not parent:
            return
        msg_id = parent['msg_id']
        if msg_id in self.pending:
            self.pending.remove(msg_id)
            self.all_completed.add(msg_id)
            self.queues[eid].remove(msg_id)
            self.completed[eid].append(msg_id)
            self.log.info("queue::request %r completed on %s", msg_id, eid)
        elif msg_id not in self.all_completed:
            # it could be a result from a dead engine that died before delivering the
            # result
            self.log.warning("queue:: unknown msg finished %r", msg_id)
            return
        # update record anyway, because the unregistration could have been premature
        rheader = msg['header']
        md = msg['metadata']
        ensure_date_is_parsed(rheader)
        completed = util.ensure_timezone(rheader['date'])
        started = extract_dates(md.get('started', None))
        result = {
            'result_header': rheader,
            'result_metadata': md,
            'result_content': msg['content'],
            'received': util.utcnow(),
            'started': started,
            'completed': completed,
        }

        result['result_buffers'] = msg['buffers']
        try:
            self.db.update_record(msg_id, result)
        except Exception:
            self.log.error("DB Error updating record %r", msg_id, exc_info=True)

    # --------------------- Broadcast traffic ------------------------------
    def save_broadcast_request(self, idents, msg):
        client_id = idents[0]
        try:
            msg = self.session.deserialize(msg)
        except Exception as e:
            self.log.error(
                f'broadcast:: client {client_id} sent invalid broadcast message:'
                f' {msg}',
                exc_info=True,
            )
            return

        record = init_record(msg)

        record['client_uuid'] = msg['header']['session']
        header = msg['header']
        msg_id = header['msg_id']
        self.pending.add(msg_id)

        try:
            self.db.add_record(msg_id, record)
        except Exception as e:
            self.log.error(f'DB Error adding record {msg_id}', exc_info=True)

    def save_broadcast_result(self, idents, msg):
        client_id = idents[0]
        try:
            msg = self.session.deserialize(msg)
        except Exception as e:
            self.log.error(
                f'broadcast::invalid broadcast result message send to {client_id}:' f''
            )

        # save the result of a completed broadcast
        parent = msg['parent_header']
        if not parent:
            self.log.warning(f'Broadcast message {msg} had no parent')
            return
        msg_id = parent['msg_id']
        header = msg['header']
        md = msg['metadata']
        engine_uuid = md.get('engine', '')
        eid = self.by_ident.get(engine_uuid.encode("utf8"), None)
        status = md.get('status', None)

        if msg_id in self.pending:
            self.log.info(f'broadcast:: broadcast {msg_id} finished on {eid}')
            self.pending.remove(msg_id)
            self.all_completed.add(msg_id)
            if eid is not None and status != 'aborted':
                self.completed[eid].append(msg_id)
            ensure_date_is_parsed(header)
            completed = util.ensure_timezone(header['date'])
            started = extract_dates(md.get('started', None))
            result = {
                'result_header': header,
                'result_metadata': msg['metadata'],
                'result_content': msg['content'],
                'started': started,
                'completed': completed,
                'received': util.utcnow(),
                'engine_uuid': engine_uuid,
                'result_buffers': msg['buffers'],
            }

            try:
                self.db.update_record(msg_id, result)
            except Exception as e:
                self.log.error(
                    f'DB Error saving broadcast result {msg_id}', msg_id, exc_info=True
                )
        else:
            self.log.debug(f'broadcast::unknown broadcast {msg_id} finished')

    # --------------------- Task Queue Traffic ------------------------------

    def save_task_request(self, idents, msg):
        """Save the submission of a task."""
        client_id = idents[0]

        try:
            msg = self.session.deserialize(msg)
        except Exception:
            self.log.error(
                "task::client %r sent invalid task message: %r",
                client_id,
                msg,
                exc_info=True,
            )
            return
        record = init_record(msg)

        record['client_uuid'] = msg['header']['session']
        record['queue'] = 'task'
        header = msg['header']
        msg_id = header['msg_id']
        self.pending.add(msg_id)
        self.unassigned.add(msg_id)
        try:
            # it's posible iopub arrived first:
            existing = self.db.get_record(msg_id)
            if existing['resubmitted']:
                for key in ('submitted', 'client_uuid', 'buffers'):
                    # don't clobber these keys on resubmit
                    # submitted and client_uuid should be different
                    # and buffers might be big, and shouldn't have changed
                    record.pop(key)
                    # still check content,header which should not change
                    # but are not expensive to compare as buffers

            for key, evalue in existing.items():
                if key.endswith('buffers'):
                    # don't compare buffers
                    continue
                rvalue = record.get(key, None)
                if evalue and rvalue and evalue != rvalue:
                    self.log.warning(
                        "conflicting initial state for record: %r:%r <%r> %r",
                        msg_id,
                        rvalue,
                        key,
                        evalue,
                    )
                elif evalue and not rvalue:
                    record[key] = evalue
            try:
                self.db.update_record(msg_id, record)
            except Exception:
                self.log.error("DB Error updating record %r", msg_id, exc_info=True)
        except KeyError:
            try:
                self.db.add_record(msg_id, record)
            except Exception:
                self.log.error("DB Error adding record %r", msg_id, exc_info=True)
        except Exception:
            self.log.error("DB Error saving task request %r", msg_id, exc_info=True)

    def save_task_result(self, idents, msg):
        """save the result of a completed task."""
        client_id = idents[0]
        try:
            msg = self.session.deserialize(msg)
        except Exception:
            self.log.error(
                "task::invalid task result message send to %r: %r",
                client_id,
                msg,
                exc_info=True,
            )
            return

        parent = msg['parent_header']
        if not parent:
            # print msg
            self.log.warning("Task %r had no parent!", msg)
            return
        msg_id = parent['msg_id']
        if msg_id in self.unassigned:
            self.unassigned.remove(msg_id)

        header = msg['header']
        md = msg['metadata']
        engine_uuid = md.get('engine', '')
        eid = self.by_ident.get(engine_uuid.encode("utf8"), None)

        status = md.get('status', None)

        if msg_id in self.pending:
            self.log.info("task::task %r finished on %s", msg_id, eid)
            self.pending.remove(msg_id)
            self.all_completed.add(msg_id)
            if eid is not None:
                if status != 'aborted':
                    self.completed[eid].append(msg_id)
                if msg_id in self.tasks[eid]:
                    self.tasks[eid].remove(msg_id)
            ensure_date_is_parsed(header)
            completed = util.ensure_timezone(header['date'])
            started = extract_dates(md.get('started', None))
            result = {
                'result_header': header,
                'result_metadata': msg['metadata'],
                'result_content': msg['content'],
                'started': started,
                'completed': completed,
                'received': util.utcnow(),
                'engine_uuid': engine_uuid,
            }

            result['result_buffers'] = msg['buffers']
            try:
                self.db.update_record(msg_id, result)
            except Exception:
                self.log.error("DB Error saving task request %r", msg_id, exc_info=True)

        else:
            self.log.debug("task::unknown task %r finished", msg_id)

    def save_task_destination(self, idents, msg):
        try:
            msg = self.session.deserialize(msg, content=True)
        except Exception:
            self.log.error("task::invalid task tracking message", exc_info=True)
            return
        content = msg['content']
        # print (content)
        msg_id = content['msg_id']
        engine_uuid = content['engine_id']
        eid = self.by_ident[engine_uuid.encode("utf8")]

        self.log.info("task::task %r arrived on %r", msg_id, eid)
        if msg_id in self.unassigned:
            self.unassigned.remove(msg_id)
        # else:
        #     self.log.debug("task::task %r not listed as MIA?!"%(msg_id))

        self.tasks[eid].append(msg_id)
        try:
            self.db.update_record(msg_id, dict(engine_uuid=engine_uuid))
        except Exception:
            self.log.error("DB Error saving task destination %r", msg_id, exc_info=True)

    # --------------------- IOPub Traffic ------------------------------

    def monitor_iopub_message(self, topics, msg):
        '''intercept iopub traffic so events can be acted upon'''
        try:
            msg = self.session.deserialize(msg, content=True)
        except Exception:
            self.log.error("iopub::invalid IOPub message", exc_info=True)
            return

        msg_type = msg['header']['msg_type']
        if msg_type == 'shutdown_reply':
            session = msg['header']['session']
            uuid_bytes = session.encode("utf8", "replace")
            eid = self.by_ident.get(uuid_bytes, None)
            if eid is None:
                self.log.error(f"Found no engine for {session}")
                return
            uuid = self.engines[eid].uuid
            self.unregister_engine(
                ident='shutdown_reply', msg=dict(content=dict(id=eid, queue=uuid))
            )

        if msg_type not in (
            'status',
            'shutdown_reply',
        ):
            self.save_iopub_message(topics, msg)

    def save_iopub_message(self, topics, msg):
        """save an iopub message into the db"""
        parent = msg['parent_header']
        if not parent:
            self.log.debug("iopub::IOPub message lacks parent: %r", msg)
            return
        msg_id = parent['msg_id']
        msg_type = msg['header']['msg_type']
        content = msg['content']

        # ensure msg_id is in db
        try:
            rec = self.db.get_record(msg_id)
        except KeyError:
            rec = None

        # stream
        d = {}
        if msg_type == 'stream':
            name = content['name']
            s = '' if rec is None else rec[name]
            d[name] = s + content['text']
        elif msg_type == 'error':
            d['error'] = content
        elif msg_type == 'execute_input':
            d['execute_input'] = content['code']
        elif msg_type in ('display_data', 'execute_result'):
            d[msg_type] = content
        elif msg_type == 'data_pub':
            self.log.info("ignored data_pub message for %s" % msg_id)
        else:
            self.log.warning("unhandled iopub msg_type: %r", msg_type)

        if not d:
            return

        if rec is None:
            # new record
            rec = empty_record()
            rec['msg_id'] = msg_id
            rec.update(d)
            d = rec
            update_record = self.db.add_record
        else:
            update_record = self.db.update_record

        try:
            update_record(msg_id, d)
        except Exception:
            self.log.error("DB Error saving iopub message %r", msg_id, exc_info=True)

    # -------------------------------------------------------------------------
    # Registration requests
    # -------------------------------------------------------------------------

    def connection_request(self, client_id, msg):
        """Reply with connection addresses for clients."""
        self.log.info("client::client %r connected", client_id)
        content = dict(status='ok')
        jsonable = {}
        for eid, ec in self.engines.items():
            jsonable[str(eid)] = ec.uuid
        content['engines'] = jsonable
        self.session.send(
            self.query, 'connection_reply', content, parent=msg, ident=client_id
        )

    def register_engine(self, reg, msg):
        """Begin registration of a new engine."""
        content = msg['content']
        try:
            uuid = content['uuid']
        except KeyError:
            self.log.error("registration::queue not specified", exc_info=True)
            return

        eid = self.new_engine_id(content.get('id'))
        self.log.debug(f"registration::requesting registration {eid}:{uuid}")

        content = dict(
            id=eid,
            status='ok',
            hb_period=self.heartmonitor_period,
            connection_info=self.engine_info,
        )
        # check if requesting available IDs:
        if uuid.encode("utf8") in self.by_ident:
            try:
                raise KeyError("uuid %r in use" % uuid)
            except Exception:
                content = error.wrap_exception()
                self.log.error("uuid %r in use", uuid, exc_info=True)
        else:
            for heart_id, ec in self.incoming_registrations.items():
                if uuid == heart_id:
                    try:
                        raise KeyError("heart_id %r in use" % uuid)
                    except Exception:
                        self.log.error("heart_id %r in use", uuid, exc_info=True)
                        content = error.wrap_exception()
                    break
                elif uuid == ec.uuid:
                    try:
                        raise KeyError("uuid %r in use" % uuid)
                    except Exception:
                        self.log.error("uuid %r in use", uuid, exc_info=True)
                        content = error.wrap_exception()
                    break

        msg = self.session.send(
            self.query, "registration_reply", content=content, ident=reg
        )
        # actually send replies to avoid async starvation during a registration flood
        # it's important that this message has actually been sent
        # before we start the stalled registration countdown
        self.query.flush(zmq.POLLOUT)

        heart = uuid.encode("utf8")

        if content['status'] == 'ok':
            self.log.info(f"registration::accepting registration {eid}:{uuid}")
            t = self.loop.add_timeout(
                self.loop.time() + self.registration_timeout,
                lambda: self._purge_stalled_registration(heart),
            )
            self.incoming_registrations[heart] = EngineConnector(
                id=eid, uuid=uuid, ident=heart, stallback=t
            )
        else:
            self.log.error(
                "registration::registration %i failed: %r", eid, content['evalue']
            )

        return eid

    def unregister_engine(self, ident, msg):
        """Unregister an engine that explicitly requested to leave."""
        try:
            eid = msg['content']['id']
        except Exception:
            self.log.error(
                f"registration::bad request for engine for unregistration: {msg['content']}",
            )
            return
        if eid not in self.engines:
            # engine not registered
            # first, check for still-pending registration
            remove_heart_id = None
            for heart_id, ec in self.incoming_registrations.items():
                if ec.id == eid:
                    remove_heart_id = heart_id
                    break
            if remove_heart_id is not None:
                self.log.info(f"registration::canceling registration {ec.id}:{ec.uuid}")
                self.incoming_registrations.pop(remove_heart_id)
            else:
                self.log.info(
                    f"registration::unregister_engine({eid}) already unregistered"
                )
            return

        self.log.info(f"registration::unregister_engine({eid})")
        ec = self.engines[eid]

        content = dict(id=eid, uuid=ec.uuid)

        # stop the heartbeats
        self.hearts.pop(ec.ident, None)
        self.expect_stopped_hearts.append(ec.ident)

        self.loop.add_timeout(
            self.loop.time() + self.registration_timeout,
            lambda: self._handle_stranded_msgs(eid, ec.uuid),
        )

        # cleanup mappings
        self.by_ident.pop(ec.ident, None)
        self.engines.pop(eid, None)

        self._save_engine_state()

        if self.notifier:
            self.session.send(
                self.notifier, "unregistration_notification", content=content
            )

    def _handle_stranded_msgs(self, eid, uuid):
        """Handle messages known to be on an engine when the engine unregisters.

        It is possible that this will fire prematurely - that is, an engine will
        go down after completing a result, and the client will be notified
        that the result failed and later receive the actual result.
        """

        outstanding = self.queues[eid]

        for msg_id in outstanding:
            self.pending.remove(msg_id)
            self.all_completed.add(msg_id)
            try:
                raise error.EngineError(
                    f"Engine {eid!r} died while running task {msg_id!r}"
                )
            except Exception:
                content = error.wrap_exception()
            # build a fake header:
            header = {}
            header['engine'] = uuid
            header['date'] = util.utcnow()
            rec = dict(result_content=content, result_header=header, result_buffers=[])
            rec['completed'] = util.ensure_timezone(header['date'])
            rec['engine_uuid'] = uuid
            try:
                self.db.update_record(msg_id, rec)
            except Exception:
                self.log.error(
                    "DB Error handling stranded msg %r", msg_id, exc_info=True
                )

    def finish_registration(self, heart):
        """Second half of engine registration, called after our HeartMonitor
        has received a beat from the Engine's Heart."""
        try:
            ec = self.incoming_registrations.pop(heart)
        except KeyError:
            self.log.error(
                f"registration::tried to finish nonexistant registration for {heart}",
                exc_info=True,
            )
            return
        duration_ms = int(1000 * (time.monotonic() - ec.registration_started))
        self.log.info(
            f"registration::finished registering engine {ec.id}:{ec.uuid} in {duration_ms}ms"
        )
        if ec.stallback is not None:
            self.loop.remove_timeout(ec.stallback)
        eid = ec.id
        self.ids.add(eid)
        self.engines[eid] = ec
        self.by_ident[ec.ident] = ec.id
        self.queues[eid] = list()
        self.tasks[eid] = list()
        self.completed[eid] = list()
        self.hearts[heart] = eid
        content = dict(id=eid, uuid=ec.uuid)
        if self.notifier:
            self.session.send(
                self.notifier, "registration_notification", content=content
            )
        self.log.info("engine::Engine Connected: %i", eid)

        self._save_engine_state()

    def _purge_stalled_registration(self, heart):
        # flush monitor messages before purging
        # first heartbeat might be waiting to be handled
        self.monitor.flush()
        if heart in self.incoming_registrations:
            ec = self.incoming_registrations.pop(heart)
            self.log.warning(
                f"registration::purging stalled registration {ec.id}:{ec.uuid}"
            )

    # -------------------------------------------------------------------------
    # Engine State
    # -------------------------------------------------------------------------

    def _cleanup_engine_state_file(self):
        """cleanup engine state mapping"""

        if os.path.exists(self.engine_state_file):
            self.log.debug("cleaning up engine state: %s", self.engine_state_file)
            try:
                os.remove(self.engine_state_file)
            except OSError:
                self.log.error(
                    "Couldn't cleanup file: %s", self.engine_state_file, exc_info=True
                )

    def _save_engine_state(self):
        """save engine mapping to JSON file"""
        if not self.engine_state_file:
            return
        self.log.debug("save engine state to %s" % self.engine_state_file)
        state = {}
        engines = {}
        for eid, ec in self.engines.items():
            engines[eid] = ec.uuid

        state['engines'] = engines

        state['next_id'] = self._idcounter

        with open(self.engine_state_file, 'w') as f:
            json.dump(state, f)

    def _load_engine_state(self):
        """load engine mapping from JSON file"""
        if not os.path.exists(self.engine_state_file):
            return

        self.log.info("loading engine state from %s" % self.engine_state_file)

        with open(self.engine_state_file) as f:
            state = json.load(f)

        save_notifier = self.notifier
        self.notifier = None
        for eid, uuid in state['engines'].items():
            heart = uuid.encode('ascii')

            self.incoming_registrations[heart] = EngineConnector(
                id=int(eid), uuid=uuid, ident=heart
            )
            self.finish_registration(heart)

        self.notifier = save_notifier

        self._idcounter = state['next_id']

    # -------------------------------------------------------------------------
    # Client Requests
    # -------------------------------------------------------------------------

    def shutdown_request(self, client_id, msg):
        """handle shutdown request."""
        self.session.send(
            self.query,
            'shutdown_reply',
            content={'status': 'ok'},
            ident=client_id,
            parent=msg,
        )
        # also notify other clients of shutdown
        self.session.send(
            self.notifier, 'shutdown_notification', content={'status': 'ok'}, parent=msg
        )
        self.loop.add_timeout(self.loop.time() + 1, self._shutdown)

    def _shutdown(self):
        self.log.info("hub::hub shutting down.")
        time.sleep(0.1)
        sys.exit(0)

    def check_load(self, client_id, msg):
        content = msg['content']
        try:
            targets = content['targets']
            targets = self._validate_targets(targets)
        except Exception:
            content = error.wrap_exception()
            self.session.send(
                self.query, "hub_error", content=content, ident=client_id, parent=msg
            )
            return

        content = dict(status='ok')
        # loads = {}
        for t in targets:
            content[bytes(t)] = len(self.queues[t]) + len(self.tasks[t])
        self.session.send(
            self.query, "load_reply", content=content, ident=client_id, parent=msg
        )

    def queue_status(self, client_id, msg):
        """Return the Queue status of one or more targets.

        If verbose, return the msg_ids, else return len of each type.

        Keys:

        * queue (pending MUX jobs)
        * tasks (pending Task jobs)
        * completed (finished jobs from both queues)
        """
        content = msg['content']
        targets = content['targets']
        try:
            targets = self._validate_targets(targets)
        except Exception:
            content = error.wrap_exception()
            self.session.send(
                self.query, "hub_error", content=content, ident=client_id, parent=msg
            )
            return
        verbose = content.get('verbose', False)
        content = dict(status='ok')
        for t in targets:
            queue = self.queues[t]
            completed = self.completed[t]
            tasks = self.tasks[t]
            if not verbose:
                queue = len(queue)
                completed = len(completed)
                tasks = len(tasks)
            content[str(t)] = {'queue': queue, 'completed': completed, 'tasks': tasks}
        content['unassigned'] = (
            list(self.unassigned) if verbose else len(self.unassigned)
        )
        # print (content)
        self.session.send(
            self.query, "queue_reply", content=content, ident=client_id, parent=msg
        )

    def purge_results(self, client_id, msg):
        """Purge results from memory. This method is more valuable before we move
        to a DB based message storage mechanism."""
        content = msg['content']
        self.log.info("Dropping records with %s", content)
        msg_ids = content.get('msg_ids', [])
        reply = dict(status='ok')
        if msg_ids == 'all':
            try:
                self.db.drop_matching_records(dict(completed={'$ne': None}))
            except Exception:
                reply = error.wrap_exception()
                self.log.exception("Error dropping records")
        else:
            pending = [m for m in msg_ids if (m in self.pending)]
            if pending:
                try:
                    raise IndexError("msg pending: %r" % pending[0])
                except Exception:
                    reply = error.wrap_exception()
                    self.log.exception("Error dropping records")
            else:
                try:
                    self.db.drop_matching_records(dict(msg_id={'$in': msg_ids}))
                except Exception:
                    reply = error.wrap_exception()
                    self.log.exception("Error dropping records")

            if reply['status'] == 'ok':
                eids = content.get('engine_ids', [])
                for eid in eids:
                    if eid not in self.engines:
                        try:
                            raise IndexError("No such engine: %i" % eid)
                        except Exception:
                            reply = error.wrap_exception()
                            self.log.exception("Error dropping records")
                        break
                    uid = self.engines[eid].uuid
                    try:
                        self.db.drop_matching_records(
                            dict(engine_uuid=uid, completed={'$ne': None})
                        )
                    except Exception:
                        reply = error.wrap_exception()
                        self.log.exception("Error dropping records")
                        break

        self.session.send(
            self.query, 'purge_reply', content=reply, ident=client_id, parent=msg
        )

    def resubmit_task(self, client_id, msg):
        """Resubmit one or more tasks."""
        parent = msg

        def finish(reply):
            self.session.send(
                self.query,
                'resubmit_reply',
                content=reply,
                ident=client_id,
                parent=parent,
            )

        content = msg['content']
        msg_ids = content['msg_ids']
        reply = dict(status='ok')
        try:
            records = self.db.find_records(
                {'msg_id': {'$in': msg_ids}}, keys=['header', 'content', 'buffers']
            )
        except Exception:
            self.log.error('db::db error finding tasks to resubmit', exc_info=True)
            return finish(error.wrap_exception())

        # validate msg_ids
        found_ids = [rec['msg_id'] for rec in records]
        pending_ids = [msg_id for msg_id in found_ids if msg_id in self.pending]
        if len(records) > len(msg_ids):
            try:
                raise RuntimeError(
                    "DB appears to be in an inconsistent state."
                    "More matching records were found than should exist"
                )
            except Exception:
                self.log.exception("Failed to resubmit task")
                return finish(error.wrap_exception())
        elif len(records) < len(msg_ids):
            missing = [m for m in msg_ids if m not in found_ids]
            try:
                raise KeyError("No such msg(s): %r" % missing)
            except KeyError:
                self.log.exception("Failed to resubmit task")
                return finish(error.wrap_exception())
        elif pending_ids:
            pass
            # no need to raise on resubmit of pending task, now that we
            # resubmit under new ID, but do we want to raise anyway?
            # msg_id = invalid_ids[0]
            # try:
            #     raise ValueError("Task(s) %r appears to be inflight" % )
            # except Exception:
            #     return finish(error.wrap_exception())

        # mapping of original IDs to resubmitted IDs
        resubmitted = {}

        # send the messages
        for rec in records:
            header = rec['header']
            msg = self.session.msg(header['msg_type'], parent=header)
            msg_id = msg['msg_id']
            msg['content'] = rec['content']

            # use the old header, but update msg_id and timestamp
            fresh = msg['header']
            header['msg_id'] = fresh['msg_id']
            header['date'] = fresh['date']
            msg['header'] = header

            self.session.send(self.resubmit, msg, buffers=rec['buffers'])

            resubmitted[rec['msg_id']] = msg_id
            self.pending.add(msg_id)
            msg['buffers'] = rec['buffers']
            try:
                self.db.add_record(msg_id, init_record(msg))
            except Exception:
                self.log.error(
                    "db::DB Error updating record: %s", msg_id, exc_info=True
                )
                return finish(error.wrap_exception())

        finish(dict(status='ok', resubmitted=resubmitted))

        # store the new IDs in the Task DB
        for msg_id, resubmit_id in resubmitted.items():
            try:
                self.db.update_record(msg_id, {'resubmitted': resubmit_id})
            except Exception:
                self.log.error(
                    "db::DB Error updating record: %s", msg_id, exc_info=True
                )

    def _extract_record(self, rec):
        """decompose a TaskRecord dict into subsection of reply for get_result"""
        io_dict = {}
        for key in ('execute_input', 'execute_result', 'error', 'stdout', 'stderr'):
            io_dict[key] = rec[key]
        content = {
            'header': rec['header'],
            'metadata': rec['metadata'],
            'result_metadata': rec['result_metadata'],
            'result_header': rec['result_header'],
            'result_content': rec['result_content'],
            'received': rec['received'],
            'io': io_dict,
        }
        if rec['result_buffers']:
            buffers = list(rec['result_buffers'])
        else:
            buffers = []

        return content, buffers

    def get_results(self, client_id, msg):
        """Get the result of 1 or more messages."""
        content = msg['content']
        msg_ids = sorted(set(content['msg_ids']))
        statusonly = content.get('status_only', False)
        pending = []
        completed = []
        content = dict(status='ok')
        content['pending'] = pending
        content['completed'] = completed
        buffers = []
        if not statusonly:
            try:
                matches = self.db.find_records(dict(msg_id={'$in': msg_ids}))
                # turn match list into dict, for faster lookup
                records = {}
                for rec in matches:
                    records[rec['msg_id']] = rec
            except Exception:
                content = error.wrap_exception()
                self.log.exception("Failed to get results")
                self.session.send(
                    self.query,
                    "result_reply",
                    content=content,
                    parent=msg,
                    ident=client_id,
                )
                return
        else:
            records = {}
        for msg_id in msg_ids:
            if msg_id in self.pending:
                pending.append(msg_id)
            elif msg_id in self.all_completed:
                completed.append(msg_id)
                if not statusonly:
                    c, bufs = self._extract_record(records[msg_id])
                    content[msg_id] = c
                    buffers.extend(bufs)
            elif msg_id in records:
                if rec['completed']:
                    completed.append(msg_id)
                    c, bufs = self._extract_record(records[msg_id])
                    content[msg_id] = c
                    buffers.extend(bufs)
                else:
                    pending.append(msg_id)
            else:
                try:
                    raise KeyError('No such message: ' + msg_id)
                except Exception:
                    content = error.wrap_exception()
                break
        self.session.send(
            self.query,
            "result_reply",
            content=content,
            parent=msg,
            ident=client_id,
            buffers=buffers,
        )

    def get_history(self, client_id, msg):
        """Get a list of all msg_ids in our DB records"""
        try:
            msg_ids = self.db.get_history()
        except Exception as e:
            content = error.wrap_exception()
            self.log.exception("Failed to get history")
        else:
            content = dict(status='ok', history=msg_ids)

        self.session.send(
            self.query, "history_reply", content=content, parent=msg, ident=client_id
        )

    def db_query(self, client_id, msg):
        """Perform a raw query on the task record database."""
        content = msg['content']
        query = extract_dates(content.get('query', {}))
        keys = content.get('keys', None)
        buffers = []
        empty = list()
        try:
            records = self.db.find_records(query, keys)
        except Exception as e:
            content = error.wrap_exception()
            self.log.exception("DB query failed")
        else:
            # extract buffers from reply content:
            if keys is not None:
                buffer_lens = [] if 'buffers' in keys else None
                result_buffer_lens = [] if 'result_buffers' in keys else None
            else:
                buffer_lens = None
                result_buffer_lens = None

            for rec in records:
                # buffers may be None, so double check
                b = rec.pop('buffers', empty) or empty
                if buffer_lens is not None:
                    buffer_lens.append(len(b))
                    buffers.extend(b)
                rb = rec.pop('result_buffers', empty) or empty
                if result_buffer_lens is not None:
                    result_buffer_lens.append(len(rb))
                    buffers.extend(rb)
            content = dict(
                status='ok',
                records=records,
                buffer_lens=buffer_lens,
                result_buffer_lens=result_buffer_lens,
            )
        # self.log.debug (content)
        self.session.send(
            self.query,
            "db_reply",
            content=content,
            parent=msg,
            ident=client_id,
            buffers=buffers,
        )

    async def become_dask(self, client_id, msg):
        """Start a dask.distributed Scheduler."""
        if self.distributed_scheduler is None:
            kwargs = msg['content'].get('scheduler_args', {})
            self.log.info("Becoming dask.distributed scheduler: %s", kwargs)
            from distributed import Scheduler

            self.distributed_scheduler = scheduler = Scheduler(**kwargs)
            await scheduler.start()
        content = {
            'status': 'ok',
            'ip': self.distributed_scheduler.ip,
            'address': self.distributed_scheduler.address,
            'port': self.distributed_scheduler.port,
        }
        self.log.info(
            "dask.distributed scheduler running at {address}".format(**content)
        )
        self.session.send(
            self.query,
            "become_dask_reply",
            content=content,
            parent=msg,
            ident=client_id,
        )

    def stop_distributed(self, client_id, msg):
        """Start a dask.distributed Scheduler."""
        if self.distributed_scheduler is not None:
            self.log.info("Stopping dask.distributed scheduler")
            self.distributed_scheduler.close()
            self.distributed_scheduler = None
        else:
            self.log.info("No dask.distributed scheduler running.")
        content = {'status': 'ok'}
        self.session.send(
            self.query,
            "stop_distributed_reply",
            content=content,
            parent=msg,
            ident=client_id,
        )
