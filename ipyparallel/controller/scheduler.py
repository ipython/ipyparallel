"""The Python scheduler for rich scheduling.

The Pure ZMQ scheduler does not allow routing schemes other than LRU,
nor does it check msg_id DAG dependencies. For those, a slightly slower
Python Scheduler exists.
"""
# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import logging

import jupyter_client.session
import traitlets.log
import zmq
from decorator import decorator
from tornado import ioloop
from traitlets import Bytes
from traitlets import default
from traitlets import Instance
from traitlets import Set
from traitlets.config import Config
from traitlets.config import LoggingConfigurable
from zmq.eventloop import zmqstream

from ipyparallel import util
from ipyparallel.util import connect_logger
from ipyparallel.util import local_logger

# local imports

# performance optimization: skip unused date deserialization
jupyter_client.session.extract_dates = lambda obj: obj


@decorator
def logged(f, self, *args, **kwargs):
    # print ("#--------------------")
    self.log.debug("scheduler::%s(*%s,**%s)", f.__name__, args, kwargs)
    # print ("#--")
    return f(self, *args, **kwargs)


class Scheduler(LoggingConfigurable):

    loop = Instance(ioloop.IOLoop)

    @default("loop")
    def _default_loop(self):
        return ioloop.IOLoop.current()

    session = Instance(jupyter_client.session.Session)

    @default("session")
    def _default_session(self):
        return jupyter_client.session.Session(parent=self)

    client_stream = Instance(
        zmqstream.ZMQStream, allow_none=True
    )  # client-facing stream
    engine_stream = Instance(
        zmqstream.ZMQStream, allow_none=True
    )  # engine-facing stream
    notifier_stream = Instance(
        zmqstream.ZMQStream, allow_none=True
    )  # hub-facing sub stream
    mon_stream = Instance(zmqstream.ZMQStream, allow_none=True)  # hub-facing pub stream
    query_stream = Instance(
        zmqstream.ZMQStream, allow_none=True
    )  # hub-facing DEALER stream

    all_completed = Set()  # set of all completed tasks
    all_failed = Set()  # set of all failed tasks
    all_done = Set()  # set of all finished tasks=union(completed,failed)
    all_ids = Set()  # set of all submitted task IDs

    ident = Bytes()  # ZMQ identity. This should just be self.session.session as bytes

    # but ensure Bytes
    @default("ident")
    def _ident_default(self):
        return self.session.bsession

    def start(self):
        self.engine_stream.on_recv(self.dispatch_result, copy=False)
        self.client_stream.on_recv(self.dispatch_submission, copy=False)

    def resume_receiving(self):
        """Resume accepting jobs."""
        self.client_stream.on_recv(self.dispatch_submission, copy=False)

    def stop_receiving(self):
        """Stop accepting jobs while there are no engines.
        Leave them in the ZMQ queue."""
        self.client_stream.on_recv(None)

    def dispatch_result(self, raw_msg):
        raise NotImplementedError("Implement in subclasses")

    def dispatch_submission(self, raw_msg):
        raise NotImplementedError("Implement in subclasses")

    def append_new_msg_id_to_msg(self, new_id, target_id, idents, msg):
        if isinstance(target_id, str):
            target_id = target_id.encode("utf8")
        new_idents = [target_id] + idents
        msg['header']['msg_id'] = new_id
        new_msg_list = self.session.serialize(msg, ident=new_idents)
        new_msg_list.extend(msg['buffers'])
        return new_msg_list

    def get_new_msg_id(self, original_msg_id, outgoing_id):
        return f'{original_msg_id}_{outgoing_id if isinstance(outgoing_id, str) else outgoing_id.decode("utf8")}'


ZMQStream = zmqstream.ZMQStream


def get_common_scheduler_streams(
    mon_addr, not_addr, reg_addr, config, logname, log_url, loglevel, in_thread
):
    if config:
        # unwrap dict back into Config
        config = Config(config)

    if in_thread:
        # use instance() to get the same Context/Loop as our parent
        ctx = zmq.Context.instance()
        loop = ioloop.IOLoop.current()
    else:
        # in a process, don't use instance()
        # for safety with multiprocessing
        ctx = zmq.Context()
        loop = ioloop.IOLoop()
        loop.make_current()
    mons = zmqstream.ZMQStream(ctx.socket(zmq.PUB), loop)
    mons.connect(mon_addr)
    nots = zmqstream.ZMQStream(ctx.socket(zmq.SUB), loop)
    nots.setsockopt(zmq.SUBSCRIBE, b'')
    nots.connect(not_addr)

    querys = ZMQStream(ctx.socket(zmq.DEALER), loop)
    querys.connect(reg_addr)

    # setup logging.
    if in_thread:
        log = traitlets.log.get_logger()
    else:
        if log_url:
            log = connect_logger(
                logname, ctx, log_url, root="scheduler", loglevel=loglevel
            )
        else:
            log = local_logger(logname, loglevel)
    return config, ctx, loop, mons, nots, querys, log


def launch_scheduler(
    scheduler_class,
    in_addr,
    out_addr,
    mon_addr,
    not_addr,
    reg_addr,
    config=None,
    logname='root',
    log_url=None,
    loglevel=logging.DEBUG,
    identity=None,
    in_thread=False,
):
    config, ctx, loop, mons, nots, querys, log = get_common_scheduler_streams(
        mon_addr, not_addr, reg_addr, config, logname, log_url, loglevel, in_thread
    )

    util.set_hwm(mons, 0)
    ins = ZMQStream(ctx.socket(zmq.ROUTER), loop)
    util.set_hwm(ins, 0)
    if identity:
        ins.setsockopt(zmq.IDENTITY, identity + b'_in')

    ins.bind(in_addr)

    outs = ZMQStream(ctx.socket(zmq.ROUTER), loop)
    util.set_hwm(outs, 0)

    if identity:
        outs.setsockopt(zmq.IDENTITY, identity + b'_out')
    outs.bind(out_addr)

    scheduler = scheduler_class(
        client_stream=ins,
        engine_stream=outs,
        mon_stream=mons,
        notifier_stream=nots,
        query_stream=querys,
        loop=loop,
        log=log,
        config=config,
    )

    scheduler.start()
    if not in_thread:
        try:
            loop.start()
        except KeyboardInterrupt:
            scheduler.log.critical("Interrupted, exiting...")
