"""The Python scheduler for rich scheduling.

The Pure ZMQ scheduler does not allow routing schemes other than LRU,
nor does it check msg_id DAG dependencies. For those, a slightly slower
Python Scheduler exists.
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

import logging
from traitlets import observe, Instance, Set, CBytes


import zmq
from zmq.eventloop import zmqstream

# local imports
from decorator import decorator
from traitlets.config.application import Application
from traitlets.config.loader import Config

from ipyparallel import util
from ipyparallel.util import connect_logger, local_logger, ioloop

import jupyter_client.session
jupyter_client.session.extract_dates = lambda obj: obj
from jupyter_client.session import SessionFactory


@decorator
def logged(f, self, *args, **kwargs):
    # print ("#--------------------")
    self.log.debug("scheduler::%s(*%s,**%s)", f.__name__, args, kwargs)
    # print ("#--")
    return f(self, *args, **kwargs)


class Scheduler(SessionFactory):
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

    ident = CBytes()  # ZMQ identity. This should just be self.session.session

    # but ensure Bytes
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

    ZMQStream = zmqstream.ZMQStream

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
    mons = zmqstream.ZMQStream(ctx.socket(zmq.PUB), loop)
    util.set_hwm(mons, 0)
    mons.connect(mon_addr)
    nots = zmqstream.ZMQStream(ctx.socket(zmq.SUB), loop)
    nots.setsockopt(zmq.SUBSCRIBE, b'')
    nots.connect(not_addr)

    querys = ZMQStream(ctx.socket(zmq.DEALER), loop)
    querys.connect(reg_addr)

    # setup logging.
    if in_thread:
        log = Application.instance().log
    else:
        if log_url:
            log = connect_logger(
                logname, ctx, log_url, root="scheduler", loglevel=loglevel
            )
        else:
            log = local_logger(logname, loglevel)

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
