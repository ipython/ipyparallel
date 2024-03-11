#!/usr/bin/env python
"""
A multi-heart Heartbeat system using PUB and ROUTER sockets. pings are sent out on the PUB,
and hearts are tracked based on their DEALER identities.
"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import asyncio
import logging
import time
import uuid

import zmq
from jupyter_client.session import Session
from tornado import ioloop
from traitlets import Bool, Dict, Float, Instance, Integer, Set, default
from traitlets.config.configurable import LoggingConfigurable
from zmq.devices import ThreadDevice, ThreadMonitoredQueue
from zmq.eventloop.zmqstream import ZMQStream

from ipyparallel import util
from ipyparallel.util import bind, connect, log_errors, set_hwm


class Heart:
    """A basic heart object for responding to a HeartMonitor.
    This is a simple wrapper with defaults for the most common
    Device model for responding to heartbeats.

    Builds a threadsafe zmq.FORWARDER Device, defaulting to using
    SUB/DEALER for in/out.

    You can specify the DEALER's IDENTITY via the optional heart_id argument."""

    device = None
    id = None

    def __init__(
        self,
        in_addr,
        out_addr,
        mon_addr=None,
        in_type=zmq.SUB,
        out_type=zmq.DEALER,
        mon_type=zmq.PUB,
        heart_id=None,
        curve_serverkey=None,
        curve_secretkey=None,
        curve_publickey=None,
    ):
        if mon_addr is None:
            self.device = ThreadDevice(zmq.FORWARDER, in_type, out_type)
        else:
            self.device = ThreadMonitoredQueue(
                in_type, out_type, mon_type, in_prefix=b"", out_prefix=b""
            )
        # do not allow the device to share global Context.instance,
        # which is the default behavior in pyzmq > 2.1.10
        self.device.context_factory = zmq.Context

        self.device.daemon = True
        self.device.connect_in(in_addr)
        self.device.connect_out(out_addr)
        if curve_serverkey:
            self.device.setsockopt_in(zmq.CURVE_SERVERKEY, curve_serverkey)
            self.device.setsockopt_in(zmq.CURVE_PUBLICKEY, curve_publickey)
            self.device.setsockopt_in(zmq.CURVE_SECRETKEY, curve_secretkey)

            self.device.setsockopt_out(zmq.CURVE_SERVERKEY, curve_serverkey)
            self.device.setsockopt_out(zmq.CURVE_PUBLICKEY, curve_publickey)
            self.device.setsockopt_out(zmq.CURVE_SECRETKEY, curve_secretkey)
            if mon_addr is not None:
                self.device.setsockopt_mon(zmq.CURVE_SERVERKEY, curve_publickey)
                self.device.setsockopt_mon(zmq.CURVE_PUBLICKEY, curve_publickey)
                self.device.setsockopt_mon(zmq.CURVE_SECRETKEY, curve_secretkey)

        if mon_addr is not None:
            self.device.connect_mon(mon_addr)
        if in_type == zmq.SUB:
            self.device.setsockopt_in(zmq.SUBSCRIBE, b"")
        if heart_id is None:
            heart_id = uuid.uuid4().bytes
        self.device.setsockopt_out(zmq.IDENTITY, heart_id)
        self.id = heart_id

    def start(self):
        return self.device.start()


class HeartMonitor(LoggingConfigurable):
    """A basic HeartMonitor class
    ping_stream: a PUB stream
    pong_stream: an ROUTER stream
    period: the period of the heartbeat in milliseconds
    """

    debug = Bool(
        False,
        config=True,
        help="""Whether to include every heartbeat in debugging output.

        Has to be set explicitly, because there will be *a lot* of output.
        """,
    )
    period = Integer(
        3000,
        config=True,
        help='The frequency at which the Hub pings the engines for heartbeats '
        '(in ms)',
    )
    max_heartmonitor_misses = Integer(
        10,
        config=True,
        help='Allowed consecutive missed pings from controller Hub to engine before unregistering.',
    )

    ping_stream = Instance(ZMQStream)
    pong_stream = Instance(ZMQStream)
    monitor_stream = Instance(ZMQStream)
    session = Instance(Session)

    @default("session")
    def _default_session(self):
        util._disable_session_extract_dates()
        return Session(parent=self)

    loop = Instance(ioloop.IOLoop)

    @default("loop")
    def _loop_default(self):
        return ioloop.IOLoop.current()

    # not settable:
    hearts = Set()
    responses = Set()
    on_probation = Dict()
    last_ping = Float(0)
    lifetime = Float(0)
    tic = Float(0)

    def start(self):
        self.log.debug("heartbeat::waiting for subscription")
        msg = self.monitor_stream.socket.recv_multipart()
        self.log.debug("heartbeat::subscription started")
        self.pong_stream.on_recv(self.handle_pong)
        self.tic = time.monotonic()
        self.caller = ioloop.PeriodicCallback(self.beat, self.period)
        self.caller.start()

    def beat(self):
        self.pong_stream.flush()
        self.last_ping = self.lifetime

        toc = time.monotonic()
        self.lifetime += toc - self.tic
        self.tic = toc
        if self.debug:
            self.log.debug("heartbeat::sending %s", self.lifetime)
        good_hearts = self.hearts.intersection(self.responses)
        missed_beats = self.hearts.difference(good_hearts)
        new_hearts = self.responses.difference(good_hearts)
        if new_hearts:
            self.handle_new_hearts(new_hearts)
        heart_failures, on_probation = self._check_missed(
            missed_beats, self.on_probation, self.hearts
        )
        if heart_failures:
            self.handle_heart_failure(heart_failures)
        self.on_probation = on_probation
        self.responses = set()
        # print self.on_probation, self.hearts
        # self.log.debug("heartbeat::beat %.3f, %i beating hearts", self.lifetime, len(self.hearts))
        self.ping_stream.send(str(self.lifetime).encode('ascii'))
        # flush stream to force immediate socket send
        self.ping_stream.flush()

    def _check_missed(self, missed_beats, on_probation, hearts):
        """Update heartbeats on probation, identifying any that have too many misses."""
        failures = []
        new_probation = {}
        for cur_heart in (b for b in missed_beats if b in hearts):
            miss_count = on_probation.get(cur_heart, 0) + 1
            self.log.info(f"heartbeat::missed {cur_heart} : {miss_count}")
            if miss_count > self.max_heartmonitor_misses:
                failures.append(cur_heart)
            else:
                new_probation[cur_heart] = miss_count
        return failures, new_probation

    def handle_new_hearts(self, hearts):
        for heart in hearts:
            self.hearts.add(heart)
        self.log.debug(f"Notifying hub of {len(hearts)} new hearts")
        self.session.send(
            self.monitor_stream,
            "new_heart",
            content={
                "hearts": [h.decode("utf8", "replace") for h in hearts],
            },
            ident=[b"heartmonitor", b""],
        )

    def handle_heart_failure(self, hearts):
        self.log.debug(f"Notifying hub of {len(hearts)} stopped hearts")
        self.session.send(
            self.monitor_stream,
            "stopped_heart",
            content={
                "hearts": [h.decode("utf8", "replace") for h in hearts],
            },
            ident=[b"heartmonitor", b""],
        )
        for heart in hearts:
            try:
                self.hearts.remove(heart)
            except KeyError:
                self.log.info("heartbeat:: %s has already been removed.", heart)

    @log_errors
    def handle_pong(self, msg):
        """a heart just beat"""
        current = str(self.lifetime).encode('ascii')
        last = str(self.last_ping).encode('ascii')
        if msg[1] == current:
            delta = time.monotonic() - self.tic
            if self.debug:
                self.log.debug(
                    "heartbeat::heart %r took %.2f ms to respond", msg[0], 1000 * delta
                )
            self.responses.add(msg[0])
        elif msg[1] == last:
            delta = time.monotonic() - self.tic + (self.lifetime - self.last_ping)
            self.log.warning(
                "heartbeat::heart %r missed a beat, and took %.2f ms to respond",
                msg[0],
                1000 * delta,
            )
            self.responses.add(msg[0])
        else:
            self.log.warning(
                "heartbeat::got bad heartbeat (possibly old?): %s (current=%.3f)",
                msg[1],
                self.lifetime,
            )


async def _setup_heartmonitor(
    ctx,
    ping_url,
    pong_url,
    monitor_url,
    log_level=logging.INFO,
    curve_publickey=None,
    curve_secretkey=None,
    **heart_monitor_kwargs,
):
    """Set up heart monitor

    For use in a background process,
    via Process(target=start_heartmonitor)
    """
    ping_socket = ctx.socket(zmq.PUB)
    bind(
        ping_socket,
        ping_url,
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
    )
    ping_stream = ZMQStream(ping_socket)

    pong_socket = ctx.socket(zmq.ROUTER)
    set_hwm(pong_socket, 0)
    bind(
        pong_socket,
        pong_url,
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
    )
    pong_stream = ZMQStream(pong_socket)

    monitor_socket = ctx.socket(zmq.XPUB)
    connect(
        monitor_socket,
        monitor_url,
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
        curve_serverkey=curve_publickey,
    )
    monitor_stream = ZMQStream(monitor_socket)

    # reinitialize logging after fork
    from .app import IPController

    app = IPController(log_level=log_level)
    heart_monitor_kwargs['log'] = app.log

    heart_monitor = HeartMonitor(
        ping_stream=ping_stream,
        pong_stream=pong_stream,
        monitor_stream=monitor_stream,
        **heart_monitor_kwargs,
    )
    heart_monitor.start()


def start_heartmonitor(
    ping_url,
    pong_url,
    monitor_url,
    log_level=logging.INFO,
    curve_publickey=None,
    curve_secretkey=None,
    **heart_monitor_kwargs,
):
    """Start a heart monitor.

    For use in a background process,
    via Process(target=start_heartmonitor)
    """
    ctx = zmq.Context()
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(
            _setup_heartmonitor(
                ctx=ctx,
                ping_url=ping_url,
                pong_url=pong_url,
                monitor_url=monitor_url,
                log_level=log_level,
                curve_publickey=curve_publickey,
                curve_secretkey=curve_secretkey,
                **heart_monitor_kwargs,
            )
        )
        loop.run_forever()
    finally:
        loop.close()
        ctx.destroy()
