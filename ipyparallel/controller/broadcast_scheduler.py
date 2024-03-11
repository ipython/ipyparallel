import logging

import zmq
from traitlets import Bool, Bytes, Integer, List, Unicode

from ipyparallel import util
from ipyparallel.controller.scheduler import (
    Scheduler,
    ZMQStream,
    get_common_scheduler_streams,
)


class BroadcastScheduler(Scheduler):
    port_name = 'broadcast'
    accumulated_replies = {}
    accumulated_targets = {}
    is_leaf = Bool(False)
    connected_sub_scheduler_ids = List(Bytes())
    outgoing_streams = List()
    depth = Integer()
    max_depth = Integer()
    name = Unicode()

    def start(self):
        self.client_stream.on_recv(self.dispatch_submission, copy=False)
        if self.is_leaf:
            super().start()
        else:
            for outgoing_stream in self.outgoing_streams:
                outgoing_stream.on_recv(self.dispatch_result, copy=False)
        self.log.info(f"BroadcastScheduler {self.name} started")

    def send_to_targets(self, msg, original_msg_id, targets, idents, is_coalescing):
        if is_coalescing:
            self.accumulated_replies[original_msg_id] = {
                target.encode('utf8'): None for target in targets
            }
            self.accumulated_targets[original_msg_id] = targets

        for target in targets:
            new_msg = self.append_new_msg_id_to_msg(
                self.get_new_msg_id(original_msg_id, target), target, idents, msg
            )
            self.engine_stream.send_multipart(new_msg, copy=False)

    def send_to_sub_schedulers(
        self, msg, original_msg_id, targets, idents, is_coalescing
    ):
        trunc = 2**self.max_depth
        fmt = f"0{self.max_depth + 1}b"

        # assign targets to sub-schedulers based on binary path
        # compute binary '010110' representation of the engine id
        targets_by_scheduler = [
            [] for i in range(len(self.connected_sub_scheduler_ids))
        ]
        for target_tuple in targets:
            path = format(target_tuple[1] % trunc, fmt)
            next_idx = int(path[self.depth + 1])  # 0 or 1
            targets_by_scheduler[next_idx].append(target_tuple)

        if is_coalescing:
            self.accumulated_replies[original_msg_id] = {
                scheduler_id: None for scheduler_id in self.connected_sub_scheduler_ids
            }
            self.accumulated_targets[original_msg_id] = {}

        for i, scheduler_id in enumerate(self.connected_sub_scheduler_ids):
            targets_for_scheduler = targets_by_scheduler[i]
            if is_coalescing:
                if targets_for_scheduler:
                    self.accumulated_targets[original_msg_id][scheduler_id] = (
                        targets_for_scheduler
                    )
                else:
                    del self.accumulated_replies[original_msg_id][scheduler_id]
            msg['metadata']['targets'] = targets_for_scheduler

            new_msg = self.append_new_msg_id_to_msg(
                self.get_new_msg_id(original_msg_id, scheduler_id),
                scheduler_id,
                idents,
                msg,
            )
            self.outgoing_streams[i].send_multipart(new_msg, copy=False)

    def coalescing_reply(self, raw_msg, msg, original_msg_id, outgoing_id, idents):
        # accumulate buffers
        self.accumulated_replies[original_msg_id][outgoing_id] = msg['buffers']
        if all(
            msg_buffers is not None
            for msg_buffers in self.accumulated_replies[original_msg_id].values()
        ):
            replies = self.accumulated_replies.pop(original_msg_id)
            self.log.debug(f"Coalescing {len(replies)} reply to {original_msg_id}")
            targets = self.accumulated_targets.pop(original_msg_id)

            new_msg = msg.copy()
            # begin rebuilding message
            # metadata['targets']
            if self.is_leaf:
                new_msg['metadata']['broadcast_targets'] = targets
            else:
                new_msg['metadata']['broadcast_targets'] = []

            # avoid duplicated msg buffers
            buffers = []
            for sub_target, msg_buffers in replies.items():
                buffers.extend(msg_buffers)
                if not self.is_leaf:
                    new_msg['metadata']['broadcast_targets'].extend(targets[sub_target])

            new_raw_msg = self.session.serialize(new_msg)
            self.client_stream.send_multipart(
                idents + new_raw_msg + buffers, copy=False
            )

    @util.log_errors
    def dispatch_submission(self, raw_msg):
        try:
            idents, msg_list = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg_list, content=False, copy=False)
        except Exception:
            self.log.error(
                f'broadcast::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return
        metadata = msg['metadata']
        msg_id = msg['header']['msg_id']
        targets = metadata['targets']

        is_coalescing = metadata['is_coalescing']

        if 'original_msg_id' not in metadata:
            metadata['original_msg_id'] = msg_id

        original_msg_id = metadata['original_msg_id']
        if self.is_leaf:
            target_idents = [t[0] for t in targets]
            self.send_to_targets(
                msg, original_msg_id, target_idents, idents, is_coalescing
            )
        else:
            self.send_to_sub_schedulers(
                msg, original_msg_id, targets, idents, is_coalescing
            )

    @util.log_errors
    def dispatch_result(self, raw_msg):
        try:
            idents, msg = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg, content=False, copy=False)
            outgoing_id = idents[0]

        except Exception:
            self.log.error(
                f'broadcast::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return
        original_msg_id = msg['metadata']['original_msg_id']
        is_coalescing = msg['metadata']['is_coalescing']
        if is_coalescing:
            self.coalescing_reply(
                raw_msg, msg, original_msg_id, outgoing_id, idents[1:]
            )
        else:
            self.client_stream.send_multipart(raw_msg[1:], copy=False)


def get_id_with_prefix(identity):
    return bytes(f'sub_scheduler_{identity}', 'utf8')


def launch_broadcast_scheduler(
    in_addr,
    out_addrs,
    mon_addr,
    not_addr,
    reg_addr,
    identity,
    config=None,
    loglevel=logging.DEBUG,
    log_url=None,
    is_leaf=False,
    in_thread=False,
    outgoing_ids=None,
    curve_publickey=None,
    curve_secretkey=None,
    depth=0,
    max_depth=0,
    scheduler_class=BroadcastScheduler,
    logname='broadcast',
):
    config, ctx, loop, mons, nots, querys, log = get_common_scheduler_streams(
        mon_addr,
        not_addr,
        reg_addr,
        config,
        logname,
        log_url,
        loglevel,
        in_thread,
        curve_serverkey=curve_publickey,
        curve_publickey=curve_publickey,
        curve_secretkey=curve_secretkey,
    )

    is_root = depth == 0
    sub_scheduler_id = get_id_with_prefix(identity)

    incoming_stream = ZMQStream(ctx.socket(zmq.ROUTER), loop)
    util.set_hwm(incoming_stream, 0)
    incoming_stream.setsockopt(zmq.IDENTITY, sub_scheduler_id)

    if is_root:
        util.bind(incoming_stream, in_addr, curve_secretkey=curve_secretkey)
    else:
        util.connect(
            incoming_stream,
            in_addr,
            curve_serverkey=curve_publickey,
            curve_publickey=curve_publickey,
            curve_secretkey=curve_secretkey,
        )

    outgoing_streams = []
    for out_addr in out_addrs:
        out = ZMQStream(ctx.socket(zmq.ROUTER), loop)
        util.set_hwm(out, 0)
        out.setsockopt(zmq.IDENTITY, sub_scheduler_id)
        util.bind(out, out_addr, curve_secretkey=curve_secretkey)
        outgoing_streams.append(out)

    scheduler_args = dict(
        client_stream=incoming_stream,
        mon_stream=mons,
        notifier_stream=nots,
        query_stream=querys,
        loop=loop,
        log=log,
        config=config,
        depth=depth,
        max_depth=max_depth,
        name=identity,
    )
    if is_leaf:
        scheduler_args.update(engine_stream=outgoing_streams[0], is_leaf=True)
    else:
        scheduler_args.update(
            connected_sub_scheduler_ids=[
                get_id_with_prefix(identity) for identity in outgoing_ids
            ],
            outgoing_streams=outgoing_streams,
        )

    scheduler = scheduler_class(**scheduler_args)

    scheduler.start()
    if not in_thread:
        try:
            loop.start()
        except KeyboardInterrupt:
            scheduler.log.critical("Interrupted, exiting...")
