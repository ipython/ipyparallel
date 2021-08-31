import logging

import zmq
from traitlets import Bool
from traitlets import Bytes
from traitlets import Integer
from traitlets import List

from ipyparallel import util
from ipyparallel.controller.scheduler import get_common_scheduler_streams
from ipyparallel.controller.scheduler import Scheduler
from ipyparallel.controller.scheduler import ZMQStream


class BroadcastScheduler(Scheduler):
    port_name = 'broadcast'
    accumulated_replies = {}
    is_leaf = Bool(False)
    connected_sub_scheduler_ids = List(Bytes())
    outgoing_streams = List()
    depth = Integer()
    max_depth = Integer()

    def start(self):
        self.client_stream.on_recv(self.dispatch_submission, copy=False)
        if self.is_leaf:
            super().start()
        else:
            for outgoing_stream in self.outgoing_streams:
                outgoing_stream.on_recv(self.dispatch_result, copy=False)

    def send_to_targets(self, msg, original_msg_id, targets, idents, is_coalescing):
        if is_coalescing:
            self.accumulated_replies[original_msg_id] = {
                bytes(target, 'utf8'): None for target in targets
            }

        for target in targets:
            new_msg = self.append_new_msg_id_to_msg(
                self.get_new_msg_id(original_msg_id, target), target, idents, msg
            )
            self.engine_stream.send_multipart(new_msg, copy=False)

    def send_to_sub_schedulers(
        self, msg, original_msg_id, targets, idents, is_coalescing
    ):
        if is_coalescing:
            self.accumulated_replies[original_msg_id] = {
                scheduler_id: None for scheduler_id in self.connected_sub_scheduler_ids
            }

        trunc = 2 ** self.max_depth
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

        for i, scheduler_id in enumerate(self.connected_sub_scheduler_ids):
            targets_for_scheduler = targets_by_scheduler[i]
            if not targets_for_scheduler and is_coalescing:
                del self.accumulated_replies[original_msg_id][scheduler_id]
            msg['metadata']['targets'] = targets_for_scheduler

            new_msg = self.append_new_msg_id_to_msg(
                self.get_new_msg_id(original_msg_id, scheduler_id),
                scheduler_id,
                idents,
                msg,
            )
            self.outgoing_streams[i].send_multipart(new_msg, copy=False)

    def coalescing_reply(self, raw_msg, msg, original_msg_id, outgoing_id):
        if all(
            msg is not None or stored_outgoing_id == outgoing_id
            for stored_outgoing_id, msg in self.accumulated_replies[
                original_msg_id
            ].items()
        ):
            new_msg = raw_msg[1:]
            new_msg.extend(
                [
                    buffer
                    for msg_buffers in self.accumulated_replies[
                        original_msg_id
                    ].values()
                    if msg_buffers
                    for buffer in msg_buffers
                ]
            )
            self.client_stream.send_multipart(new_msg, copy=False)
            del self.accumulated_replies[original_msg_id]
        else:
            self.accumulated_replies[original_msg_id][outgoing_id] = msg['buffers']

    @util.log_errors
    def dispatch_submission(self, raw_msg):
        try:
            idents, msg_list = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg_list, content=False, copy=False)
        except:
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
            self.coalescing_reply(raw_msg, msg, original_msg_id, outgoing_id)
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
    depth=0,
    max_depth=0,
):
    config, ctx, loop, mons, nots, querys, log = get_common_scheduler_streams(
        mon_addr, not_addr, reg_addr, config, 'scheduler', log_url, loglevel, in_thread
    )

    is_root = depth == 0
    sub_scheduler_id = get_id_with_prefix(identity)

    incoming_stream = ZMQStream(ctx.socket(zmq.ROUTER), loop)
    util.set_hwm(incoming_stream, 0)
    incoming_stream.setsockopt(zmq.IDENTITY, sub_scheduler_id)

    if is_root:
        incoming_stream.bind(in_addr)
    else:
        incoming_stream.connect(in_addr)

    outgoing_streams = []
    for out_addr in out_addrs:
        out = ZMQStream(ctx.socket(zmq.ROUTER), loop)
        util.set_hwm(out, 0)
        out.setsockopt(zmq.IDENTITY, sub_scheduler_id)
        out.bind(out_addr)
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

    scheduler = BroadcastScheduler(**scheduler_args)

    scheduler.start()
    if not in_thread:
        try:
            loop.start()
        except KeyboardInterrupt:
            scheduler.log.critical("Interrupted, exiting...")
