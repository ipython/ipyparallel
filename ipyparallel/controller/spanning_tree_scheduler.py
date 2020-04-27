import logging

import zmq

from ipyparallel import util
from ipyparallel.controller.scheduler import (
    Scheduler,
    get_common_scheduler_streams,
    ZMQStream,
)

SPANNING_TREE_SCHEDULER_DEPTH = 3


class SpanningTreeScheduler(Scheduler):
    accumulated_replies = {}

    def __init__(
        self, depth=0, connected_sub_schedulers=None, outgoing_streams=None, **kwargs
    ):
        super().__init__(**kwargs)
        self.connected_sub_schedulers = connected_sub_schedulers
        self.outgoing_streams = outgoing_streams
        self.depth = depth
        self.log.info('Spanning tree scheduler started')

    def start(self):
        self.client_stream.on_recv(self.dispatch_submission, copy=False)
        for outgoing_stream in self.outgoing_streams:
            outgoing_stream.on_recv(self.dispatch_result, copy=False)

    def resume_receiving(self):
        self.client_stream.on_recv(self.dispatch_submission)

    def stop_receiving(self):
        self.client_stream.on_recv(None)

    @util.log_errors
    def dispatch_submission(self, raw_msg):
        try:
            idents, msg_list = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg_list, content=False, copy=False)
        except:
            self.log.error(f'Spanning tree scheduler:: Invalid msg: {raw_msg}')
            return

        metadata = msg['metadata']
        msg_id = msg['header']['msg_id']
        targets = metadata.get('targets', [])
        if 'original_msg_id' not in metadata:
            metadata['original_msg_id'] = msg_id
            metadata['is_spanning_tree'] = True

        original_msg_id = metadata['original_msg_id']

        self.accumulated_replies[original_msg_id] = {
            scheduler_id: None for scheduler_id in self.connected_sub_schedulers
        }

        for i, scheduler_id in enumerate(self.connected_sub_schedulers):
            targets_for_scheduler = targets[
                i * len(targets) // 2 : (i + 1) * len(targets) // 2
            ]

            if not targets_for_scheduler:
                del self.accumulated_replies[original_msg_id][scheduler_id]
                continue

            msg['metadata']['targets'] = targets_for_scheduler

            new_msg = self.append_new_msg_id_to_msg(
                self.get_new_msg_id(msg_id, scheduler_id), scheduler_id, idents, msg
            )
            self.mon_stream.send_multipart([b'insptree'] + new_msg, copy=False)
            self.outgoing_streams[i].send_multipart(new_msg, copy=False)

    @util.log_errors
    def dispatch_result(self, raw_msg):
        try:
            idents, msg = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg, content=False, copy=False)
            outgoing_scheduler, _ = idents[:2]
        except:
            self.log.error(
                f'spanning tree::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return

        original_msg_id = msg['metadata']['original_msg_id']
        self.accumulated_replies[original_msg_id][outgoing_scheduler] = raw_msg[1:]

        if all(
            msg is not None
            for msg in self.accumulated_replies[original_msg_id].values()
        ):
            self.client_stream.send_multipart(
                [
                    msgpart
                    for msg in self.accumulated_replies[original_msg_id].values()
                    for msgpart in msg
                ],
                copy=False,
            )
            self.mon_stream.send_multipart([b'outsptree'] + raw_msg, copy=False)


class SpanningTreeLeafScheduler(Scheduler):
    accumulated_replies = {}

    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.log.info('Spanning tree leaf scheduler started')

    @util.log_errors
    def dispatch_submission(self, raw_msg):
        try:
            idents, msg_list = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg_list, content=False, copy=False)
        except Exception as e:
            self.log.error(f'Spanning tree scheduler:: Invalid msg: {raw_msg}')
            return

        metadata = msg['metadata']
        original_msg_id = metadata['original_msg_id']
        targets = metadata.get('targets', [])

        self.accumulated_replies[original_msg_id] = {
            bytes(target, 'utf8'): None for target in targets
        }
        for target in targets:
            new_msg = self.append_new_msg_id_to_msg(
                self.get_new_msg_id(original_msg_id, target), target, idents, msg
            )
            self.mon_stream.send_multipart([b'insptree'] + new_msg, copy=False)
            self.engine_stream.send_multipart(new_msg, copy=False)

    @util.log_errors
    def dispatch_result(self, raw_msg):
        try:
            idents, msg = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg, content=False, copy=False)
            target = idents[0]
        except:
            self.log.error(
                f'spanning tree::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return

        original_msg_id = msg['metadata']['original_msg_id']
        self.accumulated_replies[original_msg_id][target] = raw_msg[1:]

        if all(
            msg is not None
            for msg in self.accumulated_replies[original_msg_id].values()
        ):
            self.client_stream.send_multipart(
                [
                    msgpart
                    for msg in self.accumulated_replies[original_msg_id].values()
                    for msgpart in msg
                ],
                copy=False,
            )
            self.mon_stream.send_multipart([b'outsptree'] + raw_msg, copy=False)


def get_id_with_prefix(identity):
    return bytes(f'sub_scheduler_{identity}', 'utf8')


def launch_spanning_tree_scheduler(
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
):
    config, ctx, loop, mons, nots, querys, log = get_common_scheduler_streams(
        mon_addr, not_addr, reg_addr, config, 'scheduler', log_url, loglevel, in_thread
    )

    is_root = identity == 0
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
    )
    if is_leaf:
        scheduler_args.update(engine_stream=outgoing_streams[0])
        scheduler = SpanningTreeLeafScheduler(**scheduler_args)
    else:
        scheduler_args.update(
            connected_sub_schedulers=[
                get_id_with_prefix(identity) for identity in outgoing_ids
            ],
            outgoing_streams=outgoing_streams,
        )
        scheduler = SpanningTreeScheduler(**scheduler_args)

    scheduler.start()
    if not in_thread:
        try:
            loop.start()
        except KeyboardInterrupt:
            scheduler.log.critical("Interrupted, exiting...")
