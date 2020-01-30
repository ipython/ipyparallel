import zmq
from ipython_genutils.py3compat import cast_bytes

from ipyparallel import util
from ipyparallel.controller.scheduler import Scheduler


class BroadcastSchedulerNonCoalescing(Scheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.log.info('Broadcast Scheduler Started')
    @util.log_errors
    def dispatch_submission(self, raw_msg):

        try:
            idents, msg_list = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg_list, content=False, copy=False)
        except Exception as e:
            self.log.error(
                f'broadcast::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return

        # send to monitor
        self.mon_stream.send_multipart([b'intask'] + raw_msg, copy=False)

        header = msg['header']
        metadata = msg['metadata']
        original_msg_id = header['msg_id']

        targets = metadata.get('targets', [])

        for target in targets:
            msg_and_target_id = f'{original_msg_id}_{target}'
            self.all_ids.add(msg_and_target_id)
            header['msg_id'] = msg_and_target_id
            raw_msg[1] = self.session.pack(header)
            #TODO: Might have to change raw_msg to add new msg_id
            self.engine_stream.send(cast_bytes(target), flags=zmq.SNDMORE, copy=False)
            self.engine_stream.send_multipart(raw_msg, copy=False)

    @util.log_errors
    def dispatch_result(self, raw_msg):
        try:
            idents, msg = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg, content=False, copy=False)
            engine, client = idents[:2] # TODO: Make sure this is actually engine
        except Exception as e:
            self.log.error(
                f'broadcast::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return

        metadata = msg['metadata']
        parent = msg['parent_header']

        original_msg_id = parent['msg_id']
        msg_and_target_id = f'{original_msg_id}_{engine.decode("utf-8")}'
        success = metadata['status'] == 'ok'
        if success:
            self.all_completed.add(msg_and_target_id)
        else:
            self.all_failed.add(msg_and_target_id)

        # swap ids for ROUTER-ROUTER mirror
        raw_msg[:2] = [client, engine]
        self.client_stream.send_multipart(raw_msg, copy=False)
        self.all_done.add(msg_and_target_id)

        # send to Hub monitor TODO:Figure out if this is needed
        self.mon_stream.send_multipart([b'outtask'] + raw_msg, copy=False)
