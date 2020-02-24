from ipython_genutils.py3compat import cast_bytes

from ipyparallel import util
from ipyparallel.controller.scheduler import Scheduler


class BroadcastSchedulerNonCoalescing(Scheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.log.info('Broadcast non coalescing Scheduler Started')

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

        header = msg['header']
        metadata = msg['metadata']
        original_msg_id = header['msg_id']

        targets = metadata.get('targets', [])

        for target in targets:
            msg_and_target_id = f'{original_msg_id}_{target}'
            self.all_ids.add(msg_and_target_id)
            new_idents = [cast_bytes(target)] + idents

            header['msg_id'] = msg_and_target_id
            new_msg_list = self.session.serialize(msg, ident=new_idents)
            new_msg_list.extend(msg['buffers'])

            self.mon_stream.send_multipart([b'inbcast'] + new_msg_list, copy=False)
            self.log.debug("Sending %r", new_msg_list)
            self.engine_stream.send_multipart(new_msg_list, copy=False)

    @util.log_errors
    def dispatch_result(self, raw_msg):
        try:
            idents, msg = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg, content=False, copy=False)
            engine, client = idents[:2]
        except Exception as e:
            self.log.error(
                f'broadcast::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return

        metadata = msg['metadata']
        msg_id = msg['parent_header']['msg_id']

        success = metadata['status'] == 'ok'
        if success:
            self.all_completed.add(msg_id)
        else:
            self.all_failed.add(msg_id)

        # swap ids for ROUTER-ROUTER mirror
        raw_msg[:2] = [client, engine]
        self.client_stream.send_multipart(raw_msg, copy=False)
        self.all_done.add(msg_id)

        self.mon_stream.send_multipart([b'outbcast'] + raw_msg, copy=False)


class BroadcastSchedulerCoalescing(Scheduler):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self.log.info('Broadcast coalescing Scheduler Started')

    accumulated_replies = {}

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
        header = msg['header']
        metadata = msg['metadata']
        original_msg_id = header['msg_id']

        targets = metadata.get('targets', [])
        self.accumulated_replies[original_msg_id] = { f'{original_msg_id}_{target}': None for target in targets}
        metadata['original_msg_id'] = original_msg_id

        for target in targets:
            msg_and_target_id = f'{original_msg_id}_{target}'
            self.all_ids.add(msg_and_target_id)
            header['msg_id'] = msg_and_target_id
            new_idents = [cast_bytes(target)] + idents
            new_msg_list = self.session.serialize(msg, ident=new_idents)
            new_msg_list.extend(msg['buffers'])

            self.mon_stream.send_multipart([b'inbcast'] + new_msg_list, copy=False)
            # self.log.debug("Sending %r", new_msg_list)
            self.engine_stream.send_multipart(new_msg_list, copy=False)



    @util.log_errors
    def dispatch_result(self, raw_msg):
        try:
            idents, msg = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg, content=False, copy=False)
            engine, client = idents[:2]
        except Exception as e:
            self.log.error(
                f'broadcast::Invalid broadcast msg: {raw_msg}', exc_info=True
            )
            return

        metadata = msg['metadata']
        msg_id = msg['parent_header']['msg_id']

        success = metadata['status'] == 'ok'
        if success:
            self.all_completed.add(msg_id)
        else:
            self.all_failed.add(msg_id)

        original_msg_id = metadata['original_msg_id']
        self.accumulated_replies[original_msg_id][msg_id] = raw_msg
        raw_msg[:2] = [client, engine]

        if all(msg is not None for msg
               in self.accumulated_replies[original_msg_id].values()):

            self.client_stream.send_multipart(
                [
                    msgpart for msg in
                    self.accumulated_replies[original_msg_id].values()
                    for msgpart in msg
                ]
                , copy=False)
            self.all_done.add(original_msg_id)

            self.mon_stream.send_multipart([b'outbcast'] + raw_msg, copy=False)