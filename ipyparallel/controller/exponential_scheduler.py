from ipython_genutils.py3compat import cast_bytes

from ipyparallel import util
from ipyparallel.controller.scheduler import Scheduler

MESSAGES_PER_SCHEDULER = 7


class ExponentialScheduler(Scheduler):
    accumulated_replies = {}

    def __init__(
        self,
        *args,
        is_root=False,
        is_leaf=False,
        identity=None,
        connected_sub_schedulers=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.is_root = is_root
        self.is_leaf = is_leaf
        self.identity = identity
        self.connected_sub_schedulers = connected_sub_schedulers

    def add_connected_sub_scheduler(self, sub_scheduler_id):
        self.connected_sub_schedulers.append(sub_scheduler_id)

    def start(self):
        self.client_stream.on_recv(self.dispatch_submission, copy=False)

    def resume_receiving(self):
        self.client_stream.on_recv(self.dispatch_submission)

    def stop_receiving(self):
        self.client_stream.on_recv(None)

    def send_to_sub_schedulers(self, msg, targets, idents):
        original_msg_id = msg['header']['msg_id']
        self.accumulated_replies[original_msg_id] = {
            f'{original_msg_id}_{scheduler_id.decode("utf8")}': None
            for scheduler_id in self.connected_sub_schedulers
        }

        for i, scheduler_id in enumerate(self.connected_sub_schedulers):
            msg_and_scheduler_id = f'{original_msg_id}_{scheduler_id.decode("utf8")}'

            targets_for_scheduler = targets[
                i * MESSAGES_PER_SCHEDULER : (i + 1) * MESSAGES_PER_SCHEDULER
            ]
            if not targets_for_scheduler:
                del self.accumulated_replies[original_msg_id][msg_and_scheduler_id]
                continue
            msg['header']['msg_id'] = msg_and_scheduler_id
            msg['metadata']['targets'] = targets_for_scheduler
            self.all_ids.add(msg_and_scheduler_id)
            new_idents = [cast_bytes(scheduler_id + b'_in')] + idents
            new_msg_list = self.session.serialize(msg, ident=new_idents)
            new_msg_list.extend(msg['buffers'])
            self.mon_stream.send_multipart([b'inexpo'] + new_msg_list, copy=False)
            self.client_stream.send_multipart(new_msg_list, copy=False)

    @util.log_errors
    def dispatch_submission(self, raw_msg):
        self.log.info(f'Exponential msg received ')
        try:
            idents, msg_list = self.session.feed_identities(raw_msg, copy=False)
            msg = self.session.deserialize(msg_list, content=False, copy=False)
        except Exception as e:
            self.log.error(f'exponential scheduler:: Invalid msg: {raw_msg}')
            return
        header = msg['header']
        metadata = msg['metadata']
        original_msg_id = header['msg_id']
        targets = metadata.get('targets', [])
        if not self.is_leaf:
            self.send_to_sub_schedulers(msg, targets, idents)
        else:
            self.accumulated_replies[original_msg_id] = {
                f'{original_msg_id}_{target}': None for target in targets
            }
            for target in targets:
                msg_and_target_id = f'{original_msg_id}_{target}'
                self.all_ids.add(msg_and_target_id)
                header['msg_id'] = msg_and_target_id
                new_idents = [cast_bytes(target)] + idents
                new_msg_list = self.session.serialize(msg, ident=new_idents)
                new_msg_list.extend(msg['buffers'])

                self.mon_stream.send_multipart([b'inexpo'] + new_msg_list, copy=False)
                self.engine_stream.send_multipart(new_msg_list, copy=False)

    def dispatch_result(self, raw_msg):
        self.log.info(f'expo: {self.id} received {raw_msg}')
