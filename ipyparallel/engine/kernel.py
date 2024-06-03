"""IPython kernel for parallel computing"""

import asyncio
import inspect
import sys
from functools import partial

import ipykernel
from ipykernel.ipkernel import IPythonKernel
from traitlets import Integer, Type

from ipyparallel.serialize import serialize_object, unpack_apply_message
from ipyparallel.util import utcnow

from .datapub import ZMQDataPublisher


class IPythonParallelKernel(IPythonKernel):
    """Extend IPython kernel for parallel computing"""

    engine_id = Integer(-1)

    @property
    def int_id(self):
        return self.engine_id

    msg_types = getattr(IPythonKernel, 'msg_types', []) + ['apply_request']
    control_msg_types = getattr(IPythonKernel, 'control_msg_types', []) + [
        'abort_request',
        'clear_request',
    ]
    _execute_sleep = 0
    data_pub_class = Type(ZMQDataPublisher)

    def _topic(self, topic):
        """prefixed topic for IOPub messages"""
        base = "engine.%s" % self.engine_id

        return f"{base}.{topic}".encode()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # add apply_request, in anticipation of upstream deprecation
        self.shell_handlers['apply_request'] = self.apply_request
        # set up data pub
        data_pub = self.shell.data_pub = self.data_pub_class(parent=self)
        self.shell.configurables.append(data_pub)
        data_pub.session = self.session
        data_pub.pub_socket = self.iopub_socket
        self.aborted = set()

    def _abort_queues(self):
        # forward-port ipython/ipykernel#853
        # may remove after requiring ipykernel 6.9.1

        # while this flag is true,
        # execute requests will be aborted
        self._aborting = True
        self.log.info("Aborting queue")

        # Callback to signal that we are done aborting
        def stop_aborting():
            self.log.info("Finishing abort")
            self._aborting = False
            # must be awaitable for ipykernel >= 3.6
            # must also be sync for ipykernel < 3.6
            f = asyncio.Future()
            f.set_result(None)
            return f

        # put stop_aborting on the message queue
        # so that it's handled after processing of already-pending messages
        if ipykernel.version_info < (6,):
            # 10 is SHELL priority in ipykernel 5.x
            streams = self.shell_streams
            schedule_stop_aborting = partial(self.schedule_dispatch, 10, stop_aborting)
        else:
            streams = [self.shell_stream]
            schedule_stop_aborting = partial(self.schedule_dispatch, stop_aborting)

        # flush streams, so all currently waiting messages
        # are added to the queue
        for stream in streams:
            stream.flush()

        # if we have a delay, give messages this long to arrive on the queue
        # before we start accepting requests
        asyncio.get_running_loop().call_later(
            self.stop_on_error_timeout, schedule_stop_aborting
        )

        # for compatibility, return a completed Future
        # so this is still awaitable
        f = asyncio.Future()
        f.set_result(None)
        return f

    def should_handle(self, stream, msg, idents):
        """Check whether a shell-channel message should be handled

        Allows subclasses to prevent handling of certain messages (e.g. aborted requests).
        """
        if not super().should_handle(stream, msg, idents):
            return False
        msg_id = msg['header']['msg_id']
        msg_type = msg['header']['msg_type']
        if msg_id in self.aborted:
            # is it safe to assume a msg_id will not be resubmitted?
            self.aborted.remove(msg_id)
            self._send_abort_reply(stream, msg, idents)
            return False
        self.log.info(f"Handling {msg_type}: {msg_id}")
        return True

    def init_metadata(self, parent):
        """init metadata dict, for execute/apply_reply"""
        parent_metadata = parent.get('metadata', {})
        return {
            'started': utcnow(),
            'dependencies_met': True,
            'engine': self.ident,
            'is_broadcast': parent_metadata.get('is_broadcast', False),
            'is_coalescing': parent_metadata.get('is_coalescing', False),
            'original_msg_id': parent_metadata.get('original_msg_id', ''),
        }

    def finish_metadata(self, parent, metadata, reply_content):
        """Finish populating metadata.

        Run after completing a request handler.
        """
        metadata['status'] = reply_content['status']
        if reply_content['status'] == 'error':
            if reply_content['ename'] == 'UnmetDependency':
                metadata['dependencies_met'] = False
            metadata['engine_info'] = self.get_engine_info()

        return metadata

    def get_engine_info(self, method=None):
        """Return engine_info dict"""
        engine_info = dict(
            engine_uuid=self.ident,
            engine_id=self.engine_id,
        )
        if method:
            engine_info["method"] = method
        return engine_info

    def apply_request(self, stream, ident, parent):
        try:
            content = parent['content']
            bufs = parent['buffers']
            msg_id = parent['header']['msg_id']
        except Exception:
            self.log.error("Got bad msg: %s", parent, exc_info=True)
            return

        md = self.init_metadata(parent)
        reply_content, result_buf = self.do_apply(content, bufs, msg_id, md)

        # put 'ok'/'error' status in header, for scheduler introspection:
        md = self.finish_metadata(parent, md, reply_content)

        # flush i/o
        sys.stdout.flush()
        sys.stderr.flush()
        self.log.debug(f'Sending apply_reply: {msg_id}')
        self.session.send(
            stream,
            'apply_reply',
            reply_content,
            parent=parent,
            ident=ident,
            buffers=result_buf,
            metadata=md,
        )

    def do_apply(self, content, bufs, msg_id, reply_metadata):
        shell = self.shell
        try:
            working = shell.user_ns

            prefix = "_" + str(msg_id).replace("-", "") + "_"

            f, args, kwargs = unpack_apply_message(bufs, working, copy=False)

            fname = getattr(f, '__name__', 'f')

            fname = prefix + "f"
            argname = prefix + "args"
            kwargname = prefix + "kwargs"
            resultname = prefix + "result"

            ns = {fname: f, argname: args, kwargname: kwargs, resultname: None}
            # print ns
            working.update(ns)
            code = f"{resultname} = {fname}(*{argname},**{kwargname})"
            try:
                exec(code, shell.user_global_ns, shell.user_ns)
                result = working.get(resultname)
            finally:
                for key in ns:
                    working.pop(key)

            result_buf = serialize_object(
                result,
                buffer_threshold=self.session.buffer_threshold,
                item_threshold=self.session.item_threshold,
            )

        except BaseException as e:
            # invoke IPython traceback formatting
            # this sends the 'error' message
            try:
                shell.showtraceback()
            except Exception as tb_error:
                self.log.error(f"Failed to show traceback for {e}: {tb_error}")

            try:
                str_evalue = str(e)
            except Exception as str_error:
                str_evalue = f"Failed to cast exception to string: {str_error}"
            reply_content = {
                'traceback': [],
                'ename': str(type(e).__name__),
                'evalue': str_evalue,
            }
            # get formatted traceback, which ipykernel recorded
            if hasattr(shell, '_last_traceback'):
                # ipykernel 4.4
                reply_content['traceback'] = shell._last_traceback or []
            else:
                self.log.warning("Didn't find a traceback where I expected to")
            shell._last_traceback = None
            reply_content["engine_info"] = self.get_engine_info(method="apply")

            self.log.info(
                "Exception in apply request:\n%s", '\n'.join(reply_content['traceback'])
            )
            result_buf = []
            reply_content['status'] = 'error'
        else:
            reply_content = {'status': 'ok'}

        return reply_content, result_buf

    async def _do_execute_async(self, *args, **kwargs):
        super_execute = super().do_execute(*args, **kwargs)
        if inspect.isawaitable(super_execute):
            reply_content = await super_execute
        else:
            reply_content = super_execute
        # add engine info
        if reply_content['status'] == 'error':
            reply_content["engine_info"] = self.get_engine_info(method="execute")
        return reply_content

    def do_execute(self, *args, **kwargs):
        coro = self._do_execute_async(*args, **kwargs)
        if ipykernel.version_info < (6,):
            # ipykernel 5 uses gen.maybe_future which doesn't accept async def coroutines,
            # but it does accept asyncio.Futures
            return asyncio.ensure_future(coro)
        else:
            return coro

    # Control messages for msgspec extensions:

    def abort_request(self, stream, ident, parent):
        """abort a specific msg by id"""
        msg_ids = parent['content'].get('msg_ids', None)
        if isinstance(msg_ids, str):
            msg_ids = [msg_ids]
        if not msg_ids:
            f = self._abort_queues()
            if inspect.isawaitable(f):
                asyncio.ensure_future(f)
        for mid in msg_ids:
            self.aborted.add(str(mid))

        content = dict(status='ok')
        reply_msg = self.session.send(
            stream, 'abort_reply', content=content, parent=parent, ident=ident
        )
        self.log.debug("%s", reply_msg)

    def clear_request(self, stream, idents, parent):
        """Clear our namespace."""
        self.shell.reset(False)
        content = dict(status='ok')
        self.session.send(
            stream, 'clear_reply', ident=idents, parent=parent, content=content
        )

    def _send_abort_reply(self, stream, msg, idents):
        """Send a reply to an aborted request"""
        # FIXME: forward-port ipython/ipykernel#684
        self.log.info(
            f"Aborting {msg['header']['msg_id']}: {msg['header']['msg_type']}"
        )
        reply_type = msg["header"]["msg_type"].rsplit("_", 1)[0] + "_reply"
        status = {"status": "aborted"}
        md = self.init_metadata(msg)
        md = self.finish_metadata(msg, md, status)
        md.update(status)

        self.session.send(
            stream,
            reply_type,
            metadata=md,
            content=status,
            parent=msg,
            ident=idents,
        )
