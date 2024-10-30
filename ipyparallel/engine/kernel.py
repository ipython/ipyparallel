"""IPython kernel for parallel computing"""

import asyncio
import inspect
import sys

from ipykernel.ipkernel import IPythonKernel
from traitlets import Integer, Set, Type

from ipyparallel.serialize import serialize_object, unpack_apply_message
from ipyparallel.util import utcnow

from .datapub import ZMQDataPublisher


class IPythonParallelKernel(IPythonKernel):
    """Extend IPython kernel for parallel computing"""

    engine_id = Integer(-1)

    aborted = Set()

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
        return f"engine.{self.engine_id}.{topic}".encode()

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
            f = self._send_abort_reply(stream, msg, idents)
            if inspect.isawaitable(f):
                asyncio.ensure_future(f)
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
        self.shell_is_blocking = True
        try:
            reply_content, result_buf = self.do_apply(content, bufs, msg_id, md)
        finally:
            self.shell_is_blocking = False

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

            prefix = f"_{str(msg_id).replace('-', '_')}_"

            f, args, kwargs = unpack_apply_message(bufs, working, copy=False)

            fname = prefix + "f"
            argname = prefix + "args"
            kwargname = prefix + "kwargs"
            resultname = prefix + "result"

            ns = {fname: f, argname: args, kwargname: kwargs, resultname: None}
            # print ns
            working.update(ns)
            code = f"{resultname} = {fname}(*{argname}, **{kwargname})"
            try:
                exec(code, shell.user_global_ns, shell.user_ns)
                result = working.get(resultname)
            finally:
                for key in ns:
                    try:
                        working.pop(key)
                    except KeyError:
                        self.log.warning(
                            f"Failed to undefine temporary apply variable {key}, already undefined"
                        )

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

    async def do_execute(self, *args, **kwargs):
        super_execute = super().do_execute(*args, **kwargs)
        if inspect.isawaitable(super_execute):
            reply_content = await super_execute
        else:
            reply_content = super_execute
        # add engine info
        if reply_content['status'] == 'error':
            reply_content["engine_info"] = self.get_engine_info(method="execute")
        return reply_content

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
