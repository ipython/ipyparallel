"""The IPython kernel implementation"""

import getpass
import sys
import traceback

from IPython.core import release
from ipython_genutils.py3compat import builtin_mod, PY3
from IPython.utils.tokenutil import token_at_cursor, line_at_cursor
from traitlets import Instance, Type, Any, List

from .comm import CommManager
from .kernelbase import Kernel as KernelBase
from .serialize import serialize_object, unpack_apply_message
from .zmqshell import ZMQInteractiveShell


class IPythonParallelKernel(IPythonKernel):
    """Extend IPython kernel for parallel computing"""
    engine_id = Integer(-1)
    msg_types = getattr(IPythonKernel, 'msg_types', set()) + {'apply_request'}
    
    def _topic(self, topic):
        """prefixed topic for IOPub messages"""
        base = "engine.%s" % self.engine_id

        return py3compat.cast_bytes("%s.%s" % (base, topic))
        
    
    def init_metadata(self, parent):
        """init metadata dict, for execute/apply_reply"""
        return {
            'started': datetime.now(),
            'dependencies_met' : True,
            'engine' : self.ident,
        }
    
    def finish_metadata(self, parent, metadata, reply_content):
        """Finish populating metadata.
        
        Run after completing a request handler.
        """
        # FIXME: remove ipyparallel-specific code
        metadata['status'] = reply_content['status']
        if reply_content['status'] == 'error':
            if reply_content['ename'] == 'UnmetDependency':
                metadata['dependencies_met'] = False
            metadata['engine_info'] = dict(
                engine_uuid=self.ident,
                engine_id=self.engine_id,
                method='execute',
            )

        return metadata

    def apply_request(self, stream, ident, parent):
        try:
            content = parent[u'content']
            bufs = parent[u'buffers']
            msg_id = parent['header']['msg_id']
        except:
            self.log.error("Got bad msg: %s", parent, exc_info=True)
            return

        md = self._make_metadata(parent['metadata'])

        reply_content, result_buf = self.do_apply(content, bufs, msg_id, md)

        # put 'ok'/'error' status in header, for scheduler introspection:
        md['status'] = reply_content['status']

        # flush i/o
        sys.stdout.flush()
        sys.stderr.flush()

        self.session.send(stream, u'apply_reply', reply_content,
                    parent=parent, ident=ident,buffers=result_buf, metadata=md)

    def do_apply(self, content, bufs, msg_id, reply_metadata):
        shell = self.shell
        try:
            working = shell.user_ns

            prefix = "_"+str(msg_id).replace("-","")+"_"

            f,args,kwargs = unpack_apply_message(bufs, working, copy=False)

            fname = getattr(f, '__name__', 'f')

            fname = prefix+"f"
            argname = prefix+"args"
            kwargname = prefix+"kwargs"
            resultname = prefix+"result"

            ns = { fname : f, argname : args, kwargname : kwargs , resultname : None }
            # print ns
            working.update(ns)
            code = "%s = %s(*%s,**%s)" % (resultname, fname, argname, kwargname)
            try:
                exec(code, shell.user_global_ns, shell.user_ns)
                result = working.get(resultname)
            finally:
                for key in ns:
                    working.pop(key)

            result_buf = serialize_object(result,
                buffer_threshold=self.session.buffer_threshold,
                item_threshold=self.session.item_threshold,
            )

        except:
            # invoke IPython traceback formatting
            shell.showtraceback()
            # FIXME - fish exception info out of shell, possibly left there by
            # run_code.  We'll need to clean up this logic later.
            reply_content = {}
            if shell._reply_content is not None:
                reply_content.update(shell._reply_content)
                e_info = dict(engine_uuid=self.ident, engine_id=self.engine_id, method='apply')
                reply_content['engine_info'] = e_info
                # reset after use
                shell._reply_content = None

            self.send_response(self.iopub_socket, u'error', reply_content,
                                ident=self._topic('error'))
            self.log.info("Exception in apply request:\n%s", '\n'.join(reply_content['traceback']))
            result_buf = []

            if reply_content['ename'] == 'UnmetDependency':
                reply_metadata['dependencies_met'] = False
        else:
            reply_content = {'status' : 'ok'}

        return reply_content, result_buf
