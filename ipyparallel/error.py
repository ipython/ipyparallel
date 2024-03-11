"""Classes and functions for kernel related errors and exceptions.

Inheritance diagram:

.. inheritance-diagram:: ipyparallel.error
    :parts: 3
"""

import builtins
import sys
import traceback

__docformat__ = "restructuredtext en"


class IPythonError(Exception):
    """Base exception that all of our exceptions inherit from.

    This can be raised by code that doesn't have any more specific
    information."""

    pass


class KernelError(IPythonError):
    pass


class EngineError(KernelError):
    pass


class NoEnginesRegistered(KernelError):
    """Exception for operations that require some engines, but none exist"""

    def __str__(self):
        return (
            "This operation requires engines."
            " Try client.wait_for_engines(n) to wait for engines to register."
        )


class TaskAborted(KernelError):
    pass


class TaskTimeout(KernelError):
    pass


# backward-compat: use builtin TimeoutError, but preserve `error.TimeoutError` import
TimeoutError = builtins.TimeoutError


class UnmetDependency(KernelError):
    pass


class ImpossibleDependency(UnmetDependency):
    pass


class DependencyTimeout(ImpossibleDependency):
    pass


class InvalidDependency(ImpossibleDependency):
    pass


class RemoteError(KernelError):
    """Error raised elsewhere"""

    ename = None
    evalue = None
    traceback = None
    engine_info = None

    def __init__(self, ename, evalue, traceback, engine_info=None):
        self.ename = ename
        self.evalue = evalue
        self.traceback = traceback
        self.engine_info = engine_info or {}
        self.args = (ename, evalue)

    def __repr__(self):
        engineid = self.engine_info.get('engine_id', ' ')
        return f"<{self.__class__.__name__}[{engineid}]:{self.ename}({self.evalue})>"

    def __str__(self):
        label = self._get_engine_str(self.engine_info)
        engineid = self.engine_info.get('engine_id', ' ')
        return f"{label} {self.ename}: {self.evalue}"

    @staticmethod
    def _get_engine_str(engine_info):
        if not engine_info:
            return '[Engine Exception]'
        else:
            return f"[{engine_info['engine_id']}:{engine_info['method']}]"

    def render_traceback(self):
        """render traceback to a list of lines"""
        return [self._get_engine_str(self.engine_info)] + (
            self.traceback or "No traceback available"
        ).splitlines()

    def _render_traceback_(self):
        """Special method for custom tracebacks within IPython.

        This will be called by IPython instead of displaying the local traceback.

        It should return a traceback rendered as a list of lines.
        """
        return self.render_traceback()

    def print_traceback(self, excid=None):
        """print my traceback"""
        print('\n'.join(self.render_traceback()))


class TaskRejectError(KernelError):
    """Exception to raise when a task should be rejected by an engine.

    This exception can be used to allow a task running on an engine to test
    if the engine (or the user's namespace on the engine) has the needed
    task dependencies.  If not, the task should raise this exception.  For
    the task to be retried on another engine, the task should be created
    with the `retries` argument > 1.

    The advantage of this approach over our older properties system is that
    tasks have full access to the user's namespace on the engines and the
    properties don't have to be managed or tested by the controller.
    """


class CompositeError(RemoteError):
    """Error for representing possibly multiple errors on engines"""

    tb_limit = 4  # limit on how many tracebacks to draw

    def __init__(self, message, elist):
        Exception.__init__(self, *(message, elist))
        # Don't use pack_exception because it will conflict with the .message
        # attribute that is being deprecated in 2.6 and beyond.
        self.msg = message
        self.elist = elist
        self.args = [e[0] for e in elist]

    def _get_traceback(self, ev):
        try:
            tb = ev._ipython_traceback_text
        except AttributeError:
            return 'No traceback available'
        else:
            return tb

    def __str__(self):
        s = str(self.msg)
        for en, ev, etb, ei in self.elist[: self.tb_limit]:
            engine_str = self._get_engine_str(ei)
            s = s + '\n' + engine_str + en + ': ' + str(ev)
        if len(self.elist) > self.tb_limit:
            s = s + '\n.... %i more exceptions ...' % (len(self.elist) - self.tb_limit)
        return s

    def __repr__(self):
        return "CompositeError(%i)" % len(self.elist)

    def render_traceback(self, excid=None):
        """render one or all of my tracebacks to a list of lines"""
        lines = []
        if excid is None:
            for en, ev, etb, ei in self.elist[: self.tb_limit]:
                lines.append(self._get_engine_str(ei) + ":")
                lines.extend((etb or 'No traceback available').splitlines())
                lines.append('')
            if len(self.elist) > self.tb_limit:
                lines.append(
                    '... %i more exceptions ...' % (len(self.elist) - self.tb_limit)
                )
        else:
            try:
                en, ev, etb, ei = self.elist[excid]
            except Exception:
                raise IndexError("an exception with index %i does not exist" % excid)
            else:
                lines.append(self._get_engine_str(ei) + ":")
                lines.extend((etb or 'No traceback available').splitlines())

        return lines

    def print_traceback(self, excid=None):
        print('\n'.join(self.render_traceback(excid)))

    def raise_exception(self, excid=0):
        try:
            en, ev, etb, ei = self.elist[excid]
        except Exception:
            raise IndexError("an exception with index %i does not exist" % excid)
        else:
            raise RemoteError(en, ev, etb, ei)


class AlreadyDisplayedError(RemoteError):
    def __init__(self, original_error):
        self.original_error = original_error
        self.n = len(self.original_error.elist)

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.n} errors)>"

    def __str__(self):
        return f"{self.n} errors"

    def render_traceback(self):
        """IPython special method, short-circuit traceback display

        for raising when streaming output has already displayed errors
        """
        return [str(self)]


def collect_exceptions(rdict_or_list, method='unspecified'):
    """check a result dict for errors, and raise CompositeError if any exist.
    Passthrough otherwise."""
    elist = []
    if isinstance(rdict_or_list, dict):
        rlist = rdict_or_list.values()
    else:
        rlist = rdict_or_list
    for r in rlist:
        if isinstance(r, RemoteError):
            en, ev, etb, ei = r.ename, r.evalue, r.traceback, r.engine_info
            # Sometimes we could have CompositeError in our list.  Just take
            # the errors out of them and put them in our new list.  This
            # has the effect of flattening lists of CompositeErrors into one
            # CompositeError
            if en == 'CompositeError':
                for e in ev.elist:
                    elist.append(e)
            else:
                elist.append((en, ev, etb, ei))
    if len(elist) == 0:
        return rdict_or_list
    else:
        msg = "one or more exceptions raised in: %s" % (method)
        err = CompositeError(msg, elist)
        raise err


def wrap_exception(engine_info={}):
    etype, evalue, tb = sys.exc_info()
    stb = traceback.format_exception(etype, evalue, tb)
    exc_content = {
        'status': 'error',
        'traceback': stb,
        'ename': etype.__name__,
        'evalue': str(evalue),
        'engine_info': engine_info,
    }
    return exc_content


def unwrap_exception(content):
    err = RemoteError(
        content['ename'],
        content['evalue'],
        '\n'.join(content['traceback']),
        content.get('engine_info', {}),
    )
    return err
