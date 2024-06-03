"""Views of remote engines."""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import builtins
import concurrent.futures
import inspect
import secrets
import threading
import time
import warnings
from collections import deque
from contextlib import contextmanager

from decorator import decorator
from IPython import get_ipython
from traitlets import Any, Bool, CFloat, Dict, HasTraits, Instance, Integer, List, Set

import ipyparallel as ipp
from ipyparallel import util
from ipyparallel.controller.dependency import Dependency, dependent

from .. import serialize
from ..serialize import PrePickled
from . import map as Map
from .asyncresult import AsyncMapResult, AsyncResult
from .remotefunction import ParallelFunction, getname, parallel, remote

# -----------------------------------------------------------------------------
# Decorators
# -----------------------------------------------------------------------------


@decorator
def save_ids(f, self, *args, **kwargs):
    """Keep our history and outstanding attributes up to date after a method call."""
    n_previous = len(self.client.history)
    try:
        ret = f(self, *args, **kwargs)
    finally:
        nmsgs = len(self.client.history) - n_previous
        msg_ids = self.client.history[-nmsgs:]
        self.history.extend(msg_ids)
        self.outstanding.update(msg_ids)
    return ret


@decorator
def sync_results(f, self, *args, **kwargs):
    """sync relevant results from self.client to our results attribute."""
    if self._in_sync_results:
        return f(self, *args, **kwargs)
    self._in_sync_results = True
    try:
        ret = f(self, *args, **kwargs)
    finally:
        self._in_sync_results = False
        self._sync_results()
    return ret


# -----------------------------------------------------------------------------
# Classes
# -----------------------------------------------------------------------------


class View(HasTraits):
    """Base View class for more convenint apply(f,*args,**kwargs) syntax via attributes.

    Don't use this class, use subclasses.

    Methods
    -------

    spin
        flushes incoming results and registration state changes
        control methods spin, and requesting `ids` also ensures up to date

    wait
        wait on one or more msg_ids

    execution methods
        apply
        legacy: execute, run

    data movement
        push, pull, scatter, gather

    query methods
        get_result, queue_status, purge_results, result_status

    control methods
        abort, shutdown

    """

    # flags
    block = Bool(False)
    track = Bool(False)
    targets = Any()

    history = List()
    outstanding = Set()
    results = Dict()
    client = Instance('ipyparallel.Client', allow_none=True)

    _socket = Any()
    _flag_names = List(['targets', 'block', 'track'])
    _in_sync_results = Bool(False)
    _targets = Any()
    _idents = Any()

    def __init__(self, client=None, socket=None, **flags):
        super().__init__(client=client, _socket=socket)
        self.results = client.results
        self.block = client.block
        self.executor = ViewExecutor(self)

        self.set_flags(**flags)

        assert self.__class__ is not View, "Don't use base View objects, use subclasses"

    def __repr__(self):
        strtargets = str(self.targets)
        if len(strtargets) > 16:
            strtargets = strtargets[:12] + '...]'
        return f"<{self.__class__.__name__} {strtargets}>"

    def __len__(self):
        if isinstance(self.targets, list):
            return len(self.targets)
        elif isinstance(self.targets, int):
            return 1
        else:
            return len(self.client)

    def set_flags(self, **kwargs):
        """set my attribute flags by keyword.

        Views determine behavior with a few attributes (`block`, `track`, etc.).
        These attributes can be set all at once by name with this method.

        Parameters
        ----------
        block : bool
            whether to wait for results
        track : bool
            whether to create a MessageTracker to allow the user to
            safely edit after arrays and buffers during non-copying
            sends.
        """
        for name, value in kwargs.items():
            if name not in self._flag_names:
                raise KeyError("Invalid name: %r" % name)
            else:
                setattr(self, name, value)

    @contextmanager
    def temp_flags(self, **kwargs):
        """temporarily set flags, for use in `with` statements.

        See set_flags for permanent setting of flags

        Examples
        --------
        >>> view.track=False
        ...
        >>> with view.temp_flags(track=True):
        ...    ar = view.apply(dostuff, my_big_array)
        ...    ar.tracker.wait() # wait for send to finish
        >>> view.track
        False

        """
        # preflight: save flags, and set temporaries
        saved_flags = {}
        for f in self._flag_names:
            saved_flags[f] = getattr(self, f)
        self.set_flags(**kwargs)
        # yield to the with-statement block
        try:
            yield
        finally:
            # postflight: restore saved flags
            self.set_flags(**saved_flags)

    # ----------------------------------------------------------------
    # apply
    # ----------------------------------------------------------------

    def _sync_results(self):
        """to be called by @sync_results decorator

        after submitting any tasks.
        """
        delta = self.outstanding.difference(self.client.outstanding)
        completed = self.outstanding.intersection(delta)
        self.outstanding = self.outstanding.difference(completed)

    @sync_results
    @save_ids
    def _really_apply(self, f, args, kwargs, block=None, **options):
        """wrapper for client.send_apply_request"""
        raise NotImplementedError("Implement in subclasses")

    def apply(self, __ipp_f, *args, **kwargs):
        """calls ``f(*args, **kwargs)`` on remote engines, returning the result.

        This method sets all apply flags via this View's attributes.

        Returns :class:`~ipyparallel.client.asyncresult.AsyncResult`
        instance if ``self.block`` is False, otherwise the return value of
        ``f(*args, **kwargs)``.
        """
        return self._really_apply(__ipp_f, args, kwargs)

    def apply_async(self, __ipp_f, *args, **kwargs):
        """calls ``f(*args, **kwargs)`` on remote engines in a nonblocking manner.

        Returns :class:`~ipyparallel.client.asyncresult.AsyncResult` instance.
        """
        return self._really_apply(__ipp_f, args, kwargs, block=False)

    def apply_sync(self, __ipp_f, *args, **kwargs):
        """calls ``f(*args, **kwargs)`` on remote engines in a blocking manner,
        returning the result.
        """
        return self._really_apply(__ipp_f, args, kwargs, block=True)

    # ----------------------------------------------------------------
    # wrappers for client and control methods
    # ----------------------------------------------------------------
    @sync_results
    def spin(self):
        """spin the client, and sync"""
        self.client.spin()

    @sync_results
    def wait(self, jobs=None, timeout=-1):
        """waits on one or more `jobs`, for up to `timeout` seconds.

        Parameters
        ----------
        jobs : int, str, or list of ints and/or strs, or one or more AsyncResult objects
            ints are indices to self.history
            strs are msg_ids
            default: wait on all outstanding messages
        timeout : float
            a time in seconds, after which to give up.
            default is -1, which means no timeout

        Returns
        -------
        True : when all msg_ids are done
        False : timeout reached, some msg_ids still outstanding
        """
        if jobs is None:
            jobs = self.history
        return self.client.wait(jobs, timeout)

    def abort(self, jobs=None, targets=None, block=None):
        """Abort jobs on my engines.

        Note: only jobs that have not started yet can be aborted.
        To halt a running job,
        you must interrupt the engine(s) via the Cluster API.

        Parameters
        ----------
        jobs : None, str, list of strs, optional
            if None: abort all jobs.
            else: abort specific msg_id(s).
        """
        block = block if block is not None else self.block
        targets = targets if targets is not None else self.targets
        jobs = jobs if jobs is not None else list(self.outstanding)

        return self.client.abort(jobs=jobs, targets=targets, block=block)

    def queue_status(self, targets=None, verbose=False):
        """Fetch the Queue status of my engines"""
        targets = targets if targets is not None else self.targets
        return self.client.queue_status(targets=targets, verbose=verbose)

    def purge_results(self, jobs=[], targets=[]):
        """Instruct the controller to forget specific results."""
        if targets is None or targets == 'all':
            targets = self.targets
        return self.client.purge_results(jobs=jobs, targets=targets)

    def shutdown(self, targets=None, restart=False, hub=False, block=None):
        """Terminates one or more engine processes, optionally including the hub."""
        block = self.block if block is None else block
        if targets is None or targets == 'all':
            targets = self.targets
        return self.client.shutdown(
            targets=targets, restart=restart, hub=hub, block=block
        )

    def get_result(self, indices_or_msg_ids=None, block=None, owner=False):
        """return one or more results, specified by history index or msg_id.

        See :meth:`ipyparallel.client.client.Client.get_result` for details.
        """

        if indices_or_msg_ids is None:
            indices_or_msg_ids = -1
        if isinstance(indices_or_msg_ids, int):
            indices_or_msg_ids = self.history[indices_or_msg_ids]
        elif isinstance(indices_or_msg_ids, (list, tuple, set)):
            indices_or_msg_ids = list(indices_or_msg_ids)
            for i, index in enumerate(indices_or_msg_ids):
                if isinstance(index, int):
                    indices_or_msg_ids[i] = self.history[index]
        return self.client.get_result(indices_or_msg_ids, block=block, owner=owner)

    # -------------------------------------------------------------------
    # Map
    # -------------------------------------------------------------------

    @sync_results
    def map(self, f, *sequences, **kwargs):
        """override in subclasses"""
        raise NotImplementedError()

    def map_async(self, f, *sequences, **kwargs):
        """Parallel version of builtin :func:`python:map`, using this view's engines.

        This is equivalent to ``map(...block=False)``.

        See `self.map` for details.
        """
        if 'block' in kwargs:
            raise TypeError("map_async doesn't take a `block` keyword argument.")
        kwargs['block'] = False
        return self.map(f, *sequences, **kwargs)

    def map_sync(self, f, *sequences, **kwargs):
        """Parallel version of builtin :func:`python:map`, using this view's engines.

        This is equivalent to ``map(...block=True)``.

        See `self.map` for details.
        """
        if 'block' in kwargs:
            raise TypeError("map_sync doesn't take a `block` keyword argument.")
        kwargs['block'] = True
        return self.map(f, *sequences, **kwargs)

    def imap(self, f, *sequences, **kwargs):
        """Parallel version of :func:`itertools.imap`.

        See `self.map` for details.

        """

        return iter(self.map_async(f, *sequences, **kwargs))

    # -------------------------------------------------------------------
    # Decorators
    # -------------------------------------------------------------------

    def remote(self, block=None, **flags):
        """Decorator for making a RemoteFunction"""
        block = self.block if block is None else block
        return remote(self, block=block, **flags)

    def parallel(self, dist='b', block=None, **flags):
        """Decorator for making a ParallelFunction"""
        block = self.block if block is None else block
        return parallel(self, dist=dist, block=block, **flags)


class DirectView(View):
    """Direct Multiplexer View of one or more engines.

    These are created via indexed access to a client:

    >>> dv_1 = client[1]
    >>> dv_all = client[:]
    >>> dv_even = client[::2]
    >>> dv_some = client[1:3]

    This object provides dictionary access to engine namespaces:

    # push a=5:
    >>> dv['a'] = 5
    # pull 'foo':
    >>> dv['foo']

    """

    def __init__(self, client=None, socket=None, targets=None, **flags):
        super().__init__(client=client, socket=socket, targets=targets, **flags)

    @property
    def importer(self):
        """sync_imports(local=True) as a property.

        See sync_imports for details.

        """
        return self.sync_imports(True)

    @contextmanager
    def sync_imports(self, local=True, quiet=False):
        """Context Manager for performing simultaneous local and remote imports.

        'import x as y' will *not* work.  The 'as y' part will simply be ignored.

        If `local=True`, then the package will also be imported locally.

        If `quiet=True`, no output will be produced when attempting remote
        imports.

        Note that remote-only (`local=False`) imports have not been implemented.

        >>> with view.sync_imports():
        ...    from numpy import recarray
        importing recarray from numpy on engine(s)

        """

        local_import = builtins.__import__
        modules = set()
        results = []

        # get the calling frame
        # that's two steps up due to `@contextmanager`
        context_frame = inspect.getouterframes(inspect.currentframe())[2].frame

        @util.interactive
        def remote_import(name, fromlist, level):
            """the function to be passed to apply, that actually performs the import
            on the engine, and loads up the user namespace.
            """
            import sys

            user_ns = globals()
            mod = __import__(name, fromlist=fromlist, level=level)
            if fromlist:
                for key in fromlist:
                    user_ns[key] = getattr(mod, key)
            else:
                user_ns[name] = sys.modules[name]

        def view_import(name, globals={}, locals={}, fromlist=[], level=0):
            """the drop-in replacement for __import__, that optionally imports
            locally as well.
            """
            # don't override nested imports
            save_import = builtins.__import__
            builtins.__import__ = local_import

            import_frame = inspect.getouterframes(inspect.currentframe())[1].frame
            if import_frame is not context_frame:
                # only forward imports from the context frame,
                # not secondary imports
                # TODO: does this ever happen, or is the above `__import__` enough?
                return local_import(name, globals, locals, fromlist, level)

            if local:
                mod = local_import(name, globals, locals, fromlist, level)
            else:
                raise NotImplementedError("remote-only imports not yet implemented")

            key = name + ':' + ','.join(fromlist or [])
            if level <= 0 and key not in modules:
                modules.add(key)
                if not quiet:
                    if fromlist:
                        print(
                            "importing {} from {} on engine(s)".format(
                                ','.join(fromlist), name
                            )
                        )
                    else:
                        print("importing %s on engine(s)" % name)
                results.append(self.apply_async(remote_import, name, fromlist, level))
            # restore override
            builtins.__import__ = save_import

            return mod

        # override __import__
        builtins.__import__ = view_import
        try:
            # enter the block
            yield
        except ImportError:
            if local:
                raise
            else:
                # ignore import errors if not doing local imports
                pass
        finally:
            # always restore __import__
            builtins.__import__ = local_import

        for r in results:
            # raise possible remote ImportErrors here
            r.get()

    def use_dill(self):
        """Expand serialization support with dill

        adds support for closures, etc.

        This calls ipyparallel.serialize.use_dill() here and on each engine.
        """
        serialize.use_dill()
        return self.apply(serialize.use_dill)

    def use_cloudpickle(self):
        """Expand serialization support with cloudpickle.

        This calls ipyparallel.serialize.use_cloudpickle() here and on each engine.
        """
        serialize.use_cloudpickle()
        return self.apply(serialize.use_cloudpickle)

    def use_pickle(self):
        """Restore

        This reverts changes to serialization caused by `use_dill|.cloudpickle`.
        """
        serialize.use_pickle()
        return self.apply(serialize.use_pickle)

    @sync_results
    @save_ids
    def _really_apply(
        self, f, args=None, kwargs=None, targets=None, block=None, track=None
    ):
        """calls f(*args, **kwargs) on remote engines, returning the result.

        This method sets all of `apply`'s flags via this View's attributes.

        Parameters
        ----------
        f : callable
        args : list [default: empty]
        kwargs : dict [default: empty]
        targets : target list [default: self.targets]
            where to run
        block : bool [default: self.block]
            whether to block
        track : bool [default: self.track]
            whether to ask zmq to track the message, for safe non-copying sends

        Returns
        -------
        if self.block is False:
            returns AsyncResult
        else:
            returns actual result of f(*args, **kwargs) on the engine(s)
            This will be a list of self.targets is also a list (even length 1), or
            the single result if self.targets is an integer engine id
        """
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        block = self.block if block is None else block
        track = self.track if track is None else track
        targets = self.targets if targets is None else targets

        _idents, _targets = self.client._build_targets(targets)
        futures = []

        pf = PrePickled(f)
        pargs = [PrePickled(arg) for arg in args]
        pkwargs = {k: PrePickled(v) for k, v in kwargs.items()}

        for ident in _idents:
            future = self.client.send_apply_request(
                self._socket, pf, pargs, pkwargs, track=track, ident=ident
            )
            futures.append(future)
        if track:
            trackers = [_.tracker for _ in futures]
        else:
            trackers = []
        if isinstance(targets, int):
            futures = futures[0]
        ar = AsyncResult(
            self.client, futures, fname=getname(f), targets=_targets, owner=True
        )
        if block:
            try:
                return ar.get()
            except KeyboardInterrupt:
                pass
        return ar

    @sync_results
    def map(self, f, *sequences, block=None, track=False, return_exceptions=False):
        """Parallel version of builtin `map`, using this View's `targets`.

        There will be one task per target, so work will be chunked
        if the sequences are longer than `targets`.

        Results can be iterated as they are ready, but will become available in chunks.

        .. versionadded:: 7.0
            `return_exceptions`

        Parameters
        ----------
        f : callable
            function to be mapped
        *sequences : one or more sequences of matching length
            the sequences to be distributed and passed to `f`
        block : bool [default self.block]
            whether to wait for the result or not
        track : bool [default False]
            Track underlying zmq send to indicate when it is safe to modify memory.
            Only for zero-copy sends such as numpy arrays that are going to be modified in-place.
        return_exceptions : bool [default False]
            Return remote Exceptions in the result sequence instead of raising them.

        Returns
        -------
        If block=False
            An :class:`~ipyparallel.client.asyncresult.AsyncMapResult` instance.
            An object like AsyncResult, but which reassembles the sequence of results
            into a single list. AsyncMapResults can be iterated through before all
            results are complete.
        else
            A list, the result of ``map(f,*sequences)``
        """

        if block is None:
            block = self.block

        assert len(sequences) > 0, "must have some sequences to map onto!"
        pf = ParallelFunction(
            self, f, block=block, track=track, return_exceptions=return_exceptions
        )
        return pf.map(*sequences)

    @sync_results
    @save_ids
    def execute(self, code, silent=True, targets=None, block=None):
        """Executes `code` on `targets` in blocking or nonblocking manner.

        ``execute`` is always `bound` (affects engine namespace)

        Parameters
        ----------
        code : str
            the code string to be executed
        block : bool
            whether or not to wait until done to return
            default: self.block
        """
        block = self.block if block is None else block
        targets = self.targets if targets is None else targets

        _idents, _targets = self.client._build_targets(targets)
        futures = []
        for ident in _idents:
            future = self.client.send_execute_request(
                self._socket, code, silent=silent, ident=ident
            )
            futures.append(future)
        if isinstance(targets, int):
            futures = futures[0]
        ar = AsyncResult(
            self.client, futures, fname='execute', targets=_targets, owner=True
        )
        if block:
            try:
                ar.get()
                ar.wait_for_output()
            except KeyboardInterrupt:
                pass
        return ar

    def run(self, filename, targets=None, block=None):
        """Execute contents of `filename` on my engine(s).

        This simply reads the contents of the file and calls `execute`.

        Parameters
        ----------
        filename : str
            The path to the file
        targets : int/str/list of ints/strs
            the engines on which to execute
            default : all
        block : bool
            whether or not to wait until done
            default: self.block

        """
        with open(filename) as f:
            # add newline in case of trailing indented whitespace
            # which will cause SyntaxError
            code = f.read() + '\n'
        return self.execute(code, block=block, targets=targets)

    def update(self, ns):
        """update remote namespace with dict `ns`

        See `push` for details.
        """
        return self.push(ns, block=self.block, track=self.track)

    def push(self, ns, targets=None, block=None, track=None):
        """update remote namespace with dict `ns`

        Parameters
        ----------
        ns : dict
            dict of keys with which to update engine namespace(s)
        block : bool [default : self.block]
            whether to wait to be notified of engine receipt

        """

        block = block if block is not None else self.block
        track = track if track is not None else self.track
        targets = targets if targets is not None else self.targets
        # applier = self.apply_sync if block else self.apply_async
        if not isinstance(ns, dict):
            raise TypeError("Must be a dict, not %s" % type(ns))
        return self._really_apply(
            util._push, kwargs=ns, block=block, track=track, targets=targets
        )

    def get(self, key_s):
        """get object(s) by `key_s` from remote namespace

        see `pull` for details.
        """
        # block = block if block is not None else self.block
        return self.pull(key_s, block=True)

    def pull(self, names, targets=None, block=None):
        """get object(s) by `name` from remote namespace

        will return one object if it is a key.
        can also take a list of keys, in which case it will return a list of objects.
        """
        block = block if block is not None else self.block
        targets = targets if targets is not None else self.targets
        if isinstance(names, str):
            pass
        elif isinstance(names, (list, tuple, set)):
            for key in names:
                if not isinstance(key, str):
                    raise TypeError("keys must be str, not type %r" % type(key))
        else:
            raise TypeError("names must be strs, not %r" % names)
        return self._really_apply(util._pull, (names,), block=block, targets=targets)

    def scatter(
        self, key, seq, dist='b', flatten=False, targets=None, block=None, track=None
    ):
        """
        Partition a Python sequence and send the partitions to a set of engines.
        """
        block = block if block is not None else self.block
        track = track if track is not None else self.track
        targets = targets if targets is not None else self.targets

        # construct integer ID list:
        targets = self.client._build_targets(targets)[1]

        mapObject = Map.dists[dist]()
        nparts = len(targets)
        futures = []
        _lengths = []
        for index, engineid in enumerate(targets):
            partition = mapObject.getPartition(seq, index, nparts)
            if flatten and len(partition) == 1:
                ns = {key: partition[0]}
            else:
                ns = {key: partition}
            r = self.push(ns, block=False, track=track, targets=engineid)
            r.owner = False
            futures.extend(r._children)
            _lengths.append(len(partition))

        r = AsyncResult(
            self.client, futures, fname='scatter', targets=targets, owner=True
        )
        r._scatter_lengths = _lengths
        if block:
            r.wait()
        else:
            return r

    @sync_results
    @save_ids
    def gather(self, key, dist='b', targets=None, block=None):
        """
        Gather a partitioned sequence on a set of engines as a single local seq.
        """
        block = block if block is not None else self.block
        targets = targets if targets is not None else self.targets
        mapObject = Map.dists[dist]()
        msg_ids = []

        # construct integer ID list:
        targets = self.client._build_targets(targets)[1]

        futures = []
        for index, engineid in enumerate(targets):
            ar = self.pull(key, block=False, targets=engineid)
            ar.owner = False
            futures.extend(ar._children)

        r = AsyncMapResult(self.client, futures, mapObject, fname='gather')

        if block:
            try:
                return r.get()
            except KeyboardInterrupt:
                pass
        return r

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        self.update({key: value})

    def clear(self, targets=None, block=None):
        """Clear the remote namespaces on my engines."""
        block = block if block is not None else self.block
        targets = targets if targets is not None else self.targets
        return self.client.clear(targets=targets, block=block)

    # ----------------------------------------
    # activate for %px, %autopx, etc. magics
    # ----------------------------------------

    def activate(self, suffix=''):
        """Activate IPython magics associated with this View

        Defines the magics `%px, %autopx, %pxresult, %%px, %pxconfig`

        Parameters
        ----------
        suffix : str [default: '']
            The suffix, if any, for the magics.  This allows you to have
            multiple views associated with parallel magics at the same time.

            e.g. ``rc[::2].activate(suffix='_even')`` will give you
            the magics ``%px_even``, ``%pxresult_even``, etc. for running magics
            on the even engines.
        """

        from ipyparallel.client.magics import ParallelMagics

        ip = get_ipython()
        if ip is None:
            warnings.warn(
                "The IPython parallel magics (%px, etc.) only work within IPython."
            )
            return

        M = ParallelMagics(ip, self, suffix)
        ip.magics_manager.register(M)


@decorator
def _not_coalescing(method, self, *args, **kwargs):
    """Decorator for broadcast methods that can't use reply coalescing"""
    is_coalescing = self.is_coalescing
    try:
        self.is_coalescing = False
        return method(self, *args, **kwargs)
    finally:
        self.is_coalescing = is_coalescing


class BroadcastView(DirectView):
    is_coalescing = Bool(False)

    def _init_metadata(self, target_tuples):
        """initialize request metadata"""
        return dict(
            targets=target_tuples,
            is_broadcast=True,
            is_coalescing=self.is_coalescing,
        )

    def _make_async_result(self, message_future, s_idents, **kwargs):
        original_msg_id = message_future.msg_id
        if not self.is_coalescing:
            futures = []
            for ident in s_idents:
                msg_and_target_id = f'{original_msg_id}_{ident}'
                future = self.client.create_message_futures(
                    msg_and_target_id,
                    message_future.header,
                    async_result=True,
                    track=True,
                )
                self.client.outstanding.add(msg_and_target_id)
                self.client._outstanding_dict[ident].add(msg_and_target_id)
                self.outstanding.add(msg_and_target_id)
                futures.append(future[0])
            if original_msg_id in self.outstanding:
                self.outstanding.remove(original_msg_id)
        else:
            self.client.outstanding.add(original_msg_id)
            for ident in s_idents:
                self.client._outstanding_dict[ident].add(original_msg_id)
            futures = message_future

        ar = AsyncResult(self.client, futures, owner=True, **kwargs)

        if self.is_coalescing:
            # if coalescing, discard outstanding-tracking when we are done
            def _rm_outstanding(_):
                for ident in s_idents:
                    if ident in self.client._outstanding_dict:
                        self.client._outstanding_dict[ident].discard(original_msg_id)

            ar.add_done_callback(_rm_outstanding)

        return ar

    @sync_results
    @save_ids
    def _really_apply(
        self, f, args=None, kwargs=None, block=None, track=None, targets=None
    ):
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        block = self.block if block is None else block
        track = self.track if track is None else track
        targets = self.targets if targets is None else targets
        idents, _targets = self.client._build_targets(targets)

        pf = PrePickled(f)
        pargs = [PrePickled(arg) for arg in args]
        pkwargs = {k: PrePickled(v) for k, v in kwargs.items()}

        s_idents = [ident.decode("utf8") for ident in idents]
        target_tuples = list(zip(s_idents, _targets))

        metadata = self._init_metadata(target_tuples)

        ar = None

        def make_asyncresult(message_future):
            nonlocal ar
            ar = self._make_async_result(
                message_future, s_idents, fname=getname(f), targets=_targets
            )

        self.client.send_apply_request(
            self._socket,
            pf,
            pargs,
            pkwargs,
            track=track,
            metadata=metadata,
            message_future_hook=make_asyncresult,
        )

        if block:
            try:
                return ar.get()
            except KeyboardInterrupt:
                pass
        return ar

    @sync_results
    @save_ids
    @_not_coalescing
    def execute(self, code, silent=True, targets=None, block=None):
        """Executes `code` on `targets` in blocking or nonblocking manner.

        ``execute`` is always `bound` (affects engine namespace)

        Parameters
        ----------
        code : str
            the code string to be executed
        block : bool
            whether or not to wait until done to return
            default: self.block
        """
        block = self.block if block is None else block
        targets = self.targets if targets is None else targets

        _idents, _targets = self.client._build_targets(targets)
        s_idents = [ident.decode("utf8") for ident in _idents]
        target_tuples = list(zip(s_idents, _targets))

        metadata = self._init_metadata(target_tuples)

        ar = None

        def make_asyncresult(message_future):
            nonlocal ar
            ar = self._make_async_result(
                message_future, s_idents, fname='execute', targets=_targets
            )

        message_future = self.client.send_execute_request(
            self._socket,
            code,
            silent=silent,
            metadata=metadata,
            message_future_hook=make_asyncresult,
        )
        if block:
            try:
                ar.get()
                ar.wait_for_output()
            except KeyboardInterrupt:
                pass
        return ar

    @staticmethod
    def _broadcast_map(f, *sequence_names):
        """Function passed to apply

        Equivalent, but account for the fact that scatter
        occurs in a separate step.

        Does these things:
        - resolve sequence names to sequences in the user namespace
        - collect list(map(f, *squences))
        - cleanup temporary sequence variables from scatter
        """
        sequences = []
        ip = get_ipython()
        for seq_name in sequence_names:
            sequences.append(ip.user_ns.pop(seq_name))
        return list(map(f, *sequences))

    @_not_coalescing
    def map(self, f, *sequences, block=None, track=False, return_exceptions=False):
        """Parallel version of builtin `map`, using this View's `targets`.

        There will be one task per engine, so work will be chunked
        if the sequences are longer than `targets`.

        Results can be iterated as they are ready, but will become available in chunks.

        .. note::

            BroadcastView does not yet have a fully native map implementation.
            In particular, the scatter step is still one message per engine,
            identical to DirectView,
            and typically slower due to the more complex scheduler.

            It is more efficient to partition inputs via other means (e.g. SPMD based on rank & size)
            and use `apply` to submit all tasks in one broadcast.

        .. versionadded:: 8.8

        Parameters
        ----------
        f : callable
            function to be mapped
        *sequences : one or more sequences of matching length
            the sequences to be distributed and passed to `f`
        block : bool [default self.block]
            whether to wait for the result or not
        track : bool [default False]
            Track underlying zmq send to indicate when it is safe to modify memory.
            Only for zero-copy sends such as numpy arrays that are going to be modified in-place.
        return_exceptions : bool [default False]
            Return remote Exceptions in the result sequence instead of raising them.

        Returns
        -------
        If block=False
            An :class:`~ipyparallel.client.asyncresult.AsyncMapResult` instance.
            An object like AsyncResult, but which reassembles the sequence of results
            into a single list. AsyncMapResults can be iterated through before all
            results are complete.
        else
            A list, the result of ``map(f,*sequences)``
        """
        if block is None:
            block = self.block
        if track is None:
            track = self.track

        # unique identifier, since we're living in the interactive namespace
        map_key = secrets.token_hex(5)
        dist = 'b'
        map_object = Map.dists[dist]()

        seq_names = []
        for i, seq in enumerate(sequences):
            seq_name = f"_seq_{map_key}_{i}"
            seq_names.append(seq_name)
            try:
                len(seq)
            except Exception:
                # cast length-less sequences (e.g. Range) to list
                seq = list(seq)

            ar = self.scatter(seq_name, seq, dist=dist, block=False, track=track)
            scatter_chunk_sizes = ar._scatter_lengths

        # submit the map tasks as an actual broadcast
        ar = self.apply(self._broadcast_map, f, *seq_names)
        ar.owner = False
        # re-wrap messages in an AsyncMapResult to get map API
        # this is where the 'gather' reconstruction happens
        amr = ipp.AsyncMapResult(
            self.client,
            ar._children,
            map_object,
            fname=getname(f),
            return_exceptions=return_exceptions,
            chunk_sizes={
                future.msg_id: chunk_size
                for future, chunk_size in zip(ar._children, scatter_chunk_sizes)
            },
        )

        if block:
            return amr.get()
        else:
            return amr

    # scatter/gather cannot be coalescing yet
    scatter = _not_coalescing(DirectView.scatter)
    gather = _not_coalescing(DirectView.gather)


class LazyMapIterator:
    """Iterable representation of a lazy map (imap)

    Has a `.cancel()` method to stop consuming new inputs.

    .. versionadded:: 8.0
    """

    def __init__(self, gen, signal_done):
        self._gen = gen
        self._signal_done = signal_done

    def __iter__(self):
        return self._gen

    def __next__(self):
        return next(self._gen)

    def cancel(self):
        """Stop consuming the input to the map.

        Useful to e.g. stop consuming an infinite (or just large) input
        when you've arrived at the result (or error) you needed.
        """
        self._signal_done()


class LoadBalancedView(View):
    """An load-balancing View that only executes via the Task scheduler.

    Load-balanced views can be created with the client's `view` method:

    >>> v = client.load_balanced_view()

    or targets can be specified, to restrict the potential destinations:

    >>> v = client.load_balanced_view([1,3])

    which would restrict loadbalancing to between engines 1 and 3.

    """

    follow = Any()
    after = Any()
    timeout = CFloat()
    retries = Integer(0)

    _task_scheme = Any()
    _flag_names = List(
        ['targets', 'block', 'track', 'follow', 'after', 'timeout', 'retries']
    )
    _outstanding_maps = Set()

    def __init__(self, client=None, socket=None, **flags):
        super().__init__(client=client, socket=socket, **flags)
        self._task_scheme = client._task_scheme

    def _validate_dependency(self, dep):
        """validate a dependency.

        For use in `set_flags`.
        """
        if dep is None or isinstance(dep, (str, AsyncResult, Dependency)):
            return True
        elif isinstance(dep, (list, set, tuple)):
            for d in dep:
                if not isinstance(d, (str, AsyncResult)):
                    return False
        elif isinstance(dep, dict):
            if set(dep.keys()) != set(Dependency().as_dict().keys()):
                return False
            if not isinstance(dep['msg_ids'], list):
                return False
            for d in dep['msg_ids']:
                if not isinstance(d, str):
                    return False
        else:
            return False

        return True

    def _render_dependency(self, dep):
        """helper for building jsonable dependencies from various input forms."""
        if isinstance(dep, Dependency):
            return dep.as_dict()
        elif isinstance(dep, AsyncResult):
            return dep.msg_ids
        elif dep is None:
            return []
        else:
            # pass to Dependency constructor
            return list(Dependency(dep))

    def set_flags(self, **kwargs):
        """set my attribute flags by keyword.

        A View is a wrapper for the Client's apply method, but with attributes
        that specify keyword arguments, those attributes can be set by keyword
        argument with this method.

        Parameters
        ----------
        block : bool
            whether to wait for results
        track : bool
            whether to create a MessageTracker to allow the user to
            safely edit after arrays and buffers during non-copying
            sends.
        after : Dependency or collection of msg_ids
            Only for load-balanced execution (targets=None)
            Specify a list of msg_ids as a time-based dependency.
            This job will only be run *after* the dependencies
            have been met.
        follow : Dependency or collection of msg_ids
            Only for load-balanced execution (targets=None)
            Specify a list of msg_ids as a location-based dependency.
            This job will only be run on an engine where this dependency
            is met.
        timeout : float/int or None
            Only for load-balanced execution (targets=None)
            Specify an amount of time (in seconds) for the scheduler to
            wait for dependencies to be met before failing with a
            DependencyTimeout.
        retries : int
            Number of times a task will be retried on failure.
        """

        super().set_flags(**kwargs)
        for name in ('follow', 'after'):
            if name in kwargs:
                value = kwargs[name]
                if self._validate_dependency(value):
                    setattr(self, name, value)
                else:
                    raise ValueError("Invalid dependency: %r" % value)
        if 'timeout' in kwargs:
            t = kwargs['timeout']
            if not isinstance(t, (int, float, type(None))):
                raise TypeError("Invalid type for timeout: %r" % type(t))
            if t is not None:
                if t < 0:
                    raise ValueError("Invalid timeout: %s" % t)

            self.timeout = t

    @sync_results
    @save_ids
    def _really_apply(
        self,
        f,
        args=None,
        kwargs=None,
        block=None,
        track=None,
        after=None,
        follow=None,
        timeout=None,
        targets=None,
        retries=None,
    ):
        """calls f(*args, **kwargs) on a remote engine, returning the result.

        This method temporarily sets all of `apply`'s flags for a single call.

        Parameters
        ----------
        f : callable
        args : list [default: empty]
        kwargs : dict [default: empty]
        block : bool [default: self.block]
            whether to block
        track : bool [default: self.track]
            whether to ask zmq to track the message, for safe non-copying sends
        !!!!!! TODO : THE REST HERE  !!!!

        Returns
        -------
        if self.block is False:
            returns AsyncResult
        else:
            returns actual result of f(*args, **kwargs) on the engine(s)
            This will be a list of self.targets is also a list (even length 1), or
            the single result if self.targets is an integer engine id
        """

        # validate whether we can run
        if self._socket.closed():
            msg = "Task farming is disabled"
            if self._task_scheme == 'pure':
                msg += " because the pure ZMQ scheduler cannot handle"
                msg += " disappearing engines."
            raise RuntimeError(msg)

        if self._task_scheme == 'pure':
            # pure zmq scheme doesn't support extra features
            msg = "Pure ZMQ scheduler doesn't support the following flags:"
            "follow, after, retries, targets, timeout"
            if follow or after or retries or targets or timeout:
                # hard fail on Scheduler flags
                raise RuntimeError(msg)
            if isinstance(f, dependent):
                # soft warn on functional dependencies
                warnings.warn(msg, RuntimeWarning)

        # build args
        args = [] if args is None else args
        kwargs = {} if kwargs is None else kwargs
        block = self.block if block is None else block
        track = self.track if track is None else track
        after = self.after if after is None else after
        retries = self.retries if retries is None else retries
        follow = self.follow if follow is None else follow
        timeout = self.timeout if timeout is None else timeout
        targets = self.targets if targets is None else targets

        if not isinstance(retries, int):
            raise TypeError('retries must be int, not %r' % type(retries))

        if targets is None:
            idents = []
        else:
            idents = self.client._build_targets(targets)[0]
            # ensure *not* bytes
            idents = [ident.decode() for ident in idents]

        after = self._render_dependency(after)
        follow = self._render_dependency(follow)
        metadata = dict(
            after=after, follow=follow, timeout=timeout, targets=idents, retries=retries
        )

        future = self.client.send_apply_request(
            self._socket, f, args, kwargs, track=track, metadata=metadata
        )

        ar = AsyncResult(
            self.client,
            future,
            fname=getname(f),
            targets=None,
            owner=True,
        )
        if block:
            try:
                return ar.get()
            except KeyboardInterrupt:
                pass
        return ar

    @sync_results
    @save_ids
    def map(
        self,
        f,
        *sequences,
        block=None,
        chunksize=1,
        ordered=True,
        return_exceptions=False,
    ):
        """Parallel version of builtin `map`, load-balanced by this View.

        Each `chunksize` elements will be a separate task, and will be
        load-balanced. This lets individual elements be available for iteration
        as soon as they arrive.

        .. versionadded:: 7.0
            `return_exceptions`

        Parameters
        ----------
        f : callable
            function to be mapped
        *sequences : one or more sequences of matching length
            the sequences to be distributed and passed to `f`
        block : bool [default self.block]
            whether to wait for the result or not
        chunksize : int [default 1]
            how many elements should be in each task.
        ordered : bool [default True]
            Whether the results should be gathered as they arrive, or enforce
            the order of submission.

            Only applies when iterating through AsyncMapResult as results arrive.
            Has no effect when block=True.

        return_exceptions: bool [default False]
            Return Exceptions instead of raising on the first exception.

        Returns
        -------
        if block=False
            An :class:`~ipyparallel.client.asyncresult.AsyncMapResult` instance.
            An object like AsyncResult, but which reassembles the sequence of results
            into a single list. AsyncMapResults can be iterated through before all
            results are complete.
        else
            A list, the result of ``map(f,*sequences)``
        """

        # default
        if block is None:
            block = self.block

        assert len(sequences) > 0, "must have some sequences to map onto!"

        pf = ParallelFunction(
            self,
            f,
            block=block,
            chunksize=chunksize,
            ordered=ordered,
            return_exceptions=return_exceptions,
        )
        return pf.map(*sequences)

    def imap(
        self,
        f,
        *sequences,
        ordered=True,
        max_outstanding='auto',
        return_exceptions=False,
    ):
        """Parallel version of lazily-evaluated `imap`, load-balanced by this View.

        `ordered`, and `max_outstanding` can be specified by keyword only.

        Unlike other map functions in IPython Parallel,
        this one does not consume the full iterable before submitting work,
        returning a single 'AsyncMapResult' representing the full computation.

        Instead, it consumes iterables as they come, submitting up to `max_outstanding`
        tasks to the cluster before waiting on results (default: one task per engine).
        This allows it to work with infinite generators,
        and avoid potentially expensive read-ahead for large streams of inputs
        that may not fit in memory all at once.

        .. versionadded:: 7.0

        Parameters
        ----------
        f : callable
            function to be mapped
        *sequences : one or more sequences of matching length
            the sequences to be distributed and passed to `f`
        ordered : bool [default True]
            Whether the results should be yielded on a first-come-first-yield basis,
            or preserve the order of submission.

        max_outstanding : int [default len(engines)]
            The maximum number of tasks to be outstanding.

            max_outstanding=0 will greedily consume the whole generator
            (map_async may be more efficient).

            A limit of 1 should be strictly worse than running a local map,
            as there will be no parallelism.

            Use this to tune how greedily input generator should be consumed.

        return_exceptions : bool [default False]
            Return Exceptions instead of raising them.

        Returns
        -------

        lazily-evaluated generator, yielding results of `f` on each item of sequences.
        Yield-order depends on `ordered` argument.
        """

        assert len(sequences) > 0, "must have some sequences to map onto!"

        if max_outstanding == 'auto':
            max_outstanding = len(self)

        pf = PrePickled(f)

        map_id = secrets.token_bytes(16)

        # record that a map is outstanding, mainly for Executor.shutdown
        self._outstanding_maps.add(map_id)

        def signal_done():
            nonlocal iterator_done
            iterator_done = True
            self._outstanding_maps.discard(map_id)

        outstanding_lock = threading.Lock()

        if ordered:
            outstanding = deque()
            add_outstanding = outstanding.append
        else:
            outstanding = set()
            add_outstanding = outstanding.add

        def wait_for_ready():
            while not outstanding and not iterator_done:
                # no outstanding futures, need to wait for something to wait for
                time.sleep(0.1)
            if not outstanding:
                # nothing to wait for, iterator_done is True
                return []

            if ordered:
                with outstanding_lock:
                    return [outstanding.popleft()]
            else:
                # unordered, yield whatever finishes first, as soon as it's ready
                # repeat with timeout because the consumer thread may be adding to `outstanding`
                with outstanding_lock:
                    to_wait = outstanding.copy()
                done, _ = concurrent.futures.wait(
                    to_wait,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=0.5,
                )
                if done:
                    with outstanding_lock:
                        for f in done:
                            outstanding.remove(f)
                return done

        arg_iterator = iter(zip(*sequences))
        iterator_done = False

        # consume inputs in _another_ thread,
        # to avoid blocking the IO thread with a possibly blocking generator
        # only need one thread for this, though.
        consumer_pool = concurrent.futures.ThreadPoolExecutor(1)

        def consume_callback(f):
            if not iterator_done:
                consumer_pool.submit(consume_next)

        def consume_next():
            """Consume the next call from the argument iterator

            If max_outstanding, schedules consumption when the result finishes.
            If running with no limit, schedules another consumption immediately.
            """
            nonlocal iterator_done
            if iterator_done:
                return

            try:
                args = next(arg_iterator)
                ar = self.apply_async(pf, *args)
            except StopIteration:
                signal_done()
                return
            except Exception as e:
                # exception consuming iterator, propagate
                ar = concurrent.futures.Future()
                # mock get so it gets re-raised when awaited
                ar.get = lambda *args: ar.result()
                ar.set_exception(e)
                with outstanding_lock:
                    add_outstanding(ar)
                signal_done()
                return

            with outstanding_lock:
                add_outstanding(ar)
            if max_outstanding:
                ar.add_done_callback(consume_callback)
            else:
                consumer_pool.submit(consume_next)

        # kick it off
        # only need one if not using max_outstanding,
        # as each eventloop tick will submit a new item
        # otherwise, start one consumer for each slot, which will chain
        kickoff_count = 1 if max_outstanding == 0 else max_outstanding
        submit_futures = []
        for i in range(kickoff_count):
            submit_futures.append(consumer_pool.submit(consume_next))

        # await the first one, just in case it raises
        try:
            submit_futures[0].result()
        except Exception:
            # make sure we clean up
            signal_done()
            raise
        del submit_futures

        # wrap result-yielding in another call
        # because if this function is itself a generator
        # the first submission won't happen until the first result is requested
        def iter_results():
            nonlocal outstanding
            with consumer_pool:
                while not iterator_done:
                    # yield results as they become ready
                    for ready_ar in wait_for_ready():
                        yield ready_ar.get(return_exceptions=return_exceptions)

            # yield any remaining results
            if ordered:
                for ar in outstanding:
                    yield ar.get(return_exceptions=return_exceptions)
            else:
                while outstanding:
                    done, outstanding = concurrent.futures.wait(
                        outstanding, return_when=concurrent.futures.FIRST_COMPLETED
                    )
                    for ar in done:
                        yield ar.get(return_exceptions=return_exceptions)

        return LazyMapIterator(iter_results(), signal_done)

    def register_joblib_backend(self, name='ipyparallel', make_default=False):
        """Register this View as a joblib parallel backend

        To make this the default backend, set make_default=True.

        Use with::

            p = Parallel(backend='ipyparallel')
            ...

        See joblib docs for details

        Requires joblib >= 0.10

        .. versionadded:: 5.1
        """
        from joblib.parallel import register_parallel_backend

        from ._joblib import IPythonParallelBackend

        register_parallel_backend(
            name,
            lambda **kwargs: IPythonParallelBackend(view=self, **kwargs),
            make_default=make_default,
        )


class ViewExecutor(concurrent.futures.Executor):
    """A PEP-3148 Executor API for Views

    Access as view.executor
    """

    def __init__(self, view):
        self.view = view
        self._max_workers = len(self.view)

    def submit(self, fn, *args, **kwargs):
        """Same as View.apply_async"""
        return self.view.apply_async(fn, *args, **kwargs)

    def map(self, func, *iterables, **kwargs):
        """Return generator for View.map_async"""
        if 'timeout' in kwargs:
            warnings.warn("timeout unsupported in ViewExecutor.map")
            kwargs.pop('timeout')
        return self.view.imap(func, *iterables, **kwargs)

    def shutdown(self, wait=True):
        """ViewExecutor does *not* shutdown engines

        results are awaited if wait=True, but engines are *not* shutdown.
        """
        if wait:
            # wait for *submission* of outstanding maps,
            # otherwise view.wait won't know what to wait for
            outstanding_maps = getattr(self.view, "_outstanding_maps")
            if outstanding_maps:
                while outstanding_maps:
                    time.sleep(0.1)
            self.view.wait()


__all__ = ['LoadBalancedView', 'DirectView', 'ViewExecutor', 'BroadcastView']
