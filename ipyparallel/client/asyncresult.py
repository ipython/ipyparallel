"""AsyncResult objects for the client"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import concurrent.futures
import sys
import threading
import time
import warnings
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, FIRST_EXCEPTION, Future
from contextlib import contextmanager
from datetime import datetime
from functools import lru_cache, partial
from itertools import chain, repeat
from threading import Event

import zmq
from decorator import decorator
from IPython import get_ipython
from IPython.display import display, display_pretty, publish_display_data

from ipyparallel import error
from ipyparallel.util import _parse_date, compare_datetimes, progress, utcnow

from .futures import MessageFuture, multi_future


def _raw_text(s):
    display_pretty(s, raw=True)


_default = object()

# global empty tracker that's always done:
finished_tracker = zmq.MessageTracker()


@decorator
def check_ready(f, self, *args, **kwargs):
    """Check ready state prior to calling the method."""
    self.wait(0)
    if not self._ready:
        raise TimeoutError("result not ready")
    return f(self, *args, **kwargs)


_metadata_keys = []
# threading.TIMEOUT_MAX new in 3.2
_FOREVER = getattr(threading, 'TIMEOUT_MAX', int(1e6))


class AsyncResult(Future):
    """Class for representing results of non-blocking calls.

    Extends the interfaces of :py:class:`multiprocessing.pool.AsyncResult`
    and :py:class:`concurrent.futures.Future`.
    """

    msg_ids = None
    _targets = None
    _tracker = None
    _single_result = False
    owner = False
    _last_display_prefix = ""
    _stream_trailing_newline = True
    _chunk_sizes = None

    def __init__(
        self,
        client,
        children,
        fname='unknown',
        targets=None,
        owner=False,
        return_exceptions=False,
        chunk_sizes=None,
    ):
        super().__init__()
        if not isinstance(children, list):
            children = [children]
            self._single_result = True
        else:
            self._single_result = False

        self._return_exceptions = return_exceptions
        self._chunk_sizes = chunk_sizes or {}

        if isinstance(children[0], str):
            self.msg_ids = children
            self._children = []
        else:
            self._children = children
            self.msg_ids = [f.msg_id for f in children]

        self._client = client
        self._fname = fname
        self._targets = targets
        self.owner = owner

        self._ready = False
        self._ready_event = Event()
        self._output_ready = False
        self._output_event = Event()
        self._sent_event = Event()
        self._success = None
        if self._children:
            self._metadata = [f.output.metadata for f in self._children]
        else:
            self._metadata = [self._client.metadata[id] for id in self.msg_ids]
        self._init_futures()

    def _init_futures(self):
        """Build futures for results and output; hook up callbacks"""
        if not self._children:
            for msg_id in self.msg_ids:
                future = self._client._futures.get(msg_id, None)
                if not future:
                    result = self._client.results.get(msg_id, _default)
                    # result resides in local cache, construct already-resolved Future
                    if result is not _default:
                        future = MessageFuture(msg_id)
                        future.output = Future()
                        future.output.metadata = self.client.metadata[msg_id]
                        future.set_result(result)
                        future.output.set_result(None)
                if not future:
                    raise KeyError("No Future or result for msg_id: %s" % msg_id)
                self._children.append(future)

        self._result_future = multi_future(self._children)

        self._sent_future = multi_future([f.tracker for f in self._children])
        self._sent_future.add_done_callback(self._handle_sent)

        self._output_future = multi_future(
            [self._result_future] + [f.output for f in self._children]
        )
        # on completion of my constituents, trigger my own resolution
        self._result_future.add_done_callback(self._resolve_result)
        self._output_future.add_done_callback(self._resolve_output)
        self.add_done_callback(self._finalize_result)

    def _iopub_streaming_output_callback(self, eid, msg_future, msg):
        """Callback for iopub messages registered during AsyncResult.stream_output()"""
        msg_type = msg['header']['msg_type']
        ip = get_ipython()
        if ip is not None:
            in_kernel = getattr(ip, 'kernel', None) is not None
        else:
            in_kernel = False

        if msg_type == 'stream':
            msg_content = msg['content']
            stream_name = msg_content['name']

            if in_kernel:
                parent_msg_id = msg.get('parent_header', {}).get('msg_id', '')
                display_id = f"{parent_msg_id}-{stream_name}"
                md = msg_future.output.metadata
                full_stream = md[stream_name]
                if display_id in self._already_streamed:
                    update = True
                else:
                    self._already_streamed[display_id] = True
                    update = False
                publish_display_data(
                    {
                        "text/plain": f"[{stream_name}:{eid}] " + full_stream,
                    },
                    transient={"display_id": display_id},
                    update=update,
                )
                return
            else:
                stream = getattr(sys, stream_name, sys.stdout)
                self._display_stream(
                    msg_content['text'],
                    f'[{stream_name}:{eid}] ',
                    file=stream,
                )
        elif msg_type == "error":
            content = msg['content']
            if 'engine_info' not in content:
                content['engine_info'] = {
                    "engine_id": msg_future.output.metadata.engine_id,
                    "engine_uuid": msg_future.output.metadata.engine_uuid,
                    # always execute?
                    "method": msg_future.header["msg_type"].partition("_")[0],
                }

            err = self._client._unwrap_exception(msg['content'])
            self._streamed_errors += 1
            if self._streamed_errors <= error.CompositeError.tb_limit:
                print("\n".join(err.render_traceback()), file=sys.stderr)
            else:
                # single-line error after we hit the limit
                print(err, file=sys.stderr)
        elif msg_type == "execute_result":
            # mock ExecuteReply from execute_result on iopub
            from .client import ExecuteReply

            er = ExecuteReply(
                msg_id=msg_future.msg_id,
                content=msg['content'],
                metadata=msg_future.output.metadata,
            )
            display(er)

        if ip is None:
            return

        if msg_type == 'display_data':
            msg_content = msg['content']
            _raw_text(f'[output:{eid}]')
            self._republish_displaypub(msg_content, eid)

    @contextmanager
    def stream_output(self):
        """Stream output for this result as it arrives.

        Returns a context manager, during which output is streamed.
        """

        # Keep a handle on the futures so we can remove the callback later
        future_callbacks = {}
        self._already_streamed = {}
        self._stream_trailing_newline = True
        self._last_display_prefix = ""
        self._streamed_errors = 0

        for eid, msg_future in zip(self._targets, self._children):
            iopub_callback = partial(
                self._iopub_streaming_output_callback, eid, msg_future
            )
            future_callbacks[msg_future] = iopub_callback
            md = msg_future.output.metadata

            msg_future.iopub_callbacks.append(iopub_callback)
            # FIXME: there's still a race here
            # registering before publishing means possible duplicates,
            # while after means lost output

            # publish already-captured output immediately
            for name in ("stdout", "stderr"):
                text = md[name]
                if text:
                    iopub_callback(
                        {
                            "header": {"msg_type": "stream"},
                            "content": {"name": name, "text": text},
                        }
                    )
            for output in md["outputs"]:
                iopub_callback(
                    {
                        "header": {"msg_type": "display_data"},
                        "content": output,
                    }
                )
            if md["execute_result"]:
                iopub_callback(
                    {
                        "header": {"msg_type": "execute_result"},
                        "content": md["execute_result"],
                    }
                )

        try:
            yield
        finally:
            # clear stream cache
            self._already_streamed = {}

            # Remove the callbacks
            for msg_future, iopub_callback in future_callbacks.items():
                msg_future.iopub_callbacks.remove(iopub_callback)

    def __repr__(self):
        if self._ready:
            if self._success:
                state = "finished"
            else:
                state = "failed"
        else:
            state = "pending"
        return f"<{self.__class__.__name__}({self._fname}): {state}>"

    def __dir__(self):
        keys = dir(self.__class__)
        if not _metadata_keys:
            from .client import Metadata

            _metadata_keys.extend(Metadata().keys())
        keys.extend(_metadata_keys)
        return keys

    def _reconstruct_result(self, res):
        """Reconstruct our result from actual result list (always a list)

        Override me in subclasses for turning a list of results
        into the expected form.
        """
        if self._single_result:
            return res[0]
        else:
            return res

    def get(self, timeout=None, return_exceptions=None, return_when=None):
        """Return the result when it arrives.

        Arguments:

        timeout : int [default None]
            If `timeout` is not ``None`` and the result does not arrive within
            `timeout` seconds then ``TimeoutError`` is raised. If the
            remote call raised an exception then that exception will be reraised
            by get() inside a `RemoteError`.
        return_exceptions : bool [default False]
            If True, return Exceptions instead of raising them.
        return_when : None, ALL_COMPLETED, or FIRST_EXCEPTION
            FIRST_COMPLETED is not supported, and treated the same as ALL_COMPLETED.
            See :py:func:`concurrent.futures.wait` for documentation.

            When return_when=FIRST_EXCEPTION, will raise immediately on the first exception,
            rather than waiting for all results to finish before reporting errors.

        .. versionchanged:: 8.0
            Added `return_when` argument.
        """
        if return_when == FIRST_COMPLETED:
            # FIRST_COMPLETED unsupported, same as ALL_COMPLETED
            warnings.warn(
                "Ignoring unsupported AsyncResult.get(return_when=FIRST_COMPLETED)",
                UserWarning,
                stacklevel=2,
            )
            return_when = None
        elif return_when == ALL_COMPLETED:
            # None avoids call to .split() and is a tiny bit more efficient
            return_when = None

        if not self.ready():
            wait_result = self.wait(timeout, return_when=return_when)

        if return_exceptions is None:
            # default to attribute, if AsyncResult was created with return_exceptions=True
            return_exceptions = self._return_exceptions

        if self._ready:
            if self._success:
                return self.result()
            else:
                e = self.exception()
                if return_exceptions:
                    return self._reconstruct_result(self._raw_results)
                else:
                    raise e
        else:
            if return_when == FIRST_EXCEPTION:
                # this should only occur if there was an exception
                # any other situation should have triggered the ready branch above

                done, pending = wait_result
                for ar in done:
                    if not ar._success:
                        return ar.get(return_exceptions=return_exceptions)
            raise TimeoutError("Result not ready.")

    def _check_ready(self):
        if not self.ready():
            raise TimeoutError("Result not ready.")

    def ready(self):
        """Return whether the call has completed."""
        if not self._ready:
            self.wait(0)

        return self._ready

    def wait_for_output(self, timeout=-1):
        """Wait for our output to be complete.

        AsyncResult.wait only waits for the result,
        which may arrive before output is complete.
        """
        if self._output_ready:
            return True
        if timeout and timeout < 0:
            timeout = None
        return self._output_event.wait(timeout)

    def _resolve_output(self, f=None):
        """Callback that fires when outputs are ready"""
        if self.owner:
            [self._client.metadata.pop(mid, None) for mid in self.msg_ids]
        self._output_ready = True
        self._output_event.set()

    @classmethod
    def join(cls, *async_results):
        """Join multiple AsyncResults into one

        Inverse of .split(),
        used for rejoining split results in wait.

        .. versionadded:: 8.0
        """
        if not async_results:
            raise ValueError("Must specify at least one AsyncResult to join")
        first = async_results[0]
        if len(async_results) == 1:
            # only one AsyncResult, nothing to join
            return first

        return cls(
            client=first._client,
            fname=first._fname,
            return_exceptions=first._return_exceptions,
            children=list(chain(*(ar._children for ar in async_results))),
            targets=list(chain(*(ar._targets for ar in async_results))),
            owner=False,
        )

    @lru_cache
    def split(self):
        """Split an AsyncResult

        An AsyncResult object that represents multiple messages
        can be split to wait for individual results
        This can be passed to `concurrent.futures.wait` and friends
        to get partial results.

        .. versionadded:: 8.0
        """
        if len(self._children) == 1:
            # nothing to do if we're already representing a single message
            return (self,)
            self.owner = False

        if self._targets is None:
            _targets = repeat(None)
        else:
            _targets = self._targets

        flatten = not isinstance(self, AsyncMapResult)
        return tuple(
            AsyncResult(
                client=self._client,
                children=msg_future if flatten else [msg_future],
                targets=[engine_id],
                fname=self._fname,
                owner=False,
                return_exceptions=self._return_exceptions,
            )
            for engine_id, msg_future in zip(_targets, self._children)
        )

    def wait(self, timeout=-1, return_when=None):
        """Wait until the result is available or until `timeout` seconds pass.

        Arguments:

        timeout (int):
            The timeout in seconds. `-1` or None indicate an infinite timeout.
        return_when (enum):
            None, ALL_COMPLETED, FIRST_COMPLETED, or FIRST_EXCEPTION.
            Passed to :py:func:`concurrent.futures.wait`.
            If specified and not-None,

        Returns:
            ready (bool):
                For backward-compatibility.
                If `return_when` is None or unspecified,
                returns True if all tasks are done, False otherwise

            (done, pending):
                If `return_when` is any of the constants for :py:func:`concurrent.futures.wait`,
                will return two sets of AsyncResult objects
                representing the completed and still-pending subsets of results,
                matching the return value of `wait` itself.

        .. versionchanged:: 8.0
            Added `return_when`.
        """
        if timeout and timeout < 0:
            timeout = None
        if return_when is None:
            if self._ready:
                return True
            self._ready_event.wait(timeout)
            self.wait_for_output(0)
            return self._ready
        else:
            futures = self.split()
            done, pending = concurrent.futures.wait(
                futures, timeout=timeout, return_when=return_when
            )
            if done:
                self.wait_for_output(0)

            return done, pending

            # # simple cases: all done, or all pending
            # if not pending:
            #     return (None, self)
            # if not done:
            #     return (self, None)

    #
    # # neither set is empty, rejoin two subsets
    # return (self.__class__.join(*done), self.__class__.join(*pending))

    def _resolve_result(self, f=None):
        if self.done():
            return
        if f:
            results = f.result()
        else:
            results = list(map(self._client.results.get, self.msg_ids))

        # store raw results
        self._raw_results = results

        try:
            if self._single_result:
                r = results[0]
                if isinstance(r, Exception):
                    raise r
            else:
                results = self._collect_exceptions(results)
        except Exception as e:
            self._success = False
            self.set_exception(e)
        else:
            self._success = True
            self.set_result(self._reconstruct_result(results))

    def _collect_exceptions(self, results):
        """Wrap Exceptions in a CompositeError

        if self._return_exceptions is True, this is a no-op
        """
        if self._return_exceptions:
            return results
        else:
            return error.collect_exceptions(results, self._fname)

    def _finalize_result(self, f):
        if self.owner:
            [self._client.results.pop(mid, None) for mid in self.msg_ids]
        self._ready = True
        self._ready_event.set()

    def successful(self):
        """Return whether the call completed without raising an exception.

        Will raise ``RuntimeError`` if the result is not ready.
        """
        if not self.ready():
            raise RuntimeError("Cannot check successful() if not done.")
        return self._success

    # ----------------------------------------------------------------
    # Extra methods not in mp.pool.AsyncResult
    # ----------------------------------------------------------------

    def get_dict(self, timeout=-1):
        """Get the results as a dict, keyed by engine_id.

        timeout behavior is described in `get()`.
        """

        results = self.get(timeout)
        if self._single_result:
            results = [results]
        engine_ids = [md['engine_id'] for md in self._metadata]

        rdict = {}
        for engine_id, result in zip(engine_ids, results):
            if engine_id in rdict:
                n_jobs = engine_ids.count(engine_id)
                raise ValueError(
                    f"Cannot build dict, {n_jobs} jobs ran on engine #{engine_id}"
                )
            else:
                rdict[engine_id] = result

        return rdict

    @property
    def r(self):
        """result property wrapper for `get(timeout=-1)`."""
        return self.get()

    _DATE_FIELDS = [
        "submitted",
        "started",
        "completed",
        "received",
    ]

    def _parse_metadata_dates(self):
        """Ensure metadata date fields are parsed on access

        Rather than parsing timestamps from str->dt on receipt,
        parse on access for compatibility.
        """
        for md in self._metadata:
            for key in self._DATE_FIELDS:
                if isinstance(md.get(key, None), str):
                    md[key] = _parse_date(md[key])

    @property
    def metadata(self):
        """property for accessing execution metadata."""
        self._parse_metadata_dates()
        if self._single_result:
            return self._metadata[0]
        else:
            return self._metadata

    @property
    def result_dict(self):
        """result property as a dict."""
        return self.get_dict()

    def __dict__(self):
        return self.get_dict(0)

    def abort(self):
        """
        Abort my tasks, if possible.

        Only tasks that have not started yet can be aborted.

        Raises RuntimeError if already done.
        """
        if self.ready():
            raise RuntimeError("Can't abort, I am already done!")
        return self._client.abort(self.msg_ids, targets=self._targets, block=True)

    def _handle_sent(self, f):
        """Resolve sent Future, build MessageTracker"""
        trackers = f.result()
        trackers = [t for t in trackers if t is not None]
        self._tracker = zmq.MessageTracker(*trackers)
        self._sent_event.set()

    @property
    def sent(self):
        """check whether my messages have been sent."""
        return self._sent_event.is_set() and self._tracker.done

    def wait_for_send(self, timeout=-1):
        """wait for pyzmq send to complete.

        This is necessary when sending arrays that you intend to edit in-place.
        `timeout` is in seconds, and will raise TimeoutError if it is reached
        before the send completes.
        """
        if not self._sent_event.is_set():
            if timeout and timeout < 0:
                # Event doesn't like timeout < 0
                timeout = None
            elif timeout == 0:
                raise TimeoutError("Still waiting to be sent")
            # wait for Future to indicate send having been called,
            # which means MessageTracker is ready.
            tic = time.perf_counter()
            if not self._sent_event.wait(timeout):
                raise TimeoutError("Still waiting to be sent")
            if timeout:
                timeout = max(0, timeout - (time.perf_counter() - tic))
        try:
            if timeout is None:
                # MessageTracker doesn't like timeout=None
                timeout = -1
            return self._tracker.wait(timeout)
        except zmq.NotDone:
            raise TimeoutError("Still waiting to be sent")

    # -------------------------------------
    # dict-access
    # -------------------------------------

    def __getitem__(self, key):
        """getitem returns result value(s) if keyed by int/slice, or metadata if key is str."""
        if isinstance(key, int):
            self._check_ready()
            return self._collect_exceptions([self.result()[key]])[0]
        elif isinstance(key, slice):
            self._check_ready()
            return self._collect_exceptions(self.result()[key])
        elif isinstance(key, str):
            # metadata proxy *does not* require that results are done
            self.wait(0)
            self.wait_for_output(0)
            self._parse_metadata_dates()
            values = [md[key] for md in self._metadata]
            if self._single_result:
                return values[0]
            else:
                return values
        else:
            raise TypeError(
                "Invalid key type %r, must be 'int','slice', or 'str'" % type(key)
            )

    def __getattr__(self, key):
        """getattr maps to getitem for convenient attr access to metadata."""
        try:
            return self.__getitem__(key)
        except (TimeoutError, KeyError):
            raise AttributeError(
                f"{self.__class__.__name__!r} object has no attribute {key!r}"
            )

    @staticmethod
    def _wait_for_child(child, evt, timeout=_FOREVER):
        """Wait for a child to be done"""
        if child.done():
            return
        evt.clear()
        child.add_done_callback(lambda f: evt.set())
        evt.wait(timeout)

    # asynchronous iterator:
    def __iter__(self):
        if self._single_result:
            raise TypeError("AsyncResults with a single result are not iterable.")
        try:
            rlist = self.get(0)
        except TimeoutError:
            # wait for each result individually
            evt = Event()
            for child in self._children:
                self._wait_for_child(child, evt=evt)
                result = child.result()
                self._collect_exceptions([result])
                yield result
        else:
            # already done
            yield from rlist

    @lru_cache
    def __len__(self):
        return self._count_chunks(*self.msg_ids)

    @lru_cache
    def _count_chunks(self, *msg_ids):
        """Count the granular tasks"""
        return sum(self._chunk_sizes.setdefault(msg_id, 1) for msg_id in msg_ids)

    # -------------------------------------
    # Sugar methods and attributes
    # -------------------------------------

    def timedelta(self, start, end, start_key=min, end_key=max):
        """compute the difference between two sets of timestamps

        The default behavior is to use the earliest of the first
        and the latest of the second list, but this can be changed
        by passing a different

        Parameters
        ----------
        start : one or more datetime objects (e.g. ar.submitted)
        end : one or more datetime objects (e.g. ar.received)
        start_key : callable
            Function to call on `start` to extract the relevant
            entry [default: min]
        end_key : callable
            Function to call on `end` to extract the relevant
            entry [default: max]

        Returns
        -------
        dt : float
            The time elapsed (in seconds) between the two selected timestamps.
        """
        if not isinstance(start, datetime):
            # handle single_result AsyncResults, where ar.stamp is single object,
            # not a list
            start = start_key(start)
        if not isinstance(end, datetime):
            # handle single_result AsyncResults, where ar.stamp is single object,
            # not a list
            end = end_key(end)
        return compare_datetimes(end, start).total_seconds()

    @property
    def progress(self):
        """the number of tasks which have been completed at this point.

        Fractional progress would be given by 1.0 * ar.progress / len(ar)
        """
        self.wait(0)
        finished_msg_ids = set(self.msg_ids).intersection(self._client.outstanding)
        finished_count = self._count_chunks(*finished_msg_ids)
        return len(self) - finished_count

    @property
    def elapsed(self):
        """elapsed time since initial submission"""
        if self.ready():
            return self.wall_time

        now = submitted = utcnow()
        self._parse_metadata_dates()
        for md in self._metadata:
            stamp = md["submitted"]
            if stamp and stamp < submitted:
                submitted = stamp
        return compare_datetimes(now, submitted).total_seconds()

    @property
    @check_ready
    def serial_time(self):
        """serial computation time of a parallel calculation

        Computed as the sum of (completed-started) of each task
        """
        t = 0
        self._parse_metadata_dates()
        for md in self._metadata:
            t += compare_datetimes(md['completed'], md['started']).total_seconds()
        return t

    @property
    @check_ready
    def wall_time(self):
        """actual computation time of a parallel calculation

        Computed as the time between the latest `received` stamp
        and the earliest `submitted`.

        For similar comparison of other timestamp pairs, check out AsyncResult.timedelta.
        """
        return self.timedelta(self.submitted, self.received)

    def wait_interactive(
        self, interval=0.1, timeout=-1, widget=None, return_when=ALL_COMPLETED
    ):
        """interactive wait, printing progress at regular intervals.

        Parameters
        ----------
        interval : float
            Interval on which to update progress display.
        timeout : float
            Time (in seconds) to wait before raising a TimeoutError.
            -1 (default) means no timeout.
        widget : bool
            default: True if in an IPython kernel (notebook), False otherwise.
            Override default context-detection behavior for whether a widget-based progress bar
            should be used.
        return_when : concurrent.futures.ALL_COMPLETED | FIRST_EXCEPTION | FIRST_COMPLETED
        """
        if timeout and timeout < 0:
            timeout = None
        if return_when == ALL_COMPLETED:
            return_when = None
        N = len(self)
        tic = time.perf_counter()
        progress_bar = progress(widget=widget, total=N, unit='tasks', desc=self._fname)

        finished = self.ready()
        while not finished and (
            timeout is None or time.perf_counter() - tic <= timeout
        ):
            wait_result = self.wait(interval, return_when=return_when)
            progress_bar.update(self.progress - progress_bar.n)
            if return_when is None:
                finished = wait_result
            else:
                done, pending = wait_result
                if return_when == FIRST_COMPLETED:
                    finished = bool(done)
                elif return_when == FIRST_EXCEPTION:
                    finished = (not pending) or any(not ar._success for ar in done)
                else:
                    raise ValueError(f"Unrecognized return_when={return_when!r}")

        progress_bar.update(self.progress - progress_bar.n)
        progress_bar.close()

    def _republish_displaypub(self, content, eid):
        """republish individual displaypub content dicts"""
        ip = get_ipython()
        if ip is None:
            # displaypub is meaningless outside IPython
            return
        md = content['metadata'] or {}
        md['engine'] = eid
        ip.display_pub.publish(data=content['data'], metadata=md)

    def _display_stream(self, text, prefix='', file=None):
        """Redisplay a stream"""
        if not text:
            # nothing to display
            return
        if file is None:
            file = sys.stdout

        end = ""
        if prefix:
            if prefix == self._last_display_prefix:
                # same prefix, no need to re-display
                prefix = ""
            else:
                self._last_display_prefix = prefix

        if prefix and not self._stream_trailing_newline:
            # prefix changed, no trailing newline; insert newline
            pre = "\n"
        else:
            pre = ""

        if prefix:
            sep = "\n"
        else:
            sep = ""

        self._stream_trailing_newline = text.endswith("\n")
        print(f"{pre}{prefix}{sep}{text}", file=file, end="")

    def _display_single_result(self, result_only=False):
        if not result_only:
            self._display_stream(self.stdout)
            self._display_stream(self.stderr, file=sys.stderr)
        if get_ipython() is None:
            # displaypub is meaningless outside IPython
            return

        if not result_only:
            for output in self.outputs:
                self._republish_displaypub(output, self.engine_id)

        if self.execute_result is not None:
            display(self.get())

    @check_ready
    def display_outputs(self, groupby="type", result_only=False):
        """republish the outputs of the computation

        Parameters
        ----------
        groupby : str [default: type]
            if 'type':
                Group outputs by type (show all stdout, then all stderr, etc.):

                [stdout:1] foo
                [stdout:2] foo
                [stderr:1] bar
                [stderr:2] bar
            if 'engine':
                Display outputs for each engine before moving on to the next:

                [stdout:1] foo
                [stderr:1] bar
                [stdout:2] foo
                [stderr:2] bar

            if 'order':
                Like 'type', but further collate individual displaypub
                outputs.  This is meant for cases of each command producing
                several plots, and you would like to see all of the first
                plots together, then all of the second plots, and so on.

        result_only: boolean [default: False]
            Only display the execution result and skip stdout, stderr and
            display-outputs. Usually used when using streaming output
            since these outputs would have already been displayed.
        """
        self.wait_for_output()
        if self._single_result:
            self._display_single_result(result_only=result_only)
            return

        stdouts = self.stdout
        stderrs = self.stderr
        execute_results = self.execute_result
        output_lists = self.outputs
        results = self.get(return_exceptions=True)

        targets = self.engine_id

        if groupby == "engine":
            for eid, stdout, stderr, outputs, r, execute_result in zip(
                targets, stdouts, stderrs, output_lists, results, execute_results
            ):
                if not result_only:
                    self._display_stream(stdout, f'[stdout:{eid}] ')
                    self._display_stream(stderr, f'[stderr:{eid}] ', file=sys.stderr)

                if get_ipython() is None:
                    # displaypub is meaningless outside IPython
                    continue

                if (outputs and not result_only) or execute_result is not None:
                    _raw_text(f'[output:{eid}]')

                if not result_only:
                    for output in outputs:
                        self._republish_displaypub(output, eid)

                if execute_result is not None:
                    display(r)

        elif groupby in ('type', 'order'):
            if not result_only:
                # republish stdout:
                for eid, stdout in zip(targets, stdouts):
                    self._display_stream(stdout, f'[stdout:{eid}] ')

                # republish stderr:
                for eid, stderr in zip(targets, stderrs):
                    self._display_stream(stderr, f'[stderr:{eid}] ', file=sys.stderr)

            if get_ipython() is None:
                # displaypub is meaningless outside IPython
                return

            if not result_only:
                if groupby == 'order':
                    output_dict = {
                        eid: outputs for eid, outputs in zip(targets, output_lists)
                    }
                    N = max(len(outputs) for outputs in output_lists)
                    for i in range(N):
                        for eid in targets:
                            outputs = output_dict[eid]
                            if len(outputs) >= N:
                                _raw_text(f'[output:{eid}]')
                                self._republish_displaypub(outputs[i], eid)
                else:
                    # republish displaypub output
                    for eid, outputs in zip(targets, output_lists):
                        if outputs:
                            _raw_text(f'[output:{eid}]')
                        for output in outputs:
                            self._republish_displaypub(output, eid)

            # finally, add execute_result:
            for eid, r, execute_result in zip(targets, results, execute_results):
                if execute_result is not None:
                    display(r)

        else:
            raise ValueError(
                "groupby must be one of 'type', 'engine', 'collate', not %r" % groupby
            )


class AsyncMapResult(AsyncResult):
    """Class for representing results of non-blocking maps.

    AsyncMapResult.get() will properly reconstruct gathers into single object.

    AsyncMapResult is iterable at any time, and will wait on results as they come.

    If ordered=False, then the first results to arrive will come first, otherwise
    results will be yielded in the order they were submitted.
    """

    def __init__(
        self,
        client,
        children,
        mapObject,
        fname='',
        ordered=True,
        return_exceptions=False,
        chunk_sizes=None,
    ):
        self._mapObject = mapObject
        self.ordered = ordered
        AsyncResult.__init__(
            self,
            client,
            children,
            fname=fname,
            return_exceptions=return_exceptions,
            chunk_sizes=chunk_sizes,
        )
        self._single_result = False

    def _reconstruct_result(self, res):
        """Perform the gather on the actual results."""
        if self._return_exceptions:
            if any(isinstance(r, Exception) for r in res):
                # running with _return_exceptions,
                # cannot reconstruct original
                # use simple chain iterable
                flattened = []
                for r in res:
                    if isinstance(r, Exception):
                        flattened.append(r)
                    else:
                        flattened.extend(r)
                return flattened
        return self._mapObject.joinPartitions(res)

    # asynchronous iterator:
    def __iter__(self):
        it = self._ordered_iter if self.ordered else self._unordered_iter
        yield from it()

    def _yield_child_results(self, child):
        """Yield results from a child

        for use in iterator methods
        """
        rlist = child.result()
        if not isinstance(rlist, list):
            rlist = [rlist]
        self._collect_exceptions(rlist)
        yield from rlist

    # asynchronous ordered iterator:
    def _ordered_iter(self):
        """iterator for results *as they arrive*, preserving submission order."""
        try:
            rlist = self.get(0)
        except TimeoutError:
            # wait for each result individually
            evt = Event()
            for child in self._children:
                self._wait_for_child(child, evt=evt)
                yield from self._yield_child_results(child)
        else:
            # already done
            yield from rlist

    # asynchronous unordered iterator:
    def _unordered_iter(self):
        """iterator for results *as they arrive*, on FCFS basis, ignoring submission order."""
        try:
            rlist = self.get(0)
        except TimeoutError:
            pending = self._children
            while pending:
                done, pending = concurrent.futures.wait(
                    pending, return_when=FIRST_COMPLETED
                )
                for child in done:
                    yield from self._yield_child_results(child)
        else:
            # already done
            yield from rlist


class AsyncHubResult(AsyncResult):
    """Class to wrap pending results that must be requested from the Hub.

    Note that waiting/polling on these objects requires polling the Hub over the network,
    so use `AsyncHubResult.wait()` sparingly.
    """

    def _init_futures(self):
        """disable Future-based resolution of Hub results"""
        pass

    def wait(self, timeout=-1, return_when=None):
        """wait for result to complete."""
        start = time.perf_counter()
        if timeout and timeout < 0:
            timeout = None
        if self._ready:
            return True
        local_ids = [m for m in self.msg_ids if m in self._client.outstanding]
        local_ready = self._client.wait(local_ids, timeout)
        if local_ready:
            remote_ids = [m for m in self.msg_ids if m not in self._client.results]
            if not remote_ids:
                self._ready = True
            else:
                rdict = self._client.result_status(remote_ids, status_only=False)
                pending = rdict['pending']
                while pending and (
                    timeout is None or time.perf_counter() < start + timeout
                ):
                    rdict = self._client.result_status(remote_ids, status_only=False)
                    pending = rdict['pending']
                    if pending:
                        time.sleep(0.1)
                if not pending:
                    self._ready = True
        if self._ready:
            self._output_ready = True
            try:
                results = list(map(self._client.results.get, self.msg_ids))
                if self._single_result:
                    r = results[0]
                    if isinstance(r, Exception) and not self._return_exceptions:
                        raise r
                else:
                    results = self._collect_exceptions(results)
                self._success = True
                self.set_result(self._reconstruct_result(results))
            except Exception as e:
                self._success = False
                self.set_exception(e)
            finally:
                if self.owner:
                    [self._client.metadata.pop(mid) for mid in self.msg_ids]
                    [self._client.results.pop(mid) for mid in self.msg_ids]

        return self._ready


__all__ = ['AsyncResult', 'AsyncMapResult', 'AsyncHubResult']
