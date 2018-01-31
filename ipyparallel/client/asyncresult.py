"""AsyncResult objects for the client"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function

import sys
import time
from concurrent.futures import Future
from datetime import datetime
import threading
from threading import Event
try:
    from queue import Queue
except ImportError: # py2
    from Queue import Queue

from decorator import decorator
import zmq
from zmq import MessageTracker

from IPython import get_ipython
from IPython.core.display import clear_output, display, display_pretty
from ipyparallel import error
from ipyparallel.util import utcnow, compare_datetimes
from ipython_genutils.py3compat import string_types

from .futures import MessageFuture, multi_future

def _raw_text(s):
    display_pretty(s, raw=True)

_default = object()

# global empty tracker that's always done:
finished_tracker = MessageTracker()

@decorator
def check_ready(f, self, *args, **kwargs):
    """Check ready state prior to calling the method."""
    self.wait(0)
    if not self._ready:
        raise error.TimeoutError("result not ready")
    return f(self, *args, **kwargs)

_metadata_keys = []
# threading.TIMEOUT_MAX new in 3.2
_FOREVER = getattr(threading, 'TIMEOUT_MAX', int(1e6))

class AsyncResult(Future):
    """Class for representing results of non-blocking calls.

    Provides the same interface as :py:class:`multiprocessing.pool.AsyncResult`.
    """

    msg_ids = None
    _targets = None
    _tracker = None
    _single_result = False
    owner = False

    def __init__(self, client, children, fname='unknown', targets=None,
        owner=False,
    ):
        super(AsyncResult, self).__init__()
        if not isinstance(children, list):
            children = [children]
            self._single_result = True
        else:
            self._single_result = False
        
        if isinstance(children[0], string_types):
            self.msg_ids = children
            self._children = []
        else:
            self._children = children
            self.msg_ids = [ f.msg_id for f in children ]

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
            self._metadata = [ f.output.metadata for f in self._children ]
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
        
        self._sent_future = multi_future([ f.tracker for f in self._children ])
        self._sent_future.add_done_callback(self._handle_sent)
        
        self._output_future = multi_future([self._result_future] + [
            f.output for f in self._children
        ])
        # on completion of my constituents, trigger my own resolution
        self._result_future.add_done_callback(self._resolve_result)
        self._output_future.add_done_callback(self._resolve_output)
        self.add_done_callback(self._finalize_result)

    def __repr__(self):
        if self._ready:
            return "<%s: %s:finished>" % (self.__class__.__name__, self._fname)
        else:
            return "<%s: %s>" % (self.__class__.__name__, self._fname)
    
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

    def get(self, timeout=-1):
        """Return the result when it arrives.

        If `timeout` is not ``None`` and the result does not arrive within
        `timeout` seconds then ``TimeoutError`` is raised. If the
        remote call raised an exception then that exception will be reraised
        by get() inside a `RemoteError`.
        """
        if not self.ready():
            self.wait(timeout)

        if self._ready:
            if self._success:
                return self.result()
            else:
                raise self.exception()
        else:
            raise error.TimeoutError("Result not ready.")

    def _check_ready(self):
        if not self.ready():
            raise error.TimeoutError("Result not ready.")
    
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
            [ self._client.metadata.pop(mid, None) for mid in self.msg_ids ]
        self._output_ready = True
        self._output_event.set()
    
    def wait(self, timeout=-1):
        """Wait until the result is available or until `timeout` seconds pass.

        This method always returns None.
        """
        if self._ready:
            return True
        if timeout and timeout < 0:
            timeout = None
        
        self._ready_event.wait(timeout)
        self.wait_for_output(0)
        return self._ready
    
    def _resolve_result(self, f=None):
        try:
            if f:
                results = f.result()
            else:
                results = list(map(self._client.results.get, self.msg_ids))
            if self._single_result:
                r = results[0]
                if isinstance(r, Exception):
                    raise r
            else:
                results = error.collect_exceptions(results, self._fname)
            self._success = True
            self.set_result(self._reconstruct_result(results))
        except Exception as e:
            self._success = False
            self.set_exception(e)
    
    def _finalize_result(self,f):
        if self.owner:
            [ self._client.results.pop(mid, None) for mid in self.msg_ids ]
        self._ready = True
        self._ready_event.set()

    def successful(self):
        """Return whether the call completed without raising an exception.

        Will raise ``AssertionError`` if the result is not ready.
        """
        assert self.ready()
        return self._success

    #----------------------------------------------------------------
    # Extra methods not in mp.pool.AsyncResult
    #----------------------------------------------------------------

    def get_dict(self, timeout=-1):
        """Get the results as a dict, keyed by engine_id.

        timeout behavior is described in `get()`.
        """

        results = self.get(timeout)
        if self._single_result:
            results = [results]
        engine_ids = [ md['engine_id'] for md in self._metadata ]
        
        
        rdict = {}
        for engine_id, result in zip(engine_ids, results):
            if engine_id in rdict:
                raise ValueError("Cannot build dict, %i jobs ran on engine #%i" % (
                    engine_ids.count(engine_id), engine_id)
                )
            else:
                rdict[engine_id] = result

        return rdict

    @property
    def r(self):
        """result property wrapper for `get(timeout=-1)`."""
        return self.get()

    @property
    def metadata(self):
        """property for accessing execution metadata."""
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
        """abort my tasks."""
        assert not self.ready(), "Can't abort, I am already done!"
        return self._client.abort(self.msg_ids, targets=self._targets, block=True)
    
    def _handle_sent(self, f):
        """Resolve sent Future, build MessageTracker"""
        trackers = f.result()
        trackers = [t for t in trackers if t is not None]
        self._tracker = MessageTracker(*trackers)
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
                raise error.TimeoutError("Still waiting to be sent")
            # wait for Future to indicate send having been called,
            # which means MessageTracker is ready.
            tic = time.time()
            if not self._sent_event.wait(timeout):
                raise error.TimeoutError("Still waiting to be sent")
            if timeout:
                timeout = max(0, timeout - (time.time() - tic))
        try:
            if timeout is None:
                # MessageTracker doesn't like timeout=None
                timeout = -1
            return self._tracker.wait(timeout)
        except zmq.NotDone:
            raise error.TimeoutError("Still waiting to be sent")

    #-------------------------------------
    # dict-access
    #-------------------------------------

    def __getitem__(self, key):
        """getitem returns result value(s) if keyed by int/slice, or metadata if key is str.
        """
        if isinstance(key, int):
            self._check_ready()
            return error.collect_exceptions([self.result()[key]], self._fname)[0]
        elif isinstance(key, slice):
            self._check_ready()
            return error.collect_exceptions(self.result()[key], self._fname)
        elif isinstance(key, string_types):
            # metadata proxy *does not* require that results are done
            self.wait(0)
            self.wait_for_output(0)
            values = [ md[key] for md in self._metadata ]
            if self._single_result:
                return values[0]
            else:
                return values
        else:
            raise TypeError("Invalid key type %r, must be 'int','slice', or 'str'"%type(key))

    def __getattr__(self, key):
        """getattr maps to getitem for convenient attr access to metadata."""
        try:
            return self.__getitem__(key)
        except (error.TimeoutError, KeyError):
            raise AttributeError("%r object has no attribute %r"%(
                    self.__class__.__name__, key))
    
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
        except error.TimeoutError:
            # wait for each result individually
            evt = Event()
            for child in self._children:
                self._wait_for_child(child, evt=evt)
                result = child.result()
                error.collect_exceptions([result], self._fname)
                yield result
        else:
            # already done
            for r in rlist:
                yield r
    
    def __len__(self):
        return len(self.msg_ids)
    
    #-------------------------------------
    # Sugar methods and attributes
    #-------------------------------------
    
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
        return len(self) - len(set(self.msg_ids).intersection(self._client.outstanding))
    
    @property
    def elapsed(self):
        """elapsed time since initial submission"""
        if self.ready():
            return self.wall_time
        
        now = submitted = utcnow()
        for msg_id in self.msg_ids:
            if msg_id in self._client.metadata:
                stamp = self._client.metadata[msg_id]['submitted']
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
    
    def wait_interactive(self, interval=1., timeout=-1):
        """interactive wait, printing progress at regular intervals"""
        if timeout is None:
            timeout = -1
        N = len(self)
        tic = time.time()
        while not self.ready() and (timeout < 0 or time.time() - tic <= timeout):
            self.wait(interval)
            clear_output(wait=True)
            print("%4i/%i tasks finished after %4i s" % (self.progress, N, self.elapsed), end="")
            sys.stdout.flush()
        print("\ndone")
    
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
        if not text:
            # nothing to display
            return
        if file is None:
            file = sys.stdout
        end = '' if text.endswith('\n') else '\n'
        
        multiline = text.count('\n') > int(text.endswith('\n'))
        if prefix and multiline and not text.startswith('\n'):
            prefix = prefix + '\n'
        print("%s%s" % (prefix, text), file=file, end=end)
        
    
    def _display_single_result(self):
        self._display_stream(self.stdout)
        self._display_stream(self.stderr, file=sys.stderr)
        if get_ipython() is None:
            # displaypub is meaningless outside IPython
            return
        
        for output in self.outputs:
            self._republish_displaypub(output, self.engine_id)
        
        if self.execute_result is not None:
            display(self.get())
    
    @check_ready
    def display_outputs(self, groupby="type"):
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
        """
        self.wait_for_output()
        if self._single_result:
            self._display_single_result()
            return
        
        stdouts = self.stdout
        stderrs = self.stderr
        execute_results  = self.execute_result
        output_lists = self.outputs
        results = self.get()
        
        targets = self.engine_id
        
        if groupby == "engine":
            for eid,stdout,stderr,outputs,r,execute_result in zip(
                    targets, stdouts, stderrs, output_lists, results, execute_results
                ):
                self._display_stream(stdout, '[stdout:%i] ' % eid)
                self._display_stream(stderr, '[stderr:%i] ' % eid, file=sys.stderr)
                
                if get_ipython() is None:
                    # displaypub is meaningless outside IPython
                    continue
                
                if outputs or execute_result is not None:
                    _raw_text('[output:%i]' % eid)
                
                for output in outputs:
                    self._republish_displaypub(output, eid)
                
                if execute_result is not None:
                    display(r)
        
        elif groupby in ('type', 'order'):
            # republish stdout:
            for eid,stdout in zip(targets, stdouts):
                self._display_stream(stdout, '[stdout:%i] ' % eid)

            # republish stderr:
            for eid,stderr in zip(targets, stderrs):
                self._display_stream(stderr, '[stderr:%i] ' % eid, file=sys.stderr)

            if get_ipython() is None:
                # displaypub is meaningless outside IPython
                return
            
            if groupby == 'order':
                output_dict = dict((eid, outputs) for eid,outputs in zip(targets, output_lists))
                N = max(len(outputs) for outputs in output_lists)
                for i in range(N):
                    for eid in targets:
                        outputs = output_dict[eid]
                        if len(outputs) >= N:
                            _raw_text('[output:%i]' % eid)
                            self._republish_displaypub(outputs[i], eid)
            else:
                # republish displaypub output
                for eid,outputs in zip(targets, output_lists):
                    if outputs:
                        _raw_text('[output:%i]' % eid)
                    for output in outputs:
                        self._republish_displaypub(output, eid)
        
            # finally, add execute_result:
            for eid,r,execute_result in zip(targets, results, execute_results):
                if execute_result is not None:
                    display(r)
        
        else:
            raise ValueError("groupby must be one of 'type', 'engine', 'collate', not %r" % groupby)
        
        


class AsyncMapResult(AsyncResult):
    """Class for representing results of non-blocking gathers.

    This will properly reconstruct the gather.
    
    This class is iterable at any time, and will wait on results as they come.
    
    If ordered=False, then the first results to arrive will come first, otherwise
    results will be yielded in the order they were submitted.
    
    """

    def __init__(self, client, children, mapObject, fname='', ordered=True):
        self._mapObject = mapObject
        self.ordered = ordered
        AsyncResult.__init__(self, client, children, fname=fname)
        self._single_result = False

    def _reconstruct_result(self, res):
        """Perform the gather on the actual results."""
        return self._mapObject.joinPartitions(res)

    # asynchronous iterator:
    def __iter__(self):
        it = self._ordered_iter if self.ordered else self._unordered_iter
        for r in it():
            yield r
    
    def _yield_child_results(self, child):
        """Yield results from a child
        
        for use in iterator methods
        """
        rlist = child.result()
        if not isinstance(rlist, list):
            rlist = [rlist]
        error.collect_exceptions(rlist, self._fname)
        for r in rlist:
            yield r
    
    # asynchronous ordered iterator:
    def _ordered_iter(self):
        """iterator for results *as they arrive*, preserving submission order."""
        try:
            rlist = self.get(0)
        except error.TimeoutError:
            # wait for each result individually
            evt = Event()
            for child in self._children:
                self._wait_for_child(child, evt=evt)
                for r in self._yield_child_results(child):
                    yield r
        else:
            # already done
            for r in rlist:
                yield r

    # asynchronous unordered iterator:
    def _unordered_iter(self):
        """iterator for results *as they arrive*, on FCFS basis, ignoring submission order."""
        try:
            rlist = self.get(0)
        except error.TimeoutError:
            queue = Queue()
            for child in self._children:
                child.add_done_callback(queue.put)
            for i in range(len(self)):
                # use very-large timeout because no-timeout is not interruptible
                child = queue.get(timeout=_FOREVER)
                for r in self._yield_child_results(child):
                    yield r
        else:
            # already done
            for r in rlist:
                yield r


class AsyncHubResult(AsyncResult):
    """Class to wrap pending results that must be requested from the Hub.

    Note that waiting/polling on these objects requires polling the Hub over the network,
    so use `AsyncHubResult.wait()` sparingly.
    """
    
    def _init_futures(self):
        """disable Future-based resolution of Hub results"""
        pass

    def wait(self, timeout=-1):
        """wait for result to complete."""
        start = time.time()
        if self._ready:
            return
        local_ids = [m for m in self.msg_ids if m in self._client.outstanding]
        local_ready = self._client.wait(local_ids, timeout)
        if local_ready:
            remote_ids = [m for m in self.msg_ids if m not in self._client.results]
            if not remote_ids:
                self._ready = True
            else:
                rdict = self._client.result_status(remote_ids, status_only=False)
                pending = rdict['pending']
                while pending and (timeout < 0 or time.time() < start+timeout):
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
                    if isinstance(r, Exception):
                        raise r
                        self.set_result(r)
                else:
                    results = error.collect_exceptions(results, self._fname)
                self._success = True
                self.set_result(self._reconstruct_result(results))
            except Exception as e:
                self._success = False
                self.set_exception(e)
            finally:
                if self.owner:
                    [self._client.metadata.pop(mid) for mid in self.msg_ids]
                    [self._client.results.pop(mid) for mid in self.msg_ids]
            

__all__ = ['AsyncResult', 'AsyncMapResult', 'AsyncHubResult']
