"""Tests for parallel client.py"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import os
import signal
import socket
import sys
import time
import warnings
from concurrent.futures import Future
from datetime import datetime
from threading import Thread
from unittest import mock

import pytest
import tornado
from IPython import get_ipython

from ipyparallel import AsyncHubResult, DirectView, Reference, error
from ipyparallel.client import client as clientmod
from ipyparallel.util import utc

from .clienttest import ClusterTestCase, raises_remote, skip_without, wait


@pytest.mark.usefixtures('ipython')
class TestClient(ClusterTestCase):
    engine_count = 4

    def test_curve(self):
        if os.environ.get("IPP_ENABLE_CURVE") == "1":
            assert not self.client.session.key
            assert self.client.curve_serverkey
            assert self.client.curve_secretkey
        else:
            assert self.client.session.key

    def test_ids(self):
        n = len(self.client.ids)
        self.add_engines(2)
        assert len(self.client.ids) == n + 2

    def test_iter(self):
        self.minimum_engines(4)
        engine_ids = [view.targets for view in self.client]
        assert engine_ids == self.client.ids

    def test_view_indexing(self):
        """test index access for views"""
        self.minimum_engines(4)
        targets = self.client._build_targets('all')[-1]
        v = self.client[:]
        assert v.targets == targets
        t = self.client.ids[2]
        v = self.client[t]
        assert isinstance(v, DirectView)
        assert v.targets == t
        t = self.client.ids[2:4]
        v = self.client[t]
        assert isinstance(v, DirectView)
        assert v.targets == t
        v = self.client[::2]
        assert isinstance(v, DirectView)
        assert v.targets == targets[::2]
        v = self.client[1::3]
        assert isinstance(v, DirectView)
        assert v.targets == targets[1::3]
        v = self.client[:-3]
        assert isinstance(v, DirectView)
        assert v.targets == targets[:-3]
        v = self.client[-1]
        assert isinstance(v, DirectView)
        assert v.targets == targets[-1]
        with pytest.raises(TypeError):
            self.client[None]

    def test_outstanding(self):
        self.minimum_engines(1)
        e = self.client[-1]
        engine_id = self.client._engines[e.targets]
        ar = e.apply_async(time.sleep, 0.5)
        msg_id = ar.msg_ids[0]
        # verify that msg id
        assert msg_id in self.client.outstanding
        assert msg_id in self.client._outstanding_dict[engine_id]
        ar.get()
        assert msg_id not in self.client.outstanding
        assert msg_id not in self.client._outstanding_dict[engine_id]

    def test_lbview_targets(self):
        """test load_balanced_view targets"""
        v = self.client.load_balanced_view()
        assert v.targets is None
        v = self.client.load_balanced_view(-1)
        assert v.targets == [self.client.ids[-1]]
        v = self.client.load_balanced_view('all')
        assert v.targets is None

    def test_dview_targets(self):
        """test direct_view targets"""
        v = self.client.direct_view()
        assert v.targets == 'all'
        v = self.client.direct_view('all')
        assert v.targets == 'all'
        v = self.client.direct_view(-1)
        assert v.targets == self.client.ids[-1]

    def test_lazy_all_targets(self):
        """test lazy evaluation of rc.direct_view('all')"""
        v = self.client.direct_view()
        assert v.targets == 'all'

        def double(x):
            return x * 2

        seq = list(range(100))
        ref = [double(x) for x in seq]

        # add some engines, which should be used
        self.add_engines(1)
        n1 = len(self.client.ids)

        # simple apply
        r = v.apply_sync(lambda: 1)
        assert r == [1] * n1

        # map goes through remotefunction
        r = v.map_sync(double, seq)
        assert r == ref

        # add a couple more engines, and try again
        self.add_engines(2)
        n2 = len(self.client.ids)
        assert n2 != n1

        # apply
        r = v.apply_sync(lambda: 1)
        assert r == [1] * n2

        # map
        r = v.map_sync(double, seq)
        assert r == ref

    def test_targets(self):
        """test various valid targets arguments"""
        build = self.client._build_targets
        ids = self.client.ids
        idents, targets = build(None)
        assert ids == targets

    def test_clear(self):
        """test clear behavior"""
        self.minimum_engines(2)
        v = self.client[:]
        v.block = True
        v.push(dict(a=5))
        v.pull('a')
        id0 = self.client.ids[-1]
        self.client.clear(targets=id0, block=True)
        a = self.client[:-1].get('a')
        with raises_remote(NameError):
            self.client[id0].get('a')
        self.client.clear(block=True)
        for i in self.client.ids:
            with raises_remote(NameError):
                self.client[i].get('a')

    def test_get_result(self):
        """test getting results from the Hub."""
        c = clientmod.Client(profile='iptest')
        t = c.ids[-1]
        ar = c[t].apply_async(wait, 1)
        # give the monitor time to notice the message
        time.sleep(0.25)
        ahr = self.client.get_result(ar.msg_ids[0], owner=False)
        assert isinstance(ahr, AsyncHubResult)
        assert ahr.get() == ar.get()
        ar2 = self.client.get_result(ar.msg_ids[0])
        assert not isinstance(ar2, AsyncHubResult)
        assert ahr.get() == ar2.get()
        ar3 = self.client.get_result(ar2)
        assert ar3.msg_ids == ar2.msg_ids
        ar3 = self.client.get_result([ar2])
        assert ar3.msg_ids == ar2.msg_ids
        c.close()

    def test_get_execute_result(self):
        """test getting execute results from the Hub."""
        c = clientmod.Client(profile='iptest')
        t = c.ids[-1]
        cell = '\n'.join(['import time', 'time.sleep(0.25)', '5'])
        ar = c[t].execute("import time; time.sleep(1)", silent=False)
        # give the monitor time to notice the message
        time.sleep(0.25)
        ahr = self.client.get_result(ar.msg_ids[0], owner=False)
        assert isinstance(ahr, AsyncHubResult)
        assert ahr.get().execute_result == ar.get().execute_result
        ar2 = self.client.get_result(ar.msg_ids[0])
        assert not isinstance(ar2, AsyncHubResult)
        assert ahr.get() == ar2.get()
        c.close()

    def test_ids_list(self):
        """test client.ids"""
        ids = self.client.ids
        assert ids == self.client._ids
        assert ids is not self.client._ids
        ids.remove(ids[-1])
        assert ids != self.client._ids

    def test_queue_status(self):
        ids = self.client.ids
        id0 = ids[0]
        qs = self.client.queue_status(targets=id0)
        assert isinstance(qs, dict)
        assert sorted(qs.keys()), ['completed', 'queue' == 'tasks']
        allqs = self.client.queue_status()
        assert isinstance(allqs, dict)
        intkeys = list(allqs.keys())
        intkeys.remove('unassigned')
        print("intkeys", intkeys)
        intkeys = sorted(intkeys)
        ids = self.client.ids
        print("client.ids", ids)
        ids = sorted(self.client.ids)
        assert intkeys == ids
        unassigned = allqs.pop('unassigned')
        for eid, qs in allqs.items():
            assert isinstance(qs, dict)
            assert sorted(qs.keys()), ['completed', 'queue' == 'tasks']

    @pytest.mark.skipif(os.name == 'nt', reason='timing out on Windows')
    def test_shutdown(self):
        ids = self.client.ids
        id0 = ids[-1]
        pid = self.client[id0].apply_sync(os.getpid)

        self.client.shutdown(id0, block=True)

        for i in range(150):
            # give the engine 15 seconds to die
            if id0 not in self.client.ids:
                break
            time.sleep(0.1)
        assert id0 not in self.client.ids
        with pytest.raises(IndexError):
            self.client[id0]

    def test_result_status(self):
        pass
        # to be written

    def test_db_query_dt(self):
        """test db query by date"""
        hub_n_before = len(self.client.hub_history())
        client_n_before = len(self.client.history)

        for i in range(4):
            self.client[:].apply_sync(lambda: 1)
        new_entries = len(self.client.history) - client_n_before
        hist = self.client.hub_history()
        for i in range(10):
            if len(hist) < hub_n_before + new_entries:
                time.sleep(0.5)
                hist = self.client.hub_history()
            else:
                break
        else:
            raise TimeoutError(
                f"Timeout waiting for {new_entries} entries in the hub history"
            )

        print(hist)
        middle = self.client.db_query({'msg_id': hist[len(hist) // 2]})[0]
        tic = middle['submitted']
        before = self.client.db_query({'submitted': {'$lt': tic}})
        after = self.client.db_query({'submitted': {'$gte': tic}})
        assert len(before) + len(after) == len(hist)
        for b in before:
            assert b['submitted'] < tic
        for a in after:
            assert a['submitted'] >= tic
        same = self.client.db_query({'submitted': tic})
        for s in same:
            assert s['submitted'] == tic

    def test_db_query_keys(self):
        """test extracting subset of record keys"""
        found = self.client.db_query(
            {'msg_id': {'$ne': ''}}, keys=['submitted', 'completed']
        )
        for rec in found:
            assert set(rec.keys()), {'msg_id', 'submitted' == 'completed'}

    def test_db_query_default_keys(self):
        """default db_query excludes buffers"""
        found = self.client.db_query({'msg_id': {'$ne': ''}})
        for rec in found:
            keys = set(rec.keys())
            assert 'buffers' not in keys
            assert 'result_buffers' not in keys

    def test_db_query_msg_id(self):
        """ensure msg_id is always in db queries"""
        found = self.client.db_query(
            {'msg_id': {'$ne': ''}}, keys=['submitted', 'completed']
        )
        for rec in found:
            assert 'msg_id' in rec.keys()
        found = self.client.db_query({'msg_id': {'$ne': ''}}, keys=['submitted'])
        for rec in found:
            assert 'msg_id' in rec.keys()
        found = self.client.db_query({'msg_id': {'$ne': ''}}, keys=['msg_id'])
        for rec in found:
            assert 'msg_id' in rec.keys()

    def test_db_query_get_result(self):
        """pop in db_query shouldn't pop from result itself"""
        self.client[:].apply_sync(lambda: 1)
        found = self.client.db_query({'msg_id': {'$ne': ''}})
        rc2 = clientmod.Client(profile='iptest')
        # If this bug is not fixed, this call will hang:
        ar = rc2.get_result(self.client.history[-1])
        print("mark")
        ar.wait(2)
        print("mark2")
        assert ar.ready()
        print("mark3")
        ar.get()
        rc2.close()

    def test_db_query_in(self):
        """test db query with '$in','$nin' operators"""
        hist = self.client.hub_history()
        even = hist[::2]
        odd = hist[1::2]
        recs = self.client.db_query({'msg_id': {'$in': even}})
        found = [r['msg_id'] for r in recs]
        assert set(even) == set(found)
        recs = self.client.db_query({'msg_id': {'$nin': even}})
        found = [r['msg_id'] for r in recs]
        assert set(odd) == set(found)

    def test_hub_history(self):
        hist = self.client.hub_history()
        recs = self.client.db_query({'msg_id': {"$ne": ''}})
        recdict = {}
        for rec in recs:
            recdict[rec['msg_id']] = rec

        latest = datetime(1984, 1, 1).replace(tzinfo=utc)
        for msg_id in hist:
            rec = recdict[msg_id]
            newt = rec['submitted']
            assert newt >= latest
            latest = newt
        ar = self.client[-1].apply_async(lambda: 1)
        ar.get()
        time.sleep(0.25)
        assert self.client.hub_history()[-1:] == ar.msg_ids

    def _wait_for_idle(self):
        """wait for the cluster to become idle, according to the everyone."""
        rc = self.client

        # step 0. wait for local results
        # this should be sufficient 99% of the time.
        rc.wait(timeout=5)

        # step 1. wait for all requests to be noticed
        # timeout 5s, polling every 100ms
        msg_ids = set(rc.history)
        hub_hist = rc.hub_history()
        for i in range(50):
            if msg_ids.difference(hub_hist):
                time.sleep(0.1)
                hub_hist = rc.hub_history()
            else:
                break

        assert len(msg_ids.difference(hub_hist)) == 0

        # step 2. wait for all requests to be done
        # timeout 5s, polling every 100ms
        qs = rc.queue_status()
        for i in range(50):
            if qs['unassigned'] or any(
                qs[eid]['tasks'] + qs[eid]['queue'] for eid in qs if eid != 'unassigned'
            ):
                time.sleep(0.1)
                qs = rc.queue_status()
            else:
                break

        # ensure Hub up to date:
        assert qs['unassigned'] == 0
        for eid in [eid for eid in qs if eid != 'unassigned']:
            assert qs[eid]['tasks'] == 0
            assert qs[eid]['queue'] == 0

    def test_resubmit(self):
        def f():
            import random

            return random.random()

        v = self.client.load_balanced_view()
        ar = v.apply_async(f)
        r1 = ar.get(1)
        # give the Hub a chance to notice:
        self._wait_for_idle()
        ahr = self.client.resubmit(ar.msg_ids)
        r2 = ahr.get(1)
        assert r1 != r2

    def test_resubmit_chain(self):
        """resubmit resubmitted tasks"""
        v = self.client.load_balanced_view()
        ar = v.apply_async(lambda x: x, 'x' * 1024)
        ar.get()
        self._wait_for_idle()
        ars = [ar]

        for i in range(10):
            ar = ars[-1]
            ar2 = self.client.resubmit(ar.msg_ids)

        [ar.get() for ar in ars]

    def test_resubmit_header(self):
        """resubmit shouldn't clobber the whole header"""

        def f():
            import random

            return random.random()

        v = self.client.load_balanced_view()
        v.retries = 1
        ar = v.apply_async(f)
        r1 = ar.get(1)
        # give the Hub a chance to notice:
        self._wait_for_idle()
        ahr = self.client.resubmit(ar.msg_ids)
        ahr.get(1)
        time.sleep(0.5)
        records = self.client.db_query(
            {'msg_id': {'$in': ar.msg_ids + ahr.msg_ids}}, keys='header'
        )
        h1, h2 = (r['header'] for r in records)
        for key in set(h1.keys()).union(set(h2.keys())):
            if key in ('msg_id', 'date'):
                assert h1[key] != h2[key]
            else:
                assert h1[key] == h2[key]

    def test_resubmit_aborted(self):
        def f():
            import random

            return random.random()

        v = self.client.load_balanced_view()
        # restrict to one engine, so we can put a sleep
        # ahead of the task, so it will get aborted
        eid = self.client.ids[-1]
        v.targets = [eid]
        sleep = v.apply_async(time.sleep, 0.5)
        ar = v.apply_async(f)
        ar.abort()
        with pytest.raises(error.TaskAborted):
            ar.get()
        # Give the Hub a chance to get up to date:
        self._wait_for_idle()
        ahr = self.client.resubmit(ar.msg_ids)
        r2 = ahr.get(1)

    def test_resubmit_inflight(self):
        """resubmit of inflight task"""
        v = self.client.load_balanced_view()
        ar = v.apply_async(time.sleep, 1)
        # give the message a chance to arrive
        time.sleep(0.2)
        ahr = self.client.resubmit(ar.msg_ids)
        ar.get(2)
        ahr.get(2)

    def test_resubmit_badkey(self):
        """ensure KeyError on resubmit of nonexistant task"""
        with raises_remote(KeyError):
            self.client.resubmit(['invalid'])

    def test_purge_hub_results(self):
        # ensure there are some tasks
        for i in range(5):
            self.client[:].apply_sync(lambda: 1)
        # Wait for the Hub to realise the result is done:
        # This prevents a race condition, where we
        # might purge a result the Hub still thinks is pending.
        self._wait_for_idle()
        rc2 = clientmod.Client(profile='iptest')
        hist = self.client.hub_history()
        ahr = rc2.get_result([hist[-1]])
        ahr.wait(10)
        self.client.purge_hub_results(hist[-1])
        newhist = self.client.hub_history()
        assert len(newhist) + 1 == len(hist)
        rc2.close()

    def test_purge_local_results(self):
        # ensure there are some tasks
        # purge local results is mostly unnecessary now that we have Futures
        msg_id = 'asdf'
        self.client.results[msg_id] = 5
        md = self.client.metadata[msg_id]
        before = len(self.client.results)
        assert len(self.client.metadata) == before
        self.client.purge_local_results(msg_id)
        assert len(self.client.results) <= before - 1, "Not removed from results"
        assert len(self.client.metadata) <= before - 1, "Not removed from metadata"

    def test_purge_local_results_outstanding(self):
        v = self.client[-1]
        ar = v.apply_async(time.sleep, 1)
        with pytest.raises(RuntimeError):
            self.client.purge_local_results(ar)
        ar.get()
        self.client.purge_local_results(ar)

    def test_purge_all_local_results_outstanding(self):
        v = self.client[-1]
        ar = v.apply_async(time.sleep, 1)
        with pytest.raises(RuntimeError):
            self.client.purge_local_results('all')
        ar.get()
        self.client.purge_local_results('all')

    def test_purge_all_hub_results(self):
        self.client.purge_hub_results('all')
        hist = self.client.hub_history()
        assert len(hist) == 0

    def test_purge_all_local_results(self):
        self.client.purge_local_results('all')
        assert len(self.client.results) == 0, "Results not empty"
        assert len(self.client.metadata) == 0, "metadata not empty"

    def test_purge_all_results(self):
        # ensure there are some tasks
        for i in range(5):
            self.client[:].apply_sync(lambda: 1)
        assert self.client.wait(timeout=10)
        self._wait_for_idle()
        self.client.purge_results('all')
        assert len(self.client.results) == 0, "Results not empty"
        assert len(self.client.metadata) == 0, "metadata not empty"
        hist = self.client.hub_history()
        assert len(hist) == 0, "hub history not empty"

    def test_purge_everything(self):
        # ensure there are some tasks
        for i in range(5):
            self.client[:].apply_sync(lambda: 1)
        self.client.wait(timeout=10)
        self._wait_for_idle()
        self.client.purge_everything()
        # The client results
        assert len(self.client.results) == 0, "Results not empty"
        assert len(self.client.metadata) == 0, "metadata not empty"
        # The client "bookkeeping"
        assert len(self.client.session.digest_history) == 0, "session digest not empty"
        assert len(self.client.history) == 0, "client history not empty"
        # the hub results
        hist = self.client.hub_history()
        assert len(hist) == 0, "hub history not empty"

    def test_activate_on_init(self):
        ip = get_ipython()
        magics = ip.magics_manager.magics
        c = self.connect_client()
        assert 'px' in magics['line']
        assert 'px' in magics['cell']
        c.close()

    def test_activate(self):
        ip = get_ipython()
        magics = ip.magics_manager.magics
        v0 = self.client.activate(-1, '0')
        assert 'px0' in magics['line']
        assert 'px0' in magics['cell']
        assert v0.targets == self.client.ids[-1]
        v0 = self.client.activate('all', 'all')
        assert 'pxall' in magics['line']
        assert 'pxall' in magics['cell']
        assert v0.targets == 'all'

    def test_wait_interactive(self):
        ar = self.client[-1].apply_async(lambda: 1)
        self.client.wait_interactive()
        assert self.client.outstanding == set()

    def test_await_future(self):
        f = Future()

        def finish_later():
            time.sleep(0.1)
            f.set_result('future')

        Thread(target=finish_later).start()
        assert self.client.wait([f])
        assert f.done()
        assert f.result() == 'future'

    @skip_without('distributed')
    @pytest.mark.skipif(
        sys.version_info[:2] <= (3, 5), reason="become_dask doesn't work on Python 3.5"
    )
    @pytest.mark.skipif(
        tornado.version_info[:2] < (5,),
        reason="become_dask doesn't work with tornado 4",
    )
    @pytest.mark.filterwarnings("ignore:make_current")
    def test_become_dask(self):
        executor = self.client.become_dask()
        reprs = self.client[:].apply_sync(repr, Reference('distributed_worker'))
        for r in reprs:
            assert "Worker" in r

        squares = executor.map(lambda x: x * x, range(10))
        tot = executor.submit(sum, squares)
        assert tot.result() == 285

        # cleanup
        executor.close()
        self.client.stop_dask()
        ar = self.client[:].apply_async(lambda x: x, Reference('distributed_worker'))
        with raises_remote(NameError):
            ar.get()

    def test_warning_on_hostname_match(self):
        location = socket.gethostname()
        with mock.patch('ipyparallel.client.client.is_local_ip', lambda x: False):
            with mock.patch('socket.gethostname', lambda: location[0:-1]):
                with pytest.warns(RuntimeWarning):  # should trigger warning
                    c = self.connect_client()
                c.close()
            with mock.patch('socket.gethostname', lambda: location):
                c = None
                try:
                    with warnings.catch_warnings():
                        warnings.simplefilter("error", category=RuntimeWarning)
                        c = self.connect_client()
                finally:
                    if c:
                        c.close()

    def test_local_ip_true_doesnt_trigger_warning(self):
        with mock.patch('ipyparallel.client.client.is_local_ip', lambda x: True):
            c = None
            try:
                with warnings.catch_warnings():
                    warnings.simplefilter("error", category=RuntimeWarning)
                    c = self.connect_client()
            finally:
                if c:
                    c.close()

    def test_wait_for_engines(self):
        n = len(self.client)
        assert self.client.wait_for_engines(n) is None

        f = self.client.wait_for_engines(n, block=False)
        assert isinstance(f, Future)
        assert f.done()

        with pytest.raises(TimeoutError):
            self.client.wait_for_engines(n + 1, timeout=0.1)

        f = self.client.wait_for_engines(n + 1, timeout=10, block=False)
        self.add_engines(1)
        assert f.result() is None

    @pytest.mark.skipif(
        sys.platform.startswith("win"), reason="Signal tests don't pass on Windows yet"
    )
    def test_signal_engines(self):
        view = self.client[:]
        if sys.platform.startswith("win"):
            signame = 'CTRL_C_EVENT'
        else:
            signame = 'SIGINT'
        signum = getattr(signal, signame)
        for sig in (signum, signame):
            ar = view.apply_async(time.sleep, 10)
            # FIXME: use status:busy to wait for tasks to start
            time.sleep(1)
            self.client.send_signal(sig, block=True)
            with raises_remote(KeyboardInterrupt):
                ar.get()

            # make sure they were all interrupted
            for r in ar.get(return_exceptions=True):
                assert isinstance(r, error.RemoteError)
