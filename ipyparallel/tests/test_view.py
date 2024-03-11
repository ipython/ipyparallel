"""test View objects"""

# Copyright (c) IPython Development Team.
# Distributed under the terms of the Modified BSD License.
import base64
import platform
import sys
import time
from collections import namedtuple
from tempfile import NamedTemporaryFile

import pytest
import zmq
from IPython import get_ipython
from IPython.utils.io import capture_output

import ipyparallel as ipp
from ipyparallel import AsyncHubResult, AsyncMapResult, AsyncResult, error
from ipyparallel.util import interactive

from .clienttest import (
    ClusterTestCase,
    conditional_crash,
    raises_remote,
    skip_without,
    wait,
)

point = namedtuple("point", "x y")


@pytest.mark.usefixtures('ipython')
class TestView(ClusterTestCase):
    def setup_method(self):
        # On Win XP, wait for resource cleanup, else parallel test group fails
        if platform.system() == "Windows" and platform.win32_ver()[0] == "XP":
            # 1 sec fails. 1.5 sec seems ok. Using 2 sec for margin of safety
            time.sleep(2)
        super().setup_method()

    def test_z_crash_mux(self):
        """test graceful handling of engine death (direct)"""
        self.add_engines(1)
        self.minimum_engines(2)
        eid = self.client.ids[-1]
        view = self.client[-2:]
        view.scatter('should_crash', [False, True], flatten=True)
        ar = view.apply_async(conditional_crash, ipp.Reference("should_crash"))
        with raises_remote(error.EngineError):
            ar.get(10)
        tic = time.perf_counter()
        while eid in self.client.ids and time.perf_counter() - tic < 5:
            time.sleep(0.05)
        assert eid not in self.client.ids

    def test_push_pull(self):
        """test pushing and pulling"""
        data = dict(a=10, b=1.05, c=list(range(10)), d={'e': (1, 2), 'f': 'hi'})
        t = self.client.ids[-1]
        v = self.client[t]
        push = v.push
        pull = v.pull
        v.block = True
        nengines = len(self.client)
        push({'data': data})
        d = pull('data')
        assert d == data
        self.client[:].push({'data': data})
        d = self.client[:].pull('data', block=True)
        assert d == nengines * [data]
        ar = push({'data': data}, block=False)
        assert isinstance(ar, AsyncResult)
        r = ar.get()
        ar = self.client[:].pull('data', block=False)
        assert isinstance(ar, AsyncResult)
        r = ar.get()
        assert r == nengines * [data]
        self.client[:].push(dict(a=10, b=20))
        r = self.client[:].pull(('a', 'b'), block=True)
        assert r, nengines * [[10 == 20]]

    def test_push_pull_function(self):
        "test pushing and pulling functions"

        def testf(x):
            return 2.0 * x

        t = self.client.ids[-1]
        v = self.client[t]
        v.block = True
        push = v.push
        pull = v.pull
        execute = v.execute
        push({'testf': testf})
        r = pull('testf')
        assert r(1.0) == testf(1.0)
        execute('r = testf(10)')
        r = pull('r')
        assert r == testf(10)
        ar = self.client[:].push({'testf': testf}, block=False)
        ar.get()
        ar = self.client[:].pull('testf', block=False)
        rlist = ar.get()
        for r in rlist:
            assert r(1.0) == testf(1.0)
        execute("def g(x): return x*x")
        r = pull(('testf', 'g'))
        assert (r[0](10), r[1](10)) == (testf(10), 100)

    def test_push_function_globals(self):
        """test that pushed functions have access to globals"""

        @interactive
        def geta():
            return a  # noqa: F821

        # self.add_engines(1)
        v = self.client[-1]
        v.block = True
        v['f'] = geta
        with raises_remote(NameError):
            v.execute('b=f()')
        v.execute('a=5')
        v.execute('b=f()')
        assert v['b'] == 5

    def test_push_function_defaults(self):
        """test that pushed functions preserve default args"""

        def echo(a=10):
            return a

        v = self.client[-1]
        v.block = True
        v['f'] = echo
        v.execute('b=f()')
        assert v['b'] == 10

    def test_get_result(self):
        """test getting results from the Hub."""
        c = ipp.Client(profile='iptest')
        # self.add_engines(1)
        t = c.ids[-1]
        v = c[t]
        v2 = self.client[t]
        ar = v.apply_async(wait, 1)
        # give the monitor time to notice the message
        time.sleep(0.25)
        ahr = v2.get_result(ar.msg_ids[0], owner=False)
        assert isinstance(ahr, AsyncHubResult)
        assert ahr.get() == ar.get()
        ar2 = v2.get_result(ar.msg_ids[0])
        assert not isinstance(ar2, AsyncHubResult)
        assert ahr.get() == ar2.get()
        c.close()

    def test_run_newline(self):
        """test that run appends newline to files"""
        with NamedTemporaryFile('w', delete=False) as f:
            f.write(
                """def g():
                return 5
                """
            )
        v = self.client[-1]
        v.run(f.name, block=True)
        assert v.apply_sync(lambda f: f(), ipp.Reference('g')) == 5

    def test_apply_f_kwarg(self):
        v = self.client[-1]

        def echo_kwargs(**kwargs):
            return kwargs

        kwargs = v.apply_async(echo_kwargs, f=5).get(timeout=30)
        assert kwargs == dict(f=5)

    def test_apply_tracked(self):
        """test tracking for apply"""
        # self.add_engines(1)
        t = self.client.ids[-1]
        v = self.client[t]
        v.block = False

        def echo(n=1024 * 1024, **kwargs):
            with v.temp_flags(**kwargs):
                return v.apply(lambda x: x, 'x' * n)

        ar = echo(1, track=False)
        ar.wait_for_send(5)
        assert isinstance(ar._tracker, zmq.MessageTracker)
        assert ar.sent
        ar = echo(track=True)
        ar.wait_for_send(5)
        assert isinstance(ar._tracker, zmq.MessageTracker)
        assert ar.sent == ar._tracker.done
        ar._tracker.wait()
        assert ar.sent

    def test_push_tracked(self):
        t = self.client.ids[-1]
        ns = dict(x='x' * 1024 * 1024)
        v = self.client[t]
        ar = v.push(ns, block=False, track=False)
        assert ar.sent

        ar = v.push(ns, block=False, track=True)
        ar.wait_for_send()
        assert ar.sent == ar._tracker.done
        assert ar.sent
        ar.get()

    def test_scatter_tracked(self):
        t = self.client.ids
        x = 'x' * 1024 * 1024
        ar = self.client[t].scatter('x', x, block=False, track=False)
        assert ar.sent

        ar = self.client[t].scatter('x', x, block=False, track=True)
        ar._sent_event.wait()
        assert isinstance(ar._tracker, zmq.MessageTracker)
        assert ar.sent == ar._tracker.done
        ar.wait_for_send()
        assert ar.sent
        ar.get()

    def test_remote_reference(self):
        v = self.client[-1]
        v['a'] = 123
        ra = ipp.Reference('a')
        b = v.apply_sync(lambda x: x, ra)
        assert b == 123

    def test_scatter_gather(self):
        view = self.client[:]
        seq1 = list(range(16))
        view.scatter('a', seq1)
        seq2 = view.gather('a', block=True)
        assert seq2 == seq1
        with raises_remote(NameError):
            view.gather('asdf', block=True)

    @skip_without('numpy')
    def test_scatter_gather_numpy(self):
        import numpy
        from numpy.testing import assert_array_equal

        view = self.client[:]
        a = numpy.arange(64)
        view.scatter('a', a, block=True)
        b = view.gather('a', block=True)
        assert_array_equal(b, a)

    def test_scatter_gather_lazy(self):
        """scatter/gather with targets='all'"""
        view = self.client.direct_view(targets='all')
        x = list(range(64))
        view.scatter('x', x)
        gathered = view.gather('x', block=True)
        assert gathered == x

    @skip_without('numpy')
    def test_apply_numpy(self):
        """view.apply(f, ndarray)"""
        import numpy
        from numpy.testing import assert_array_equal

        A = numpy.random.random((100, 100))
        view = self.client[-1]
        for dt in ['int32', 'uint8', 'float32', 'float64']:
            B = A.astype(dt)
            C = view.apply_sync(lambda x: x, B)
            assert_array_equal(B, C)

    @skip_without('numpy')
    def test_apply_numpy_object_dtype(self):
        """view.apply(f, ndarray) with dtype=object"""
        import numpy
        from numpy.testing import assert_array_equal

        view = self.client[-1]

        A = numpy.array([dict(a=5)])
        B = view.apply_sync(lambda x: x, A)
        assert_array_equal(A, B)

        A = numpy.array([(0, dict(b=10))], dtype=[('i', int), ('o', object)])
        B = view.apply_sync(lambda x: x, A)
        assert_array_equal(A, B)

    @skip_without('numpy')
    def test_push_pull_recarray(self):
        """push/pull recarrays"""
        import numpy
        from numpy.testing import assert_array_equal

        view = self.client[-1]

        R = numpy.array(
            [
                (1, 'hi', 0.0),
                (2**30, 'there', 2.5),
                (-99999, 'world', -12345.6789),
            ],
            [('n', int), ('s', '|S10'), ('f', float)],
        )

        view['RR'] = R
        R2 = view['RR']

        r_dtype, r_shape = view.apply_sync(
            interactive(lambda: (RR.dtype, RR.shape))  # noqa: F821
        )
        assert r_dtype == R.dtype
        assert r_shape == R.shape
        assert R2.dtype == R.dtype
        assert R2.shape == R.shape
        assert_array_equal(R2, R)

    @skip_without('pandas')
    def test_push_pull_timeseries(self):
        """push/pull pandas.Series"""
        import pandas

        ts = pandas.Series(list(range(10)))

        view = self.client[-1]

        view.push(dict(ts=ts), block=True)
        rts = view['ts']

        assert type(rts) == type(ts)
        assert (ts == rts).all()

    def test_map(self):
        view = self.client[:]

        def f(x):
            return x**2

        data = list(range(16))
        ar = view.map_async(f, data)
        assert len(ar) == len(data)
        r = ar.get()
        assert r, list(map(f == data))

    def test_map_empty_sequence(self):
        view = self.client[:]
        r = view.map_sync(lambda x: x, [])
        assert r == []

    def test_map_iterable(self):
        """test map on iterables (direct)"""
        view = self.client[:]
        # 101 is prime, so it won't be evenly distributed
        arr = range(101)
        # ensure it will be an iterator, even in Python 3
        it = iter(arr)
        r = view.map_sync(lambda x: x, it)
        assert r == list(arr)

    @skip_without('numpy')
    def test_map_numpy(self):
        """test map on numpy arrays (direct)"""
        import numpy
        from numpy.testing import assert_array_equal

        view = self.client[:]
        # 101 is prime, so it won't be evenly distributed
        arr = numpy.arange(101)
        ar = view.map_async(lambda x: x, arr)
        assert len(ar) == len(arr)
        r = ar.get()
        assert_array_equal(r, arr)

    def test_scatter_gather_nonblocking(self):
        data = list(range(16))
        view = self.client[:]
        view.scatter('a', data, block=False)
        ar = view.gather('a', block=False)
        assert ar.get() == data

    @skip_without('numpy')
    def test_scatter_gather_numpy_nonblocking(self):
        import numpy
        from numpy.testing import assert_array_equal

        a = numpy.arange(64)
        view = self.client[:]
        ar = view.scatter('a', a, block=False)
        assert isinstance(ar, AsyncResult)
        amr = view.gather('a', block=False)
        assert isinstance(amr, AsyncMapResult)
        assert_array_equal(amr.get(), a)

    def test_execute(self):
        view = self.client[:]
        # self.client.debug=True
        execute = view.execute
        ar = execute('c=30', block=False)
        assert isinstance(ar, AsyncResult)
        ar = execute('d=[0,1,2]', block=False)
        self.client.wait(ar, 1)
        assert len(ar.get()) == len(self.client)
        for c in view['c']:
            assert c == 30

    def test_abort(self):
        view = self.client[-1]
        ar = view.execute('import time; time.sleep(1)', block=False)
        ar2 = view.apply_async(lambda: 2)
        view.abort(ar2)
        ar3 = view.apply_async(lambda: 3)
        view.abort(ar3.msg_ids)
        with pytest.raises(error.TaskAborted):
            ar2.get()
        with pytest.raises(error.TaskAborted):
            ar3.get()

    def test_abort_all(self):
        """view.abort() aborts all outstanding tasks"""
        view = self.client[-1]
        ars = [view.apply_async(time.sleep, 0.25) for i in range(10)]
        view.abort()
        view.wait(timeout=5)
        for ar in ars[5:]:
            with pytest.raises(error.TaskAborted):
                ar.get()

    def test_temp_flags(self):
        view = self.client[-1]
        view.block = True
        with view.temp_flags(block=False):
            assert not view.block
        assert view.block

    def test_importer(self):
        view = self.client[-1]
        view.clear(block=True)
        with view.importer:
            import re  # noqa: F401

        @interactive
        def findall(pat, s):
            # this globals() step isn't necessary in real code
            # only to prevent a closure in the test
            re = globals()['re']  # noqa: F811
            return re.findall(pat, s)

        assert view.apply_sync(findall, r'\w+', 'hello world') == 'hello world'.split()

    def test_unicode_execute(self):
        """test executing unicode strings"""
        v = self.client[-1]
        v.block = True
        if sys.version_info[0] >= 3:
            code = "a='é'"
        else:
            code = "a=u'é'"
        v.execute(code)
        assert v['a'] == 'é'

    def test_unicode_apply_result(self):
        """test unicode apply results"""
        v = self.client[-1]
        r = v.apply_sync(lambda: 'é')
        assert r == 'é'

    def test_unicode_apply_arg(self):
        """test passing unicode arguments to apply"""
        v = self.client[-1]

        @interactive
        def check_unicode(a, check):
            assert not isinstance(a, bytes), "%r is bytes, not unicode" % a
            assert isinstance(check, bytes), "%r is not bytes" % check
            assert a.encode('utf8') == check, f"{a} != {check}"

        for s in ['é', 'ßø®∫', 'asdf']:
            try:
                v.apply_sync(check_unicode, s, s.encode('utf8'))
            except error.RemoteError as e:
                if e.ename == 'AssertionError':
                    self.fail(e.evalue)
                else:
                    raise e

    def test_map_reference(self):
        """view.map(<Reference>, *seqs) should work"""
        v = self.client[:]
        v.scatter('n', self.client.ids, flatten=True)
        v.execute("f = lambda x,y: x*y")
        rf = ipp.Reference('f')
        nlist = list(range(10))
        mlist = nlist[::-1]
        expected = [m * n for m, n in zip(mlist, nlist)]
        result = v.map_sync(rf, mlist, nlist)
        assert result == expected

    def test_apply_reference(self):
        """view.apply(<Reference>, *args) should work"""
        v = self.client[:]
        v.scatter('n', self.client.ids, flatten=True)
        v.execute("f = lambda x: n*x")
        rf = ipp.Reference('f')
        result = v.apply_sync(rf, 5)
        expected = [5 * id for id in self.client.ids]
        assert result == expected

    def test_eval_reference(self):
        v = self.client[self.client.ids[0]]
        v['g'] = list(range(5))
        rg = ipp.Reference('g[0]')

        def echo(x):
            return x

        assert v.apply_sync(echo, rg) == 0

    def test_reference_nameerror(self):
        v = self.client[self.client.ids[0]]
        r = ipp.Reference('elvis_has_left')

        def echo(x):
            return x

        with raises_remote(NameError):
            v.apply_sync(echo, r)

    def test_single_engine_map(self):
        e0 = self.client[self.client.ids[0]]
        r = list(range(5))
        check = [-1 * i for i in r]
        result = e0.map_sync(lambda x: -1 * x, r)
        assert result == check

    def test_len(self):
        """len(view) makes sense"""
        e0 = self.client[self.client.ids[0]]
        assert len(e0) == 1
        v = self.client[:]
        assert len(v) == len(self.client.ids)
        v = self.client.direct_view('all')
        assert len(v) == len(self.client.ids)
        v = self.client[:2]
        assert len(v) == 2
        v = self.client[:1]
        assert len(v) == 1
        v = self.client.load_balanced_view()
        assert len(v) == len(self.client.ids)

    # begin execute tests

    def test_execute_reply(self):
        e0 = self.client[self.client.ids[0]]
        e0.block = True
        ar = e0.execute("5", silent=False)
        er = ar.get()
        assert str(er) == "<ExecuteReply[%i]: 5>" % er.execution_count
        assert er.execute_result['data']['text/plain'] == '5'

    def test_execute_reply_rich(self):
        e0 = self.client[self.client.ids[0]]
        e0.block = True
        e0.execute("from IPython.display import Image, HTML")
        ar = e0.execute("Image(data=b'garbage', format='png', width=10)", silent=False)
        er = ar.get()
        b64data = base64.b64encode(b'garbage').decode('ascii')
        data, metadata = er._repr_png_()
        assert data.strip() == b64data.strip()
        assert metadata == dict(width=10)
        ar = e0.execute("HTML('<b>bold</b>')", silent=False)
        er = ar.get()
        assert er._repr_html_() == "<b>bold</b>"

    def test_execute_reply_stdout(self):
        e0 = self.client[self.client.ids[0]]
        e0.block = True
        ar = e0.execute("print (5)", silent=False)
        er = ar.get()
        assert er.stdout.strip() == '5'

    def test_execute_result(self):
        """execute triggers execute_result with silent=False"""
        view = self.client[:]
        ar = view.execute("5", silent=False, block=True)

        expected = [{'text/plain': '5'}] * len(view)
        mimes = [out['data'] for out in ar.execute_result]
        assert mimes == expected

    def test_execute_silent(self):
        """execute does not trigger execute_result with silent=True"""
        view = self.client[:]
        ar = view.execute("5", block=True)
        expected = [None] * len(view)
        assert ar.execute_result == expected

    def test_execute_magic(self):
        """execute accepts IPython commands"""
        view = self.client[:]
        view.execute("a = 5")
        ar = view.execute("%whos", block=True)
        # this will raise, if that failed
        ar.get(5)
        for stdout in ar.stdout:
            lines = stdout.splitlines()
            assert lines[0].split(), ['Variable', 'Type' == 'Data/Info']
            found = False
            for line in lines[2:]:
                split = line.split()
                if split == ['a', 'int', '5']:
                    found = True
                    break
            assert found, "whos output wrong: %s" % stdout

    def test_execute_displaypub(self):
        """execute tracks display_pub output"""
        view = self.client[:]
        view.execute("from IPython.core.display import *")
        ar = view.execute("[ display(i) for i in range(5) ]", block=True)

        expected = [{'text/plain': str(j)} for j in range(5)]
        for outputs in ar.outputs:
            mimes = [out['data'] for out in outputs]
            assert mimes == expected

    def test_apply_displaypub(self):
        """apply tracks display_pub output"""
        view = self.client[:]
        view.execute("from IPython.core.display import *")

        @interactive
        def publish():
            [display(i) for i in range(5)]  # noqa: F821

        ar = view.apply_async(publish)
        ar.get(5)
        assert ar.wait_for_output(5)
        expected = [{'text/plain': str(j)} for j in range(5)]
        for outputs in ar.outputs:
            mimes = [out['data'] for out in outputs]
            assert mimes == expected

    def test_execute_raises(self):
        """exceptions in execute requests raise appropriately"""
        view = self.client[-1]
        ar = view.execute("1/0")
        with raises_remote(ZeroDivisionError):
            ar.get(2)

    def test_remoteerror_render_exception(self):
        """RemoteErrors get nice tracebacks"""
        view = self.client[-1]
        ar = view.execute("1/0")
        ip = get_ipython()
        ip.user_ns['ar'] = ar
        with capture_output() as io:
            ip.run_cell("ar.get(2)")

        assert 'ZeroDivisionError' in io.stdout, io.stdout

    def test_compositeerror_render_exception(self):
        """CompositeErrors get nice tracebacks"""
        view = self.client[:]
        ar = view.execute("1/0")
        ip = get_ipython()
        ip.user_ns['ar'] = ar

        with capture_output() as io:
            ip.run_cell("ar.get(2)")

        count = min(error.CompositeError.tb_limit, len(view))

        assert io.stdout.count('ZeroDivisionError'), count * 2 == io.stdout
        assert io.stdout.count('by zero'), count == io.stdout
        assert io.stdout.count(':execute'), count == io.stdout

    def test_compositeerror_truncate(self):
        """Truncate CompositeErrors with many exceptions"""
        view = self.client[:]
        requests = []
        for i in range(10):
            requests.append(view.execute("1/0"))

        ar = self.client.get_result(requests)
        try:
            ar.get()
        except error.CompositeError as _e:
            e = _e
        else:
            self.fail("Should have raised CompositeError")

        lines = e.render_traceback()
        with capture_output() as io:
            e.print_traceback()

        assert "more exceptions" in lines[-1]
        count = e.tb_limit

        assert io.stdout.count('ZeroDivisionError'), 2 * count == io.stdout
        assert io.stdout.count('by zero'), count == io.stdout
        assert io.stdout.count(':execute'), count == io.stdout

    def test_magic_pylab(self):
        """%pylab works on engines"""
        pytest.importorskip('matplotlib')
        view = self.client[-1]
        ar = view.execute("%pylab inline")
        # at least check if this raised:
        reply = ar.get(5)
        # include imports, in case user config
        ar = view.execute("plot(rand(100))", silent=False)
        reply = ar.get(5)
        assert ar.wait_for_output(5)
        assert len(reply.outputs) == 1
        output = reply.outputs[0]
        assert "data" in output
        data = output['data']
        assert "image/png" in data

    def test_func_default_func(self):
        """interactively defined function as apply func default"""

        def foo():
            return 'foo'

        def bar(f=foo):
            return f()

        view = self.client[-1]
        ar = view.apply_async(bar)
        r = ar.get(10)
        assert r == 'foo'

    def test_data_pub_single(self):
        view = self.client[-1]
        ar = view.execute(
            '\n'.join(
                [
                    'from ipyparallel.datapub import publish_data',
                    'for i in range(5):',
                    '  publish_data(dict(i=i))',
                ]
            ),
            block=False,
        )
        assert isinstance(ar.data, dict)
        ar.get(5)
        assert ar.wait_for_output(5)
        assert ar.data == dict(i=4)

    def test_data_pub(self):
        view = self.client[:]
        ar = view.execute(
            '\n'.join(
                [
                    'from ipyparallel.datapub import publish_data',
                    'for i in range(5):',
                    '  publish_data(dict(i=i))',
                ]
            ),
            block=False,
        )
        assert all(isinstance(d, dict) for d in ar.data)
        ar.get(5)
        assert ar.wait_for_output(5)
        assert ar.data == [dict(i=4)] * len(ar)

    def test_can_list_arg(self):
        """args in lists are canned"""
        view = self.client[-1]
        view['a'] = 128
        rA = ipp.Reference('a')
        ar = view.apply_async(lambda x: x, [rA])
        r = ar.get(5)
        assert r == [128]

    def test_can_dict_arg(self):
        """args in dicts are canned"""
        view = self.client[-1]
        view['a'] = 128
        rA = ipp.Reference('a')
        ar = view.apply_async(lambda x: x, dict(foo=rA))
        r = ar.get(5)
        assert r == dict(foo=128)

    def test_can_list_kwarg(self):
        """kwargs in lists are canned"""
        view = self.client[-1]
        view['a'] = 128
        rA = ipp.Reference('a')
        ar = view.apply_async(lambda x=5: x, x=[rA])
        r = ar.get(5)
        assert r == [128]

    def test_can_dict_kwarg(self):
        """kwargs in dicts are canned"""
        view = self.client[-1]
        view['a'] = 128
        rA = ipp.Reference('a')
        ar = view.apply_async(lambda x=5: x, dict(foo=rA))
        r = ar.get(5)
        assert r == dict(foo=128)

    def test_map_ref(self):
        """view.map works with references"""
        view = self.client[:]
        ranks = sorted(self.client.ids)
        view.scatter('rank', ranks, flatten=True)
        rrank = ipp.Reference('rank')

        amr = view.map_async(lambda x: x * 2, [rrank] * len(view))
        drank = amr.get(5)
        assert drank == [r * 2 for r in ranks]

    def test_nested_getitem_setitem(self):
        """get and set with view['a.b']"""
        view = self.client[-1]
        view.execute(
            '\n'.join(
                [
                    'class A: pass',
                    'a = A()',
                    'a.b = 128',
                ]
            ),
            block=True,
        )
        ra = ipp.Reference('a')

        r = view.apply_sync(lambda x: x.b, ra)
        assert r == 128
        assert view['a.b'] == 128

        view['a.b'] = 0

        r = view.apply_sync(lambda x: x.b, ra)
        assert r == 0
        assert view['a.b'] == 0

    def test_return_namedtuple(self):
        def namedtuplify(x, y):
            return point(x, y)

        view = self.client[-1]
        p = view.apply_sync(namedtuplify, 1, 2)
        assert p.x == 1
        assert p.y == 2

    def test_apply_namedtuple(self):
        def echoxy(p):
            return p.y, p.x

        view = self.client[-1]
        tup = view.apply_sync(echoxy, point(1, 2))
        assert tup, 2 == 1

    def test_sync_imports(self):
        view = self.client[-1]
        with capture_output() as io:
            with view.sync_imports():
                import IPython  # noqa
        assert "IPython" in io.stdout

        @interactive
        def find_ipython():
            return 'IPython' in globals()

        assert view.apply_sync(find_ipython)

    def test_sync_imports_quiet(self):
        view = self.client[-1]
        with capture_output() as io:
            with view.sync_imports(quiet=True):
                import IPython  # noqa
        assert io.stdout == ''

        @interactive
        def find_ipython():
            return 'IPython' in globals()

        assert view.apply_sync(find_ipython)

    @skip_without('cloudpickle')
    def test_use_cloudpickle(self):
        view = self.client[:]
        view['_a'] = 'engine'
        __main__ = sys.modules['__main__']

        @interactive
        def get_a():
            return _a  # noqa: F821

        # verify initial condition
        a_list = view.apply_sync(get_a)
        assert a_list == ['engine'] * len(view)

        # enable cloudpickle
        view.use_cloudpickle()

        # cloudpickle prefers client values
        __main__._a = 'client'
        a_list = view.apply_sync(get_a)
        assert a_list == ['client'] * len(view)

        # still works even if remote is undefined
        view.execute('del _a', block=True)
        a_list = view.apply_sync(get_a)
        assert a_list == ['client'] * len(view)

        # restore pickle, shouldn't resolve
        view.use_pickle()
        with raises_remote(NameError):
            view.apply_sync(get_a)

    @skip_without('cloudpickle')
    def test_cloudpickle_push_pull(self):
        view = self.client[:]
        # enable cloudpickle
        view.use_cloudpickle()

        # push/pull still work
        view.push({"key": "pushed"}, block=True)
        ar = view.pull("key")
        assert ar.get(timeout=10) == ["pushed"] * len(view)

        # restore pickle, should get the same value
        view.use_pickle()
        ar = view.pull("key")
        assert ar.get(timeout=10) == ["pushed"] * len(view)

    @skip_without('cloudpickle')
    @pytest.mark.xfail(reason="@require doesn't work with cloudpickle")
    def test_cloudpickle_require(self):
        view = self.client[:]
        # enable cloudpickle
        view.use_cloudpickle()
        assert (
            'types' not in globals()
        ), "Test condition isn't met if types is already imported"

        @ipp.require("types")
        @ipp.interactive
        def func(x):
            return types.SimpleNamespace(n=x)  # noqa: F821

        res = view.apply_async(func, 5)
        assert res.get(timeout=10) == []
        view.use_pickle()

    def test_block_kwarg(self):
        """
        Tests that kwargs such as 'block' can be
        specified when creating a view, in this
        case a BroadcastView
        """
        view = self.client.broadcast_view(block=True)
        assert view.block is True
        # Execute something as a sanity check
        view.execute("5", silent=False)
