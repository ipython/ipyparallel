"""test LoadBalancedView objects"""

import time
from itertools import count

import pytest

import ipyparallel as ipp
from ipyparallel import error

from .clienttest import ClusterTestCase, crash, raises_remote


class TestLoadBalancedView(ClusterTestCase):
    def setup_method(self):
        super().setup_method()
        self.view = self.client.load_balanced_view()

    def test_z_crash(self):
        """test graceful handling of engine death (balanced)"""
        self.add_engines(1)
        ar = self.view.apply_async(crash)
        with raises_remote(error.EngineError):
            ar.get(10)
        eid = ar.engine_id
        tic = time.time()
        while eid in self.client.ids and time.time() - tic < 5:
            time.sleep(0.01)
        assert eid not in self.client.ids

    def test_map(self):
        def f(x):
            return x**2

        data = list(range(16))
        ar = self.view.map_async(f, data)
        assert len(ar) == len(data)
        r = ar.get()
        assert r == list(map(f, data))

    def test_map_generator(self):
        def f(x):
            return x**2

        data = list(range(16))
        ar = self.view.map_async(f, iter(data))
        r = ar.get()
        assert r == list(map(f, iter(data)))

    def test_map_short_first(self):
        def f(x, y):
            if y is None:
                return y
            if x is None:
                return x
            return x * y

        data = list(range(10))
        data2 = list(range(4))

        ar = self.view.map_async(f, data, data2)
        assert len(ar) == len(data2)
        r = ar.get()
        assert r == list(map(f, data, data2))

    def test_map_short_last(self):
        def f(x, y):
            if y is None:
                return y
            if x is None:
                return x
            return x * y

        data = list(range(4))
        data2 = list(range(10))

        ar = self.view.map_async(f, data, data2)
        assert len(ar) == len(data)
        r = ar.get()
        assert r == list(map(f, data, data2))

    def test_map_unordered(self):
        def f(x):
            return x**2

        def slow_f(x):
            import time

            time.sleep(0.05 * x)
            return x**2

        data = list(range(16, 0, -1))
        reference = list(map(f, data))

        amr = self.view.map_async(slow_f, data, ordered=False)
        assert isinstance(amr, ipp.AsyncMapResult)
        # check individual elements, retrieved as they come
        # list comprehension uses __iter__
        astheycame = [r for r in amr]
        # Ensure that at least one result came out of order:
        assert astheycame, reference != "should not have preserved order"
        assert sorted(astheycame, reverse=True) == reference, "result corrupted"

    def test_map_ordered(self):
        def f(x):
            return x**2

        def slow_f(x):
            import time

            time.sleep(0.05 * x)
            return x**2

        data = list(range(16, 0, -1))
        reference = list(map(f, data))

        amr = self.view.map_async(slow_f, data)
        assert isinstance(amr, ipp.AsyncMapResult)
        # check individual elements, retrieved as they come
        # list(amr) uses __iter__
        astheycame = list(amr)
        # Ensure that results came in order
        assert astheycame == reference
        assert amr.get() == reference

    def test_map_iterable(self):
        """test map on iterables (balanced)"""
        view = self.view
        # 101 is prime, so it won't be evenly distributed
        arr = range(101)
        # so that it will be an iterator, even in Python 3
        it = iter(arr)
        r = view.map_sync(lambda x: x, arr)
        assert r == list(arr)

    def test_imap_max_outstanding(self):
        view = self.view

        source = count()

        def task(i):
            import time

            time.sleep(0.1)
            return i

        gen = view.imap(task, source, max_outstanding=5)
        # should submit at least max_outstanding
        first_result = next(gen)
        assert len(view.history) >= 5
        # retrieving results should submit another task
        second_result = next(gen)
        assert 5 <= len(view.history) <= 10
        self.client.wait(timeout=self.timeout)

    def test_imap_infinite(self):
        view = self.view

        source = count()

        def task(i):
            import time

            time.sleep(0.1)
            return i

        gen = view.imap(task, source, max_outstanding=2)
        results = []
        for i in gen:
            results.append(i)
            if i >= 3:
                break
        # stop consuming the iterator
        gen.cancel()

        assert len(results) == 4

        # wait
        self.client.wait(timeout=self.timeout)
        # verify that max_outstanding wasn't exceeded
        assert 4 <= len(self.view.history) < 10

    def test_imap_unordered(self):
        self.minimum_engines(4)
        view = self.view

        source = count()

        def yield_up_and_down(n):
            for i in range(n):
                if i % 4 == 0:
                    yield 1 + i / 100
                else:
                    yield i / 100

        def task(t):
            import time

            time.sleep(t)
            return t

        gen = view.imap(task, yield_up_and_down(10), max_outstanding=2, ordered=False)
        results = []
        for i, t in enumerate(gen):
            results.append(t)
            if i >= 2:
                break

        # stop consuming
        gen.cancel()

        assert len(results) == 3
        print(results)
        assert all([r < 1 for r in results])

        # wait
        self.client.wait(timeout=self.timeout)
        # verify that max_outstanding wasn't exceeded
        assert 4 <= len(self.view.history) <= 6

    def test_imap_return_exceptions(self):
        view = self.view

        source = count()

        def fail_on_even(n):
            if n % 2 == 0:
                raise ValueError("even!")
            return n

        gen = view.imap(fail_on_even, range(5), return_exceptions=True)
        for i, r in enumerate(gen):
            if i % 2 == 0:
                assert isinstance(r, error.RemoteError)
            else:
                assert r == i

    def test_abort(self):
        view = self.view
        ar = self.client[:].apply_async(time.sleep, 0.5)
        ar = self.client[:].apply_async(time.sleep, 0.5)
        time.sleep(0.2)
        ar2 = view.apply_async(lambda: 2)
        ar3 = view.apply_async(lambda: 3)
        view.abort(ar2)
        view.abort(ar3.msg_ids)
        with pytest.raises(error.TaskAborted):
            ar2.get()
        with pytest.raises(error.TaskAborted):
            ar3.get()

    def test_retries(self):
        self.minimum_engines(3)
        view = self.view

        def fail():
            raise ValueError("Failed!")

        for r in range(len(self.client) - 1):
            with view.temp_flags(retries=r):
                with raises_remote(ValueError):
                    view.apply_sync(fail)

        with view.temp_flags(retries=len(self.client), timeout=0.1):
            with raises_remote(error.TaskTimeout):
                view.apply_sync(fail)

    def test_short_timeout(self):
        self.minimum_engines(2)
        view = self.view

        def fail():
            import time

            time.sleep(0.25)
            raise ValueError("Failed!")

        with view.temp_flags(retries=1, timeout=0.01):
            with raises_remote(ValueError):
                view.apply_sync(fail)

    def test_invalid_dependency(self):
        view = self.view
        with view.temp_flags(after='12345'):
            with raises_remote(error.InvalidDependency):
                view.apply_sync(lambda: 1)

    def test_impossible_dependency(self):
        self.minimum_engines(2)
        view = self.client.load_balanced_view()
        ar1 = view.apply_async(lambda: 1)
        ar1.get()
        e1 = ar1.engine_id
        e2 = e1
        while e2 == e1:
            ar2 = view.apply_async(lambda: 1)
            ar2.get()
            e2 = ar2.engine_id

        with view.temp_flags(follow=[ar1, ar2]):
            with raises_remote(error.ImpossibleDependency):
                view.apply_sync(lambda: 1)

    def test_follow(self):
        ar = self.view.apply_async(lambda: 1)
        ar.get()
        ars = []
        first_id = ar.engine_id

        self.view.follow = ar
        for i in range(5):
            ars.append(self.view.apply_async(lambda: 1))
        self.view.wait(ars)
        for ar in ars:
            assert ar.engine_id == first_id

    def test_after(self):
        view = self.view
        ar = view.apply_async(time.sleep, 0.5)
        with view.temp_flags(after=ar):
            ar2 = view.apply_async(lambda: 1)

        ar.wait()
        ar2.wait()
        assert ar2.started >= ar.completed
