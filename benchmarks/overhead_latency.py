import timeit
import ipyparallel as ipp
from benchmarks.utils import wait_for


def echo(delay=0):
    def inner_echo(x):
        import time

        if delay:
            time.sleep(delay)
        return x

    return inner_echo


class OverheadLatencySuite:
    client, lview, params = None, None, [[0]]
    param_names = ['number of tasks', 'delay for echo']
    timer = timeit.default_timer
    timeout = 120

    def setup(self, *_):
        n = self.params[0][0]
        self.client = ipp.Client(profile='asv')
        wait_for(lambda: len(self.client) >= n)
        self.lview = self.client.load_balanced_view(targets=slice(n))

    def teardown(self, *_):
        if self.client:
            self.client.close()


def timing_decorator(cls):
    setattr(
        cls,
        'time_n_tasks',
        lambda self, tasks, delay: self.lview.map_sync(echo(delay), [None] * tasks),
    )
    return cls


@timing_decorator
class Engines1(OverheadLatencySuite):
    params = [[1, 10], [0, 0.1, 1]]


@timing_decorator
class Engines10(OverheadLatencySuite):
    params = [[10, 100], [0, 0.1, 1]]


@timing_decorator
class Engines100(OverheadLatencySuite):
    params = [[100, 1000], [0, 0.1, 1]]


@timing_decorator
class Engines100NoDelay(OverheadLatencySuite):
    params = [[100, 1000, 10000, 100000], [0]]
