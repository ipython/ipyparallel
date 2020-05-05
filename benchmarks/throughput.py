import ipyparallel as ipp
import timeit
from benchmarks.utils import wait_for, echo
import numpy as np


class ThroughputSuite:
    client = None
    param_names = ['delay', 'Number of engines', 'Number of bytes']
    timer = timeit.default_timer
    timeout = 200

    def setup(self, n=200, *_):
        wait_for(lambda: len(self.client) >= n)

    # def teardown(self, *_):
    #     if self.client:
    #         self.client.close()
    #

#
# def timing_decorator(cls):
#     setattr(
#         cls,
#         'time_broadcast',
#         lambda self, delay, engines, number_of_bytes: self.view.apply_sync(
#             echo(delay),
#             np.array([0] * number_of_bytes, dtype=np.int8),
#             targets=slice(engines),
#         ),
#     )


def make_benchmark(get_view):
    # @timing_decorator
    class Benchmark(ThroughputSuite):
        params = [
            [0, .1],
            [1, 10, 50, 100, 200],
            [10, 100, 1000, 10_000, 100_000, 1_000_000, 10_000_000],
        ]

        def __init__(self):
            super().__init__()
            self.client = ipp.Client(profile='asv')
            self.view = get_view(self)

        def time_broadcast(self, delay, engines, number_of_bytes):
            self.view.apply_sync(
                echo(delay),
                np.array([0] * number_of_bytes, dtype=np.int8),
                targets=slice(engines),
            )

    return Benchmark


class DirectViewBroadCast(
    make_benchmark(lambda benchmark: benchmark.client.direct_view())
):
    pass


class CoalescingBroadcast(
    make_benchmark(
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=True)
    )
):
    pass


class NonCoalescingBroadcast(
    make_benchmark(
        lambda benchmark: benchmark.client.broadcast_view(is_coalescing=False)
    )
):
    pass


class SpanningTreeBroadCast(
    make_benchmark(lambda benchmark: benchmark.client.spanning_tree_view())
):
    pass
