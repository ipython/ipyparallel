import ipyparallel as ipp
import timeit
from benchmarks.utils import wait_for, echo
import numpy as np

delay = [0]
engines = [1, 10, 50, 100, 200]
byte_param = [10, 100, 1000, 10_000, 100_000, 1_000_000, 2_000_000]

class ThroughputSuite:
    client = None
    param_names = ['delay', 'Number of engines', 'Number of bytes']
    timer = timeit.default_timer
    timeout = 300

    def setup(self, *args):
        self.client = ipp.Client(profile='asv')
        wait_for(lambda: len(self.client) >= max(engines))

    def teardown(self, *_):
        if self.client:
            self.client.close()


def make_benchmark(get_view):
    # @timing_decorator
    class Benchmark(ThroughputSuite):
        params = [
            delay,
            engines,
            byte_param,
        ]

        def __init__(self):
            super().__init__()

        def time_broadcast(self, delay, engines, number_of_bytes):
            self.view = get_view(self)
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

