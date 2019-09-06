import ipyparallel as ipp
import timeit
from benchmarks.utils import wait_for
import numpy as np


class ThroughputSuite:
    client = None
    param_names = ["Number of engines", "Number of bytes"]
    timer = timeit.default_timer
    timeout = 200

    def setup(self, n=10, *_):
        self.client = ipp.Client(profile="asv")
        wait_for(lambda: len(self.client) >= n)

    def teardown(self, *_):
        if self.client:
            self.client.close()


class NumpyArrayBroadcast(ThroughputSuite):
    params = [[1, 10, 50, 100], [10, 1000, 10_000, 100_000, 1_000_000, 10_000_000]]

    def time_broadcast(self, engines, numBytes):
        self.client[:engines]["x"] = np.array([0] * numBytes, dtype=np.int8)
