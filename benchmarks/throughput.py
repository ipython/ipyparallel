import ipyparallel as ipp
from benchmarks.utils import wait_for
import numpy as np


class ThroughputSuite:
    client = None

    def setup(self, n=10, *_):
        raise NotImplementedError
        self.client = ipp.Client(profile='asv')
        wait_for(lambda: len(self.client) >= n)
        self.lview = self.client.load_balanced_view(targets=slice(n))

    def teardown(self):
        if self.client:
            self.client.close()


class NumpyArrayBroadcast(ThroughputSuite):
    params = [[1, 10, 50, 100], [10, 1000, 10_000, 100_1000, 1000_000]]

    def time_broadcast(self, _, numBytes):
        self.lview[:]['x'] = np.array([0] * numBytes, dtype=np.int8)
