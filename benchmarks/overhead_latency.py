import timeit
import ipyparallel as ipp
import numpy as np
from benchmarks.utils import wait_for, echo, echo_many_arguments


class OverheadLatencySuite:
    client, lview, params = None, None, [[0]]
    param_names = ["number of tasks", "delay for echo"]
    timer = timeit.default_timer
    timeout = 200

    def __init__(self, n):
        self.n = n

    def setup(self, *_):
        self.client = ipp.Client(profile="asv")
        wait_for(lambda: len(self.client) >= self.n)
        self.lview = self.client.load_balanced_view(targets=slice(self.n))

    def teardown(self, *_):
        if self.client:
            self.client.close()


def timing_decorator(cls):
    setattr(
        cls,
        "time_n_tasks",
        lambda self, tasks, delay: self.lview.map_sync(echo(delay), [None] * tasks),
    )
    return cls


@timing_decorator
class Engines1(OverheadLatencySuite):
    def __init__(self):
        super().__init__(1)

    params = [[1, 10, 100], [0, 0.1, 1]]


@timing_decorator
class Engines10(OverheadLatencySuite):
    def __init__(self):
        super().__init__(10)

    params = [[1, 10, 100], [0, 0.1, 1]]


@timing_decorator
class Engines100(OverheadLatencySuite):
    def __init__(self):
        super().__init__(100)

    params = [[1, 10, 100, 1000], [0, 0.1, 1]]


class Engines100NoDelay(OverheadLatencySuite):
    def __init__(self):
        super().__init__(100)

    params = [[1, 10, 100, 1000, 10000], [0]]

    def time_n_tasks(self, tasks, _):
        self.lview.map_sync(echo(0), [None] * tasks)

    def time_n_task_non_blocking(self, tasks, _):
        self.lview.map(echo(0), [None] * tasks, block=False)


class EchoManyArguments(OverheadLatencySuite):
    NUMBER_OF_ENGINES = 16
    params = [2, 4, 8, 16, 32, 64, 128, 255]
    param_names = ['Number of arguments']

    def __init__(self):
        super().__init__(EchoManyArguments.NUMBER_OF_ENGINES)

    def time_echo_with_many_arguments(self, number_of_arguments):
        self.lview.map(
            lambda x: echo_many_arguments(*x),
            [
                (np.empty(1, dtype=np.int8) for n in range(number_of_arguments))
                for x in range(self.n)
            ]
        )
