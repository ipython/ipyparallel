import timeit
import ipyparallel as ipp
import numpy as np
from benchmarks.utils import wait_for, echo, echo_many_arguments


class OverheadLatencyBase:
    client, view, params = None, None, [[0]]
    param_names = ["number of tasks", "delay for echo"]
    timer = timeit.default_timer
    timeout = 200

    def __init__(self, n):
        self.n = n

    def setup(self, *_):
        self.client = ipp.Client(profile="asv")
        wait_for(lambda: len(self.client) >= self.n)

    def teardown(self, *_):
        if self.client:
            self.client.close()


def timing_decorator(cls):
    setattr(
        cls,
        "time_n_tasks",
        lambda self, tasks, delay: self.view.map_sync(echo(delay), [None] * tasks),
    )
    return cls


class OverHeadLatencyLoadBalanced(OverheadLatencyBase):
    def setup(self, *_):
        super().setup()
        self.view = self.client.load_balanced_view(targets=slice(self.n))


class OverHeadLatencyDirectView(OverheadLatencyBase):
    def setup(self, *_):
        super().setup()
        self.view = self.client[: self.n]


def make_engine_class(base_class, n, engine_params):
    @timing_decorator
    class Engine(base_class):
        def __init__(self):
            super().__init__(n)

        params = engine_params

    return Engine


class Engines1DirectView(
    make_engine_class(OverHeadLatencyDirectView, 1, [[1, 10, 100], [0, 0.1, 1]])
):
    pass


class Engines1LoadBalanced(
    make_engine_class(OverHeadLatencyLoadBalanced, 1, [[1, 10, 100], [0, 0.1, 1]])
):
    pass


class Engines10DirectView(
    make_engine_class(OverHeadLatencyDirectView, 10, [[1, 10, 100], [0, 0.1, 1]])
):
    pass


class Engines10LoadBalanced(
    make_engine_class(OverHeadLatencyLoadBalanced, 10, [[1, 10, 100], [0, 0.1, 1]])
):
    pass


# class Engines100DirectView(
#     make_engine_class(OverHeadLatencyDirectView, 100, [[1, 10, 100, 1000], [0, 0.1, 1]])
# ):
#     pass
#
#
# class Engines100LoadBalanced(
#     make_engine_class(
#         OverHeadLatencyLoadBalanced, 100, [[1, 10, 100, 1000], [0, 0.1, 1]]
#     )
# ):
#     pass


def make_no_delay_engine_class(base_class, n, engine_params):
    class Engine(base_class):
        def __init__(self):
            super().__init__(n)

        params = engine_params

        def time_n_tasks(self, tasks, _):
            self.view.map_sync(echo(0), [None] * tasks)

        def time_n_task_non_blocking(self, tasks, _):
            self.view.map(echo(0), [None] * tasks, block=False)

    return Engine


class Engines100NoDelayLoadBalanced(
    make_no_delay_engine_class(
        OverHeadLatencyLoadBalanced, 100, [[1, 10, 100, 1000, 10000], [0]]
    )
):
    pass


class Engines100NoDelayDirectView(
    make_no_delay_engine_class(
        OverHeadLatencyDirectView, 100, [[1, 10, 100, 1000, 10000], [0]]
    )
):
    pass


def create_echo_many_arguments_class(base_class):
    class EchoManyArguments(base_class):
        params = [2, 4, 8, 16, 32, 64, 128, 255]
        param_names = ['Number of arguments']

        def __init__(self):
            super().__init__(16)

        def time_echo_with_many_arguments(self, number_of_arguments):
            self.view.map(
                lambda x: echo_many_arguments(*x),
                [
                    tuple(
                        np.empty(1, dtype=np.int8) for n in range(number_of_arguments)
                    )
                    for x in range(self.n)
                ],
                block=False,
            )

    return EchoManyArguments


class EchoManyArgumentsLoadBalanced(
    create_echo_many_arguments_class(OverHeadLatencyLoadBalanced)
):
    pass


class EchoManyArgumentsDirectView(
    create_echo_many_arguments_class(OverHeadLatencyDirectView)
):
    pass
