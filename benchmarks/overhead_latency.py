import atexit
import timeit
from typing import Callable
import time
import ipyparallel as ipp
import os
from benchmarks.utils import Controller, client_file_name, \
    start_n_engines, terminate_engines


def wait_for(condition: Callable):
    for _ in range(300):
        if condition():
            break
        else:
            time.sleep(.1)
    assert condition()


def echo(delay=0):
    def inner_echo(x):
        import time
        if delay:
            time.sleep(delay)
        return x
    return inner_echo


class OverheadLatencySuite:
    controller, client, engines, n = None, None, [], 0
    param_names = ['number of tasks', 'delay for echo']
    timer = timeit.default_timer
    params = [[1], [0, .1, 1]]

    def __init__(self, n=1):
        self.n = n
        self.params = [[1], [0, .1, 1]]

    def exit_function(self):
        # print('Exiting')
        OverheadLatencySuite.controller.terminate()
        terminate_engines(self.engines)
        try:
            os.remove(client_file_name())
        except FileNotFoundError:
            pass

    def setup(self, *_):
        # if OverheadLatencySuite.controller is None:
        #     print('Making a new controller')
        #     OverheadLatencySuite.controller = Controller()
            # wait_for(lambda: os.path.exists(client_file_name()))
            # atexit.register(self.exit_function)
        self.client = ipp.Client()
        # self.engines = self.engines + start_n_engines(self.n - len(self.client))
        wait_for(lambda: len(self.client) >= self.n)
        self.dview = self.client[:self.n]

    def teardown(self, *_):
        self.client.close()

    def time_n_tasks(self, tasks, delay):
        self.dview.map_sync(echo(delay), [None] * tasks)

class Engines10(OverheadLatencySuite):
    def __init__(self):
        super().__init__(10)
        self.params = [[10, 100], [0, .1, 1]]


class Engines100(OverheadLatencySuite):
    def __init__(self):
        super().__init__(100)
        self.params = [[100, 1000], [0, .1, 1]]


class Engines100NoDelay(OverheadLatencySuite):
    def __init__(self):
        super().__init__(100)
        self.params = [[100, 1000, 10000, 100000], [0]]

