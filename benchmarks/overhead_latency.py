from typing import Callable
import ipyparallel as ipp
import os
from benchmarks.utils import Controller, client_file_name, echo, \
    start_n_engines, terminate_engines


def wait_for(condition: Callable):
    import time
    for _ in range(300):
        if condition():
            break
        else:
            time.sleep(.1)
    assert condition()


n_engines = [1, 10, 30]
delays = [0, .1]#, 1]


class OverheadLatencySuite:
    params = (n_engines, delays)
    param_names = ['number of engines', 'delay for ech'
                                        '']
    controller, engines, client = None, None, None


    def setup(self, n, delay):
        print('param: ', n, delay)
        self.controller = Controller()
        self.engines = start_n_engines(n)
        wait_for(lambda: os.path.exists(client_file_name()))
        self.client = ipp.Client()
        wait_for(lambda: len(self.client) == n)
        self.l_b_view = self.client.load_balanced_view()

    def teardown(self, *_):
        self.client.shutdown(hub=True)
        self.client.close()
        self.controller.terminate()
        terminate_engines(self.engines)
        os.remove(client_file_name())

    def time_one_task_on_n_engines(self, _, delay):
        self.l_b_view.apply(lambda x: echo(x, delay), None)

    def time_n_tasks_on_n_engines(self, n, delay):
        self.l_b_view.map(lambda x: echo(x, delay), [None for _ in range(n)])

    def time_100_tasks_on_n_engines(self, n, delay):
        self.l_b_view.map(lambda x: echo(x, delay), [None for _ in range(100)])

    def time_1000_tasks_on_n_engines(self, n, delay):
        self.l_b_view.map(lambda x: echo(x, delay), [None for _ in range(1000)])

    # def time_10000_tasks_on_n_engines(self, n, delay): Will take several hours to run with the current paramaters
    #         self.l_b_view.map(lambda x: echo(*x), [(None, p[1]) for _ in range(10000)])

    def time_10n_tasks_on_n_engines(self, n, delay):
        self.l_b_view.map(lambda x: echo(x, delay), [None for _ in range(n * 10)])
