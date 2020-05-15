import sys
import time
from subprocess import Popen
import ipyparallel as ipp


def wait_for(client):
    for _ in range(300):
        if len(client) > 0:
            return
        else:
            time.sleep(0.1)


def start_cluster(engines=30):
    Popen(
        ['ipcluster', 'start', '-n', '5', '--daemon', '--profile=asv']
    )


class TestingSuite:
    params = map(lambda x: str(x), [1, 2, 3, 4, 5])
    timeout = 30

    def setup(self, *args):
        print('starting cluster')
        self.ps = start_cluster()
        print('starting client')
        self.client = ipp.Client(profile='asv')
        print('checking client len')
        wait_for(self.client)
        print(self.client)
        print('client len', len(self.client))
        print('finished waiting for engines')

    def time_testing(self, x):
        lol = 3
        # self.client[:].apply_sync(lambda y: y, x)

    def teardown(self, *args):
        Popen(
            ['ipcluster', 'stop', '--profile=asv']
        )
