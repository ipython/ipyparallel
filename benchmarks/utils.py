import sys
from subprocess import Popen
import os
from time import sleep
from typing import List

from IPython.paths import get_ipython_dir
from os.path import join

null = open(os.devnull, 'wb')
profile = 'profile_default'

asv_directory = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))

def launch(cmd):
    return Popen(cmd, stdout=null, stderr=null,
                 env={"PYTHONPATH": asv_directory})

def Controller():
    # if not controller_exists():
    return launch([sys.executable, '-m', 'ipyparallel.controller'])


def Engine():
    return launch([sys.executable, '-m', 'ipyparallel.engine'])


def client_file_name():
    return join(get_ipython_dir(), 'profile_default', 'security',
                'ipcontroller-client.json')


def start_n_engines(n: int) -> List:
    if n < 1:
        return []
    engines = []
    for i in range(1, n + 1):
        if i % 10 == 0:
            sleep(1)
        engines.append(Engine())
    return engines


def terminate_engines(engines: List):
    for e in engines:
        e.terminate()
