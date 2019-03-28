import sys
from subprocess import Popen
import os
from typing import List

from IPython.paths import get_ipython_dir
from os.path import join

null = open(os.devnull, 'wb')
profile = 'profile_default'


def launch(cmd):
    return Popen(cmd, stdout=null, stderr=null) # env={'IPYTHONDIR':IPYTHONDIR}


def Controller():
    return launch([sys.executable, '-m', 'ipyparallel.controller'])


def Engine():
    return launch([sys.executable, '-m', 'ipyparallel.engine'])


def client_file_name():
    return join(get_ipython_dir(), 'profile_default', 'security', 'ipcontroller-client.json')


def echo(x, delay=0):
    import time
    if delay:
        time.sleep(delay)
    return x


def start_n_engines(n: int) -> List:
    return [Engine() for _ in range(n)]


def terminate_engines(engines: List):
    for e in engines:
        e.terminate()