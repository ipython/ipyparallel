import time
from subprocess import check_call

import ipyparallel as ipp

n = 20


check_call(f'ipcluster start -n {n} --daemon --profile=asv --debug', shell=True)
c = ipp.Client(profile='asv')
seen = -1

running_engines = len(c)

while running_engines < n:
    if seen != running_engines:
        print(running_engines)
        seen = running_engines
    running_engines = len(c)
    time.sleep(0.1)

check_call('ipcluster stop --profile=asv', shell=True)
