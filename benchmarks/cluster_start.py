import atexit
import sys
import time
from subprocess import Popen

import ipyparallel as ipp
from benchmarks.throughput import wait_for


def start_cluster(depth, number_of_engines, path='', log_output_to_file=False):
    ipcontroller_cmd = (
        f'{path}ipcontroller --profile=asv --nodb '
        f'--cluster-id=depth_{depth} '
        f'--HubFactory.broadcast_scheduler_depth={depth} '
        f'--HubFactory.db_class=NoDB'
    )
    print(ipcontroller_cmd)
    ipengine_cmd = f'{path}ipengine --profile=asv ' f'--cluster-id=depth_{depth} '
    ps = [
        Popen(
            ipcontroller_cmd.split(),
            stdout=(
                open('ipcontroller_output.log', 'a+')
                if log_output_to_file
                else sys.stdout
            ),
            stderr=(
                open('ipcontroller_error_output.log', 'a+')
                if log_output_to_file
                else sys.stdout
            ),
            stdin=sys.stdin,
        )
    ]
    time.sleep(2)
    client = ipp.Client(profile='asv', cluster_id=f'depth_{depth}')
    print(ipengine_cmd)
    for i in range(number_of_engines):
        ps.append(
            Popen(
                ipengine_cmd.split(),
                stdout=(
                    open('ipengine_output.log', 'a+')
                    if log_output_to_file
                    else sys.stdout
                ),
                stderr=(
                    open('ipengine_error_output.log', 'a+')
                    if log_output_to_file
                    else sys.stdout
                ),
                stdin=sys.stdin,
            )
        )
        if i % 10 == 0:
            wait_for(lambda: len(client) >= i - 10)
        if i % 20 == 0:
            time.sleep(2)
            print(f'{len(client)} engines started')

    return ps


if __name__ == '__main__':
    if len(sys.argv) > 3:
        depth = sys.argv[1]
        number_of_engines = int(sys.argv[3])
    else:
        depth = 3
        number_of_engines = 30

    ps = start_cluster(depth, number_of_engines)

    for p in ps:
        p.wait()

    def clean_up():
        for p in ps:
            p.kill()

    atexit.register(clean_up)
