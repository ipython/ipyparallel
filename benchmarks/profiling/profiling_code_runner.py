import datetime
import os
import sys
import time
from subprocess import Popen, check_call, check_output

import ipyparallel as ipp


def wait_for(condition):
    for _ in range(750):
        if condition():
            break
        else:
            time.sleep(0.1)
    if not condition():
        raise TimeoutError('wait_for took to long to finish')


def get_time_stamp() -> str:
    return (
        str(datetime.datetime.now()).split(".")[0].replace(" ", "-").replace(":", "-")
    )


def start_cmd(cmd, blocking=True):
    print(cmd)
    return (
        check_call(
            cmd,
            stdout=sys.__stdout__,
            stderr=open('spanning_tree_error.out', 'a+'),
            shell=True,
        )
        if blocking
        else Popen(
            cmd,
            stdout=sys.__stdout__,
            stderr=open('spanning_tree_error.out', 'a+'),
            shell=True,
        )
    )


def stop_cluster():
    if '-s' not in sys.argv:
        start_cmd('ipcluster stop --profile=asv')


# atexit.register(stop_cluster)

PROFILING_TASKS = [
    'many_empty_tasks',
    'many_empty_tasks_non_blocking',
    'tasks_with_large_data',
    'echo_many_arguments',
]

VIEW_TYPES = ['direct', 'load_balanced']


def get_tasks_to_execute(program_arguments):
    return (
        [f'{program_arguments[2]} {view}' for view in VIEW_TYPES]
        if len(program_arguments) >= 2
        else [f'{task} {view}' for task in PROFILING_TASKS for view in VIEW_TYPES]
    )


if __name__ == "__main__":
    print('profiling_code_runner_started')
    if '-s' not in sys.argv:
        n = int(sys.argv[1]) if len(sys.argv) > 1 else 16
        client = ipp.Client(profile='asv')
        print(f'Waiting for {n} engines to get available')
        try:
            wait_for(lambda: len(client) >= n)
        except TimeoutError as e:
            print(e)
            exit(1)
        print('Starting the profiling')

    controller_pid = check_output('pgrep -f ipyparallel.controller', shell=True)
    number_of_schedulers = 15
    scheduler_pids = sorted(int(x) for x in controller_pid.decode('utf-8').split())[
        -number_of_schedulers:
    ]

    client_output_path = os.path.join(os.getcwd(), 'spanning_tree_client.svg')

    files_to_upload = [client_output_path]
    ps = []
    for i, scheduler_pid in enumerate(scheduler_pids):
        scheduler_output_path = os.path.join(
            os.getcwd(), f'spanning_tree_{i}_scheduler.svg'
        )
        files_to_upload.append(scheduler_output_path)
        ps.append(
            start_cmd(
                f'sudo py-spy --function -d 60 --flame {scheduler_output_path} --pid {scheduler_pid}',
                blocking=False,
            )
        )

    start_cmd(
        f'sudo py-spy --function -d 60 --flame {client_output_path} -- python profiling_code.py tasks_with_large_data spanning_tree'
    )
    print('client ended')
    for p in ps:
        p.wait()
