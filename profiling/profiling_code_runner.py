import sys
import os
import atexit

master_project_path = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir)  # '..'
)
master_project_parent = os.path.abspath(
    os.path.join(master_project_path, os.pardir)  # '..'
)
from ipyparallel_master_project.benchmarks.utils import wait_for, time_stamp
from ipyparallel_master_project.logger import get_profiling_log_file_name
import ipyparallel_master_project.profiling.view_profiling_results as view_results
from subprocess import check_call, check_output, Popen
import ipyparallel_master_project.profiling.profiling_code as profiling_code
import ipyparallel as ipp


sys.path.insert(0, master_project_parent)

LOG_FILE_NAME = get_profiling_log_file_name()
PROFILING_CODE_PATH = os.path.abspath(profiling_code.__file__)
ALL_RESULTS_DIRECTORY = os.path.join(master_project_path, 'results', 'profiling')
VIEW_RESULTS_PATH = os.path.abspath(view_results.__file__)
# OUTPUT_FILE_NAME = f'{LOG_FILE_NAME}.cprof'
PROFILING_FUNCTIONS = [
    'echo_many_arguments',
    'many_empty_tasks',
    'many_empty_tasks_non_blocking',
    'tasks_with_large_data',
]


def start_cmd(cmd, blocking=True):
    print(cmd)
    return (
        check_call(
            cmd,
            stdout=sys.__stdout__,
            stderr=open(f'{LOG_FILE_NAME}_error.out', 'a+'),
            shell=True,
        )
        if blocking
        else Popen(
            cmd,
            stdout=sys.__stdout__,
            stderr=open(f'{LOG_FILE_NAME}_error.out', 'a+'),
            shell=True,
        )
    )


def stop_cluster():
    if '-s' not in sys.argv:
        start_cmd(f'ipcluster stop --profile=asv')


atexit.register(stop_cluster)

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

    CURRENT_RESULTS_DIR = os.path.join(ALL_RESULTS_DIRECTORY, time_stamp())
    os.mkdir(CURRENT_RESULTS_DIR)

    if '-s' not in sys.argv:
        n = sys.argv[1] if len(sys.argv) > 1 else 16
        start_cmd(f'ipcluster start -n {n} --daemon --profile=asv')
        client = ipp.Client(profile='asv')
        print(f'Waiting for {n} engines to get available')
        try:
            wait_for(lambda: len(client) >= n)
        except TimeoutError as e:
            print(e)
            start_cmd(f'ipcluster stop --profile=asv')
            exit(1)
        print('Starting the profiling')

    controller_pid = check_output('pgrep -f ipyparallel.controller', shell=True)
    scheduler_pid = max((int(x) for x in controller_pid.decode('utf-8').split()))

    for task_name in get_tasks_to_execute(sys.argv):
        scheduler_output_path = os.path.join(
            CURRENT_RESULTS_DIR, f'{task_name.replace(" ", "_")}_scheduler.svg'
        )
        client_output_path = os.path.join(
            CURRENT_RESULTS_DIR, f'{task_name.replace(" ", "_")}_client.svg'
        )
        p = start_cmd(
            f'sudo py-spy --function -d 30 --flame {scheduler_output_path} --pid {scheduler_pid}',
            blocking=False,
        )
        start_cmd(
            f'sudo py-spy --function -d 30 --flame {client_output_path} -- python {PROFILING_CODE_PATH} {task_name}'
        )
        p.wait()
