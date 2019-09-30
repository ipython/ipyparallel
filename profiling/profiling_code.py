import ipyparallel as ipp
import numpy as np
import os
import sys

# ensure ipyparallel_master_project is on sys.path
master_project_parent = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)  # '..'
)


def add_to_path(path):
    import sys
    sys.path.insert(0, path)


# add it to path before importing it
add_to_path(master_project_parent)
from ipyparallel_master_project.benchmarks.utils import echo, echo_many_arguments


def profile_many_empty_tasks(lview, n, block=True):
    lview.map(echo(0), [None] * n, block=block)


def profile_echo_many_arguments(lview, number_of_arguments):
    lview.map(
        lambda x: echo_many_arguments(*x),
        [
            tuple(np.empty(1, dtype=np.int8) for n in range(number_of_arguments))
            for x in range(16)
        ],
        block=False,
    )


def profile_tasks_with_large_data(client, num_bytes):
    for i in range(10):
        client[:].apply_sync(echo(0), np.empty(num_bytes, dtype=np.int8))


def run_profiling(selected_profiling_task):
    client = ipp.Client(profile='asv')
    # add it to path on the engines
    client[:].apply_sync(add_to_path, master_project_parent)
    lview = client.load_balanced_view()
    if selected_profiling_task == 'many_empty_tasks':
        for x in range(1, 5):
            profile_many_empty_tasks(lview, 10 ** x)
    elif selected_profiling_task == 'many_empty_tasks_non_blocking':
        for x in range(1, 5):
            profile_many_empty_tasks(lview, 10 ** x, block=False)
    elif selected_profiling_task == 'tasks_with_large_data':
        for x in range(1, 7):
            profile_tasks_with_large_data(client, 10 ** x)
    elif selected_profiling_task == 'echo_many_arguments':
        for number_of_arguments in ((2 ** x) - 1 for x in range(1, 8)):
            profile_echo_many_arguments(lview, number_of_arguments)


if __name__ == "__main__":
    run_profiling(sys.argv[1])
