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


def profile_many_empty_tasks(lview, n):
    lview.map_sync(echo(0), [None] * n)


def profile_many_empty_tasks_non_blocking(lview, n):
    lview.map(echo(0), [None] * n, block=False)


def profile_echo_many_arguments(lview, t):
    lview.map(
        lambda x: echo_many_arguments(*x),
        [(np.empty(1, dtype=np.int8) for n in range(t)) for x in range(16)],
    )


def profile_tasks_with_large_data(client, num_bytes):
    for i in range(10):
        client[:].apply_sync(echo(0), np.empty(num_bytes, dtype=np.int8))


def run_profiling(selected_profiling_task):
    client = ipp.Client(profile='asv')
    # add it to path on the engines
    client[:].apply_sync(add_to_path, master_project_parent)
    lview = client.load_balanced_view()
    # for x in range(1, 5):
    #     profile_many_empty_tasks(lview, 10 ** x)
    for x in range(1, 7):
        print(x)
        profile_tasks_with_large_data(client, 20 ** x)


if __name__ == "__main__":
    run_profiling(sys.argv[1])
