import ipyparallel as ipp
import numpy as np
from ipyparallel_master_project.benchmarks.utils import echo


def profile_many_empty_tasks(lview, n):
    lview.map_sync(echo(0), [None] * n)


def profile_tasks_with_large_data(client, num_bytes):
    client[:]['x'] = np.array([0] * num_bytes, dtype=np.int8)


def run_profiling():
    client = ipp.Client(profile='asv')
    lview = client.load_balanced_view()
    for x in range(1, 5):
        profile_many_empty_tasks(lview, 10 ** x)
    for x in range(1, 7):
        profile_tasks_with_large_data(client, 20 ** x)


if __name__ == "__main__":
    run_profiling()
