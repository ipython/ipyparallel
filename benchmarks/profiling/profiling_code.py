import sys

import numpy as np

import ipyparallel as ipp


def echo(delay=0):
    def inner_echo(x, **kwargs):
        import time

        if delay:
            time.sleep(delay)
        return x

    return inner_echo


def profile_many_empty_tasks(lview, n, block=True):
    lview.map(echo(0), [None] * n, block=block)


# def profile_echo_many_arguments(lview, number_of_arguments):
#     lview.map(
#         lambda x: echo_many_arguments(*x),
#         [
#                         tuple(np.empty(1, dtype=np.int8) for n in range(number_of_arguments))
#             for x in range(16)
#         ],
#         block=False,
#     )


def profile_tasks_with_large_data(lview, num_bytes):
    for _ in range(10):
        for i in range(10):
            lview.apply_sync(echo(0), np.array([0] * num_bytes, dtype=np.int8))


def run_profiling(selected_profiling_task, selected_view):
    client = ipp.Client(profile='asv')
    # add it to path on the engines
    # client[:].apply_sync(add_to_path, master_project_parent)
    print('profiling task: ', selected_profiling_task)
    print('profiling view: ', selected_view)
    if selected_view == 'direct':
        view = client[:]
    elif selected_view == 'spanning_tree':
        view = client.spanning_tree_view()
    else:
        view = client.load_balanced_view()

    # view = client[:] if selected_view == 'direct' else client.load_balanced_view()

    if selected_profiling_task == 'many_empty_tasks':
        for x in range(1, 5):
            profile_many_empty_tasks(view, 10**x)
    elif selected_profiling_task == 'many_empty_tasks_non_blocking':
        for x in range(1, 5):
            profile_many_empty_tasks(view, 10**x, block=False)
    elif selected_profiling_task == 'tasks_with_large_data':
        for x in range(1, 8):
            profile_tasks_with_large_data(view, 10**x)
    # elif selected_profiling_task == 'echo_many_arguments':
    #     for i in range(100):
    #         for number_of_arguments in ((2 ** x) - 1 for x in range(1, 9)):
    #             profile_echo_many_arguments(view, number_of_arguments)


if __name__ == "__main__":
    run_profiling(sys.argv[1], sys.argv[2])
